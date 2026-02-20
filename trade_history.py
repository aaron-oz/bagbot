import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent / "trade_history.db"

_conn = None


def init_db(db_path=None):
    """Initialize the database connection. Pass ':memory:' for tests."""
    global _conn, DB_PATH
    if db_path is not None:
        DB_PATH = db_path
    _conn = sqlite3.connect(str(DB_PATH) if not isinstance(DB_PATH, str) else DB_PATH)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            trade_type TEXT NOT NULL,
            netuid INTEGER NOT NULL,
            tao_amount REAL NOT NULL,
            alpha_amount REAL NOT NULL,
            price_at_trade REAL NOT NULL,
            slippage REAL,
            validator_hotkey TEXT NOT NULL
        )
    """)
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_netuid ON trades(netuid)")
    _conn.commit()


def _get_conn():
    global _conn
    if _conn is None:
        init_db()
    return _conn


def record_trade(trade_type, netuid, tao_amount, alpha_amount, price_at_trade,
                 slippage, validator_hotkey, timestamp=None):
    """Insert a trade record. timestamp defaults to current time."""
    if timestamp is None:
        timestamp = int(time.time())
    conn = _get_conn()
    conn.execute(
        """INSERT INTO trades
           (timestamp, trade_type, netuid, tao_amount, alpha_amount, price_at_trade, slippage, validator_hotkey)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (timestamp, trade_type, netuid, tao_amount, alpha_amount, price_at_trade, slippage, validator_hotkey)
    )
    conn.commit()


def get_cost_basis(netuid):
    """
    Calculate weighted-average cost basis for a subnet.

    Returns dict with:
        avg_buy_price, total_tao_invested, total_alpha_held,
        realized_pnl, total_tao_received
    Or None if no trades for this subnet.
    """
    conn = _get_conn()
    rows = conn.execute(
        "SELECT trade_type, tao_amount, alpha_amount FROM trades WHERE netuid = ? ORDER BY timestamp",
        (netuid,)
    ).fetchall()

    if not rows:
        return None

    total_tao_invested = 0.0
    total_alpha_held = 0.0
    total_tao_received = 0.0
    realized_pnl = 0.0

    for trade_type, tao_amount, alpha_amount in rows:
        if trade_type == 'buy':
            total_tao_invested += tao_amount
            total_alpha_held += alpha_amount
        elif trade_type == 'sell':
            if total_alpha_held > 0:
                fraction_sold = min(alpha_amount / total_alpha_held, 1.0)
                cost_of_sold = total_tao_invested * fraction_sold
                realized_pnl += tao_amount - cost_of_sold
                total_tao_invested -= cost_of_sold
                total_alpha_held -= alpha_amount
                total_tao_received += tao_amount
            else:
                # Selling with no tracked holdings (e.g. pre-existing stake)
                realized_pnl += tao_amount
                total_tao_received += tao_amount

    # Clamp near-zero floating point noise
    if total_alpha_held < 1e-9:
        total_alpha_held = 0.0
    if total_tao_invested < 1e-12:
        total_tao_invested = 0.0

    avg_buy_price = (total_tao_invested / total_alpha_held) if total_alpha_held > 0 else 0.0

    return {
        'avg_buy_price': avg_buy_price,
        'total_tao_invested': total_tao_invested,
        'total_alpha_held': total_alpha_held,
        'realized_pnl': realized_pnl,
        'total_tao_received': total_tao_received,
    }


def get_all_cost_bases():
    """Cost basis for every traded subnet. Returns dict[int, dict]."""
    conn = _get_conn()
    netuids = conn.execute("SELECT DISTINCT netuid FROM trades").fetchall()
    result = {}
    for (netuid,) in netuids:
        basis = get_cost_basis(netuid)
        if basis is not None:
            result[netuid] = basis
    return result


def insert_baseline_buy(netuid, alpha_amount, price, validator_hotkey, timestamp):
    """Insert a synthetic buy to establish cost basis for pre-existing positions."""
    conn = _get_conn()
    tao_amount = alpha_amount * price
    conn.execute(
        """INSERT INTO trades
           (timestamp, trade_type, netuid, tao_amount, alpha_amount, price_at_trade, slippage, validator_hotkey)
           VALUES (?, 'buy', ?, ?, ?, ?, 0.0, ?)""",
        (timestamp, netuid, tao_amount, alpha_amount, price, validator_hotkey)
    )
    conn.commit()


def delete_subnet_trades(netuid):
    """Delete all trades for a subnet."""
    conn = _get_conn()
    conn.execute("DELETE FROM trades WHERE netuid = ?", (netuid,))
    conn.commit()


def _ensure_snapshots_table():
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS position_snapshots (
            timestamp INTEGER NOT NULL,
            netuid INTEGER NOT NULL,
            alpha_held REAL NOT NULL,
            price REAL NOT NULL,
            tao_invested REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_netuid_ts
        ON position_snapshots(netuid, timestamp)
    """)
    conn.commit()


def record_snapshot(netuid, alpha_held, price, tao_invested, timestamp=None):
    """Record a position snapshot for delta-pnl tracking."""
    if timestamp is None:
        timestamp = int(time.time())
    conn = _get_conn()
    _ensure_snapshots_table()
    conn.execute(
        "INSERT INTO position_snapshots (timestamp, netuid, alpha_held, price, tao_invested) VALUES (?, ?, ?, ?, ?)",
        (timestamp, netuid, alpha_held, price, tao_invested)
    )
    conn.commit()


def record_snapshots_bulk(entries, timestamp=None):
    """Record multiple snapshots in one transaction. entries: list of (netuid, alpha_held, price, tao_invested)."""
    if timestamp is None:
        timestamp = int(time.time())
    conn = _get_conn()
    _ensure_snapshots_table()
    conn.executemany(
        "INSERT INTO position_snapshots (timestamp, netuid, alpha_held, price, tao_invested) VALUES (?, ?, ?, ?, ?)",
        [(timestamp, n, a, p, t) for n, a, p, t in entries]
    )
    conn.commit()


def get_pnl_delta(netuid, hours):
    """
    Compute the change in unrealized P&L over the given time window.

    unrealized_pnl = alpha_held * price - tao_invested

    We compare avg(pnl) over a recent window vs avg(pnl) over an earlier window,
    with smoothing proportional to the lookback (window = lookback / 4).

    Returns (delta_tao, pct_of_invested) or (None, None) if insufficient data.
    """
    conn = _get_conn()
    _ensure_snapshots_table()
    now = int(time.time())
    lookback_secs = int(hours * 3600)
    # Smoothing window: 1/4 of the lookback, minimum 3 minutes
    smooth_secs = max(lookback_secs // 4, 180)

    # Recent window: [now - smooth_secs, now]
    recent = conn.execute(
        """SELECT alpha_held * price - tao_invested, tao_invested
           FROM position_snapshots
           WHERE netuid = ? AND timestamp >= ? AND timestamp <= ?""",
        (netuid, now - smooth_secs, now)
    ).fetchall()

    # Earlier window: [now - lookback - smooth_secs/2, now - lookback + smooth_secs/2]
    earlier_center = now - lookback_secs
    earlier = conn.execute(
        """SELECT alpha_held * price - tao_invested, tao_invested
           FROM position_snapshots
           WHERE netuid = ? AND timestamp >= ? AND timestamp <= ?""",
        (netuid, earlier_center - smooth_secs // 2, earlier_center + smooth_secs // 2)
    ).fetchall()

    if not recent or not earlier:
        return None, None

    avg_pnl_now = sum(r[0] for r in recent) / len(recent)
    avg_invested_now = sum(r[1] for r in recent) / len(recent)
    avg_pnl_earlier = sum(r[0] for r in earlier) / len(earlier)

    delta = avg_pnl_now - avg_pnl_earlier
    pct = (delta / avg_invested_now * 100) if avg_invested_now > 0 else None

    return delta, pct


def get_all_pnl_deltas(hours):
    """Get pnl deltas for all subnets with snapshot data. Returns dict[int, (delta, pct)]."""
    conn = _get_conn()
    _ensure_snapshots_table()
    netuids = conn.execute("SELECT DISTINCT netuid FROM position_snapshots").fetchall()
    result = {}
    for (netuid,) in netuids:
        delta, pct = get_pnl_delta(netuid, hours)
        if delta is not None:
            result[netuid] = (delta, pct)
    return result


def cleanup_old_snapshots(max_age_days=10):
    """Remove snapshots older than max_age_days to keep the DB lean."""
    conn = _get_conn()
    _ensure_snapshots_table()
    cutoff = int(time.time()) - max_age_days * 86400
    conn.execute("DELETE FROM position_snapshots WHERE timestamp < ?", (cutoff,))
    conn.commit()


def get_portfolio_summary():
    """
    Aggregate portfolio stats across all subnets.

    Returns dict with: total_invested, total_received, realized_pnl
    """
    bases = get_all_cost_bases()
    total_invested = 0.0
    total_received = 0.0
    realized_pnl = 0.0
    for basis in bases.values():
        total_invested += basis['total_tao_invested']
        total_received += basis['total_tao_received']
        realized_pnl += basis['realized_pnl']
    return {
        'total_invested': total_invested,
        'total_received': total_received,
        'realized_pnl': realized_pnl,
    }
