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


def get_subnet_price_delta(netuid, current_alpha, current_price, hours):
    """
    How much has price movement alone gained/lost on the current position?

    Formula: current_alpha * (price_now - price_then)

    This is transfer-proof: it only cares about what you hold RIGHT NOW
    and how the market has moved.  Buys/sells/transfers don't affect it.

    Returns (delta_tao, pct) or (None, None) if no historical price available.
    """
    import price_history
    diff, avg_price = price_history.get_price_change(netuid, current_price, hours)
    if diff is None or current_alpha == 0:
        return None, None

    delta = current_alpha * diff
    value_then = current_alpha * avg_price
    pct = (diff / avg_price * 100) if avg_price > 0 else None
    return delta, pct


def get_all_subnet_price_deltas(stake_info, stats, hours):
    """
    Price-movement deltas for all held subnets.

    Args:
        stake_info: dict of hotkey → {netuid → stake_obj}
        stats: dict of netuid → {"price": float, ...}
        hours: lookback window

    Returns dict[int, (delta_tao, pct)].
    """
    # Collect total alpha per subnet across all validators
    alpha_by_subnet = {}
    for hotkey_stakes in stake_info.values():
        for netuid, stake_obj in hotkey_stakes.items():
            alpha = float(stake_obj.stake) if stake_obj else 0.0
            if alpha > 0:
                alpha_by_subnet[netuid] = alpha_by_subnet.get(netuid, 0.0) + alpha

    result = {}
    for netuid, alpha in alpha_by_subnet.items():
        price = stats.get(netuid, {}).get('price', 0.0)
        if price > 0:
            delta, pct = get_subnet_price_delta(netuid, alpha, price, hours)
            if delta is not None:
                result[netuid] = (delta, pct)
    return result


# ── Portfolio snapshots (for transfer-adjusted total bag tracking) ───

def _ensure_portfolio_table():
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            timestamp INTEGER NOT NULL,
            tao_balance REAL NOT NULL,
            total_staked_value REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_portfolio_ts
        ON portfolio_snapshots(timestamp)
    """)
    conn.commit()


def record_portfolio_snapshot(tao_balance, total_staked_value, timestamp=None):
    """Record total portfolio state for transfer-adjusted delta tracking."""
    if timestamp is None:
        timestamp = int(time.time())
    conn = _get_conn()
    _ensure_portfolio_table()
    conn.execute(
        "INSERT INTO portfolio_snapshots (timestamp, tao_balance, total_staked_value) VALUES (?, ?, ?)",
        (timestamp, tao_balance, total_staked_value)
    )
    conn.commit()


def _net_trade_flow(since_ts, until_ts):
    """
    Net TAO flow from trades in window: sells return TAO (+), buys spend TAO (-).
    Returns the net change in TAO balance that trades alone explain.
    """
    conn = _get_conn()
    rows = conn.execute(
        """SELECT trade_type, tao_amount FROM trades
           WHERE timestamp >= ? AND timestamp <= ?""",
        (since_ts, until_ts)
    ).fetchall()
    net = 0.0
    for trade_type, tao_amount in rows:
        if trade_type == 'sell':
            net += tao_amount   # sell returns TAO to balance
        elif trade_type == 'buy':
            net -= tao_amount   # buy spends TAO from balance
    return net


def get_portfolio_delta(hours):
    """
    Transfer-adjusted portfolio value change over the given window.

    Uses smoothed averaging windows (same as per-subnet deltas) for stability.
    Transfer detection only counts trades BETWEEN the two windows to avoid
    double-counting trades that are already reflected in the averaged balances.

    total_value = tao_balance + total_staked_value
    net_transfers = actual_tao_balance_change - trade_explained_change
    delta = (total_value_now - total_value_then) - net_transfers

    Returns (delta_tao, pct) or (None, None) if insufficient data.
    """
    conn = _get_conn()
    _ensure_portfolio_table()
    now = int(time.time())
    lookback_secs = int(hours * 3600)
    smooth_secs = max(lookback_secs // 4, 180)

    # Recent window: [now - smooth_secs, now]
    recent_start = now - smooth_secs
    recent = conn.execute(
        """SELECT tao_balance, total_staked_value
           FROM portfolio_snapshots
           WHERE timestamp >= ? AND timestamp <= ?""",
        (recent_start, now)
    ).fetchall()

    # Earlier window: [center - smooth/2, center + smooth/2]
    earlier_center = now - lookback_secs
    earlier_start = earlier_center - smooth_secs // 2
    earlier_end = earlier_center + smooth_secs // 2
    earlier = conn.execute(
        """SELECT tao_balance, total_staked_value
           FROM portfolio_snapshots
           WHERE timestamp >= ? AND timestamp <= ?""",
        (earlier_start, earlier_end)
    ).fetchall()

    if not recent or not earlier:
        return None, None

    bal_now = sum(r[0] for r in recent) / len(recent)
    staked_now = sum(r[1] for r in recent) / len(recent)
    total_now = bal_now + staked_now

    bal_then = sum(r[0] for r in earlier) / len(earlier)
    staked_then = sum(r[1] for r in earlier) / len(earlier)
    total_then = bal_then + staked_then

    # Detect transfers: only count trades BETWEEN the two windows.
    # Trades within each window are already reflected in the averaged balances.
    actual_bal_change = bal_now - bal_then
    trade_flow = _net_trade_flow(earlier_end, recent_start)
    net_transfers = actual_bal_change - trade_flow

    # Subtract transfers so we only see trading/market performance
    delta = (total_now - total_then) - net_transfers
    pct = (delta / total_then * 100) if total_then > 0 else None

    return delta, pct


def cleanup_old_snapshots(max_age_days=10):
    """Remove old snapshots to keep the DB lean."""
    conn = _get_conn()
    _ensure_snapshots_table()
    _ensure_portfolio_table()
    cutoff = int(time.time()) - max_age_days * 86400
    conn.execute("DELETE FROM position_snapshots WHERE timestamp < ?", (cutoff,))
    conn.execute("DELETE FROM portfolio_snapshots WHERE timestamp < ?", (cutoff,))
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
