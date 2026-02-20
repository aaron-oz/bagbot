#!/usr/bin/env python3
"""
One-time script to parse staking.log and backfill trade_history.db.

Log patterns:
  BUY:
    "Want to buy sn{N} at price {P} ..."
    "About to stake τ{TAO} to {N} with expected slippage of {S}%"
    "Attempting to stake {TAO} TAO to subnet {N}"
    "Failed to stake ... (ExtrinsicResponse:" or "Staked ..."
    followed by multi-line response containing "success: True/False"
    and call_args with 'amount_staked' (rao), 'netuid', 'hotkey'

  SELL:
    "About to unstake {ALPHA} alpha (~{TAO} TAO) in sn{N} on hotkey {HK} ..."
    "Attempting to unstake {ALPHA} alpha from subnet {N}"
    "Failed to unstake ... (ExtrinsicResponse:" or "Unstaked ..."
    followed by multi-line response containing "success: True/False"
    and call_args with 'amount_unstaked' (rao), 'netuid', 'hotkey'
    and data with 'balance_before'/'balance_after' for exact TAO received
"""

import re
import sys
from datetime import datetime
from pathlib import Path

import trade_history

LOG_PATH = Path(__file__).parent / "staking.log"

# Regex patterns
TIMESTAMP_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+')
ATTEMPT_STAKE_RE = re.compile(r'Attempting to stake ([\d.]+) TAO to subnet (\d+)')
ATTEMPT_UNSTAKE_RE = re.compile(r'Attempting to unstake ([\d.]+) alpha from subnet (\d+)')
SUCCESS_RE = re.compile(r'^\tsuccess: (True|False)')
AMOUNT_STAKED_RE = re.compile(r"'amount_staked': (\d+)")
AMOUNT_UNSTAKED_RE = re.compile(r"'amount_unstaked': (\d+)")
HOTKEY_RE = re.compile(r"'hotkey': '([A-Za-z0-9]+)'")
NETUID_CALL_RE = re.compile(r"'netuid': (\d+)")
LIMIT_PRICE_RE = re.compile(r"'limit_price': (\d+)")
BALANCE_BEFORE_RE = re.compile(r"'balance_before': τ([\d.]+)")
BALANCE_AFTER_RE = re.compile(r"'balance_after': τ([\d.]+)")

# Context patterns (preceding lines)
WANT_TO_BUY_RE = re.compile(r'Want to buy sn(\d+) at price ([\d.]+)')
ABOUT_TO_STAKE_RE = re.compile(r'About to stake .+ to (\d+) with expected slippage of ([\d.]+)%')
ABOUT_TO_UNSTAKE_RE = re.compile(r'About to unstake ([\d.]+) alpha \(~([\d.]+) TAO\) in sn(\d+) on hotkey (\S+) with expected slippage of ([\d.]+)%')

RAO_TO_TAO = 1e-9


def parse_timestamp(line):
    m = TIMESTAMP_RE.match(line)
    if m:
        dt = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
        return int(dt.timestamp())
    return None


def backfill(log_path=None):
    if log_path is None:
        log_path = LOG_PATH

    trade_history.init_db()
    conn = trade_history._get_conn()

    # Load existing trades for dedup: set of (timestamp, netuid, trade_type)
    existing = set()
    for row in conn.execute("SELECT timestamp, netuid, trade_type FROM trades"):
        existing.add((row[0], row[1], row[2]))

    lines = Path(log_path).read_text(errors='replace').splitlines()
    total = len(lines)

    buys_added = 0
    sells_added = 0
    skipped = 0

    # Context from preceding lines
    last_want_to_buy = {}  # netuid -> price
    last_about_to_stake_slippage = {}  # netuid -> slippage
    last_about_to_unstake = {}  # netuid -> {alpha, approx_tao, hotkey, slippage}

    i = 0
    while i < total:
        line = lines[i]

        # Track context: "Want to buy" lines
        m = WANT_TO_BUY_RE.search(line)
        if m:
            netuid = int(m.group(1))
            price = float(m.group(2))
            last_want_to_buy[netuid] = price

        # Track context: "About to stake" lines
        m = ABOUT_TO_STAKE_RE.search(line)
        if m:
            netuid = int(m.group(1))
            slippage = float(m.group(2))
            last_about_to_stake_slippage[netuid] = slippage

        # Track context: "About to unstake" lines
        m = ABOUT_TO_UNSTAKE_RE.search(line)
        if m:
            alpha = float(m.group(1))
            approx_tao = float(m.group(2))
            netuid = int(m.group(3))
            hotkey = m.group(4)
            slippage = float(m.group(5))
            last_about_to_unstake[netuid] = {
                'alpha': alpha, 'approx_tao': approx_tao,
                'hotkey': hotkey, 'slippage': slippage
            }

        # === BUY: "Attempting to stake" ===
        m = ATTEMPT_STAKE_RE.search(line)
        if m:
            timestamp = parse_timestamp(line)
            tao_amount = float(m.group(1))
            netuid = int(m.group(2))

            # Scan forward for the ExtrinsicResponse block
            success = None
            hotkey = None
            amount_staked_rao = None
            limit_price_rao = None
            j = i + 1
            while j < min(i + 50, total):
                resp_line = lines[j]
                sm = SUCCESS_RE.search(resp_line)
                if sm:
                    success = sm.group(1) == 'True'
                hm = HOTKEY_RE.search(resp_line)
                if hm and hotkey is None:
                    hotkey = hm.group(1)
                am = AMOUNT_STAKED_RE.search(resp_line)
                if am:
                    amount_staked_rao = int(am.group(1))
                lm = LIMIT_PRICE_RE.search(resp_line)
                if lm:
                    limit_price_rao = int(lm.group(1))
                # End of response block
                if resp_line.startswith(')'):
                    break
                j += 1

            if success and timestamp:
                # Get price from context or compute from limit_price
                price = last_want_to_buy.get(netuid)
                if price is None and limit_price_rao:
                    price = limit_price_rao * RAO_TO_TAO
                if price is None:
                    price = 0.0

                # Use rao amount if available, otherwise use parsed TAO
                if amount_staked_rao:
                    tao_amount = amount_staked_rao * RAO_TO_TAO

                alpha_amount = tao_amount / price if price > 0 else 0.0
                slippage = last_about_to_stake_slippage.get(netuid, 0.0)
                if hotkey is None:
                    hotkey = ''

                key = (timestamp, netuid, 'buy')
                if key not in existing:
                    trade_history.record_trade(
                        'buy', netuid, tao_amount, alpha_amount,
                        price, slippage, hotkey, timestamp=timestamp
                    )
                    existing.add(key)
                    buys_added += 1
                else:
                    skipped += 1

            i = j + 1
            continue

        # === SELL: "Attempting to unstake" ===
        m = ATTEMPT_UNSTAKE_RE.search(line)
        if m:
            timestamp = parse_timestamp(line)
            alpha_amount = float(m.group(1))
            netuid = int(m.group(2))

            # Scan forward for the ExtrinsicResponse block
            success = None
            hotkey = None
            amount_unstaked_rao = None
            limit_price_rao = None
            balance_before = None
            balance_after = None
            j = i + 1
            while j < min(i + 50, total):
                resp_line = lines[j]
                sm = SUCCESS_RE.search(resp_line)
                if sm:
                    success = sm.group(1) == 'True'
                hm = HOTKEY_RE.search(resp_line)
                if hm and hotkey is None:
                    hotkey = hm.group(1)
                am = AMOUNT_UNSTAKED_RE.search(resp_line)
                if am:
                    amount_unstaked_rao = int(am.group(1))
                lm = LIMIT_PRICE_RE.search(resp_line)
                if lm:
                    limit_price_rao = int(lm.group(1))
                bb = BALANCE_BEFORE_RE.search(resp_line)
                if bb:
                    balance_before = float(bb.group(1))
                ba = BALANCE_AFTER_RE.search(resp_line)
                if ba:
                    balance_after = float(ba.group(1))
                if resp_line.startswith(')'):
                    break
                j += 1

            if success and timestamp:
                # Alpha from rao if available
                if amount_unstaked_rao:
                    alpha_amount = amount_unstaked_rao * RAO_TO_TAO

                # TAO received: use approx_tao from "About to unstake" context line.
                # This is computed from the bot's real-time price * alpha_to_sell.
                # - balance_before/after is unreliable (spans entire block, other trades pollute it)
                # - limit_price regex can match a neighboring extrinsic from a different subnet
                ctx = last_about_to_unstake.get(netuid)
                if ctx:
                    tao_received = ctx['approx_tao']
                else:
                    tao_received = 0.0

                price = tao_received / alpha_amount if alpha_amount > 0 else 0.0
                slippage_ctx = last_about_to_unstake.get(netuid)
                slippage = slippage_ctx['slippage'] if slippage_ctx else 0.0
                if hotkey is None:
                    hotkey = slippage_ctx['hotkey'] if slippage_ctx else ''

                key = (timestamp, netuid, 'sell')
                if key not in existing:
                    trade_history.record_trade(
                        'sell', netuid, tao_received, alpha_amount,
                        price, slippage, hotkey, timestamp=timestamp
                    )
                    existing.add(key)
                    sells_added += 1
                else:
                    skipped += 1

            i = j + 1
            continue

        i += 1

    print(f"Backfill complete: {buys_added} buys, {sells_added} sells added ({skipped} duplicates skipped)")
    print(f"Total trades in DB: {conn.execute('SELECT COUNT(*) FROM trades').fetchone()[0]}")

    # Print per-subnet summary
    summary = trade_history.get_all_cost_bases()
    if summary:
        print(f"\nCost basis summary ({len(summary)} subnets):")
        for netuid in sorted(summary):
            b = summary[netuid]
            print(f"  sn{netuid}: avg_cost={b['avg_buy_price']:.6f}, "
                  f"invested={b['total_tao_invested']:.4f} TAO, "
                  f"alpha={b['total_alpha_held']:.1f}, "
                  f"realized_pnl={b['realized_pnl']:+.4f} TAO")

    portfolio = trade_history.get_portfolio_summary()
    print(f"\nPortfolio: invested={portfolio['total_invested']:.4f}, "
          f"received={portfolio['total_received']:.4f}, "
          f"realized_pnl={portfolio['realized_pnl']:+.4f}")


if __name__ == '__main__':
    backfill()
