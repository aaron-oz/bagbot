from typing import List, Dict, Tuple
import time
from rich.table import Table
from rich.panel import Panel
from rich import box
import bagbot_settings
import price_history
import trade_history

def price_proximity_bar(buyprice, sellprice, currentprice, bar_width=20):
    """
    Generate an ASCII bar showing how close currentprice is to buyprice or sellprice,
    with the bar scaled to always include all three prices.

    Args:
        buyprice (float): The buy limit order price.
        sellprice (float): The sell limit order price.
        currentprice (float): The current market price.
        bar_width (int): The width of the ASCII bar in characters (default 20).

    Returns:
        None: Prints an ASCII bar and price info to the console.
    """
    # Determine the range to include all prices
    min_price = min(buyprice, sellprice, currentprice)
    max_price = max(buyprice, sellprice, currentprice)

    # Add padding (10% of the range or 0.1 minimum) for readability
    price_range = max_price - min_price
    padding = price_range * 0.1 if price_range > 0 else 0.1
    bar_min = min_price - padding
    bar_max = max_price + padding

    # Initialize the bar
    bar = ['-'] * bar_width

    # Calculate positions for buy, sell, and current prices
    def price_to_position(price):
        pos = int(((price - bar_min) / (bar_max - bar_min)) * bar_width)
        return max(0, min(pos, bar_width - 1))  # Clamp to valid range

    buy_pos = price_to_position(buyprice)
    sell_pos = price_to_position(sellprice)
    current_pos = price_to_position(currentprice)

    # Place markers, handling overlaps
    bar[current_pos] = '|'  # Current price marker
    if buy_pos == current_pos:
        bar[buy_pos] = 'X'  # Overlap of current and buy
    else:
        bar[buy_pos] = 'B'
    if sell_pos == current_pos:
        bar[sell_pos] = 'X'  # Overlap of current and sell
    elif sell_pos == buy_pos:
        bar[sell_pos] = 'Y'  # Overlap of buy and sell (rare, but possible if equal)
    else:
        bar[sell_pos] = 'S'

    # Convert bar to string
    bar_str = ''.join(bar)

    # Calculate proximity to closest limit order
    dist_to_buy = abs(currentprice - buyprice)
    dist_to_sell = abs(currentprice - sellprice)
    closest_dist = min(dist_to_buy, dist_to_sell)
    closest_price = buyprice if dist_to_buy < dist_to_sell else sellprice
    percentage = (closest_dist / max(abs(max_price - min_price), 0.01)) * 100

    return bar_str


def _fmt_price_diff(diff):
    """Format a price difference with enough decimals to show meaningful digits."""
    abs_diff = abs(diff)
    if abs_diff == 0:
        return "0.0"
    if abs_diff >= 1:
        return f"{diff:.1f}"
    # Find first significant digit and show 1 decimal after it
    import math
    digits_after_dot = -math.floor(math.log10(abs_diff))
    precision = digits_after_dot + 1
    return f"{diff:.{precision}f}"


def get_price_arrow(netuid, current_price, hours_ago):
    """Get a colored percentage showing alpha price change over the given window."""
    diff, avg_price = price_history.get_price_change(netuid, current_price, hours_ago)
    if diff is None or avg_price == 0:
        return "-"
    pct = (diff / avg_price) * 100
    formatted = f"{pct:+.1f}%"
    if pct > 0:
        return f"[green]{formatted}[/green]"
    elif pct < 0:
        return f"[red]{formatted}[/red]"
    return formatted


def _fmt_delta_pnl(delta, pct):
    """Format a delta-pnl value as colored TAO with percentage."""
    if delta is None:
        return "-"
    color = "green" if delta >= 0 else "red"
    # Show TAO amount with adaptive precision
    abs_d = abs(delta)
    if abs_d >= 1:
        tao_str = f"{delta:+.2f}"
    elif abs_d >= 0.01:
        tao_str = f"{delta:+.3f}"
    else:
        tao_str = f"{delta:+.4f}"
    if pct is not None:
        return f"[{color}]{tao_str} ({pct:+.1f}%)[/{color}]"
    return f"[{color}]{tao_str}[/{color}]"


def print_table_rich(
    botInstance,
    console,
    stake_info: Dict,
    allowed_subnets: List[int],
    stats: Dict[int, Dict],
    balance: float,
    subnet_grids: Dict[int, Dict]
):
    """
    Print a Rich table
    """

    timestamp = int(time.time())
    total_stake_value = 0.0

    from datetime import datetime
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    table = Table(title=f"Staking Allocations - {formatted_time}", header_style="bold white on dark_blue", box=box.SIMPLE_HEAVY)
    table.add_column("Subnet", justify="right", style="bright_cyan")
    table.add_column("Name", justify="left", style="white")
    table.add_column("Alpha", justify="right", style="magenta")
    table.add_column("Max Alpha", justify="right", style="magenta")
    table.add_column("% Filled", justify="right", style="magenta")
    table.add_column("TAO Value", justify="right", style="yellow")
    table.add_column("H", justify="right", style="white")
    table.add_column("D", justify="right", style="white")
    table.add_column("W", justify="right", style="white")
    table.add_column("M", justify="right", style="white")
    table.add_column("\u0394 1h", justify="right", style="white")
    table.add_column("\u0394 1d", justify="right", style="white")
    table.add_column("\u0394 1w", justify="right", style="white")
    table.add_column("Buy Lower", justify="right", style="grey66")
    table.add_column("Curr Buy", justify="right", style="bright_green")
    table.add_column("Buy Upper", justify="right", style="grey66")
    table.add_column("Price", justify="left", style="bright_cyan")
    table.add_column("Sell Lower", justify="right", style="grey66")
    table.add_column("Curr Sell", justify="right", style="bright_red")
    table.add_column("Sell Upper", justify="right", style="grey66")
    table.add_column("Price Proximity", justify="right", style="white")

    # Load delta-pnl data for all windows
    try:
        deltas_1h = trade_history.get_all_pnl_deltas(1)
        deltas_1d = trade_history.get_all_pnl_deltas(24)
        deltas_1w = trade_history.get_all_pnl_deltas(168)
    except Exception:
        deltas_1h, deltas_1d, deltas_1w = {}, {}, {}

    # Accumulators for portfolio-wide delta-pnl
    total_delta = {1: 0.0, 24: 0.0, 168: 0.0}
    total_delta_invested = {1: 0.0, 24: 0.0, 168: 0.0}
    total_delta_count = {1: 0, 24: 0, 168: 0}

    # Accumulators for average H/D/W/M percentage changes
    total_pct = {1: 0.0, 24: 0.0, 168: 0.0, 720: 0.0}
    total_pct_count = {1: 0, 24: 0, 168: 0, 720: 0}

    # Collect all unique subnet IDs across all validators
    all_netuids = set()
    for hotkey in stake_info:
        all_netuids.update(stake_info[hotkey].keys())

    for netuid in all_netuids:
        stake_amt = botInstance.my_current_stake(netuid)

        if netuid in stats:
            price = float(stats[netuid]["price"])
            name = stats[netuid].get("name", "")
        else:
            price = 0.0
            name = ""

        # Get previous average delta; if none, use the current delta.

        buy_threshold = botInstance.get_subnet_buy_threshold(netuid)
        sell_threshold = botInstance.get_subnet_sell_threshold(netuid)

        if stake_amt == 0 and buy_threshold is None:
            continue


        stake_value = stake_amt * price
        total_stake_value += stake_value

        # Accumulate H/D/W/M percentage changes
        for hours in (1, 24, 168, 720):
            diff, avg_price = price_history.get_price_change(netuid, price, hours)
            if diff is not None and avg_price != 0:
                total_pct[hours] += (diff / avg_price) * 100
                total_pct_count[hours] += 1

        # Delta-pnl columns
        d1h = deltas_1h.get(netuid)
        d1d = deltas_1d.get(netuid)
        d1w = deltas_1w.get(netuid)
        delta_1h_str = _fmt_delta_pnl(*d1h) if d1h else "-"
        delta_1d_str = _fmt_delta_pnl(*d1d) if d1d else "-"
        delta_1w_str = _fmt_delta_pnl(*d1w) if d1w else "-"

        # Accumulate portfolio-wide deltas
        for hours, deltas in [(1, deltas_1h), (24, deltas_1d), (168, deltas_1w)]:
            if netuid in deltas:
                delta, pct = deltas[netuid]
                total_delta[hours] += delta
                total_delta_count[hours] += 1

        prox_bar = ''
        try:
            if buy_threshold is not None and sell_threshold is not None:
                prox_bar = price_proximity_bar(buy_threshold, sell_threshold, price)
            elif buy_threshold and sell_threshold is None:
                prox_bar = price_proximity_bar(buy_threshold, 1, price)
            elif buy_threshold is None and sell_threshold:
                prox_bar = price_proximity_bar(0, sell_threshold, price)
        except:
            print(traceback.format_exc())
            print(f'Trouble with the proximity bar, skipping for {netuid}')

        probably_buying = False
        if buy_threshold and buy_threshold > price:
            probably_buying = True

        probably_selling = False
        if sell_threshold and sell_threshold < price:
            probably_selling = True


        buy_threshold = f"{buy_threshold:.6f}" if buy_threshold else ''
        sell_threshold = f"{sell_threshold:.6f}" if sell_threshold else ''
        high_buy = botInstance.determine_buy_at_for_amount(botInstance.subnet_grids.get(netuid,{}), 0) or ''
        high_buy = f"{high_buy:.5f}" if high_buy else ''

        low_buy = None
        if botInstance.subnet_grids.get(netuid,{}).get('buy_upper'):
            low_buy = botInstance.determine_buy_at_for_amount(botInstance.subnet_grids.get(netuid,{}), botInstance.subnet_grids.get(netuid,{}).get('max_alpha'))
        low_buy = f"{low_buy:.5f}" if low_buy else ''

        high_sell = bagbot_settings.SUBNET_SETTINGS.get(netuid,{}).get('sell_upper') or bagbot_settings.SUBNET_SETTINGS.get(netuid,{}).get('sell_lower') or ''
        high_sell = f"{high_sell:.5f}" if high_sell else ''
        low_sell = None
        if botInstance.subnet_grids.get(netuid,{}).get('sell_lower'):
            low_sell = botInstance.determine_sell_at_for_amount(botInstance.subnet_grids.get(netuid,{}), botInstance.subnet_grids.get(netuid,{}).get('max_alpha'))
        low_sell = f"{low_sell:.5f}" if low_sell else ''

        max_stake_amt = botInstance.subnet_grids.get(netuid,{}).get('max_alpha',0)
        stake_amount_str = f"{stake_amt:.0f}"
        max_stake_str = f"{max_stake_amt:.0f}" if max_stake_amt > 0 else ''
        stake_perc_filled = str(int(stake_amt*100.0/max_stake_amt)) + '%' if max_stake_amt > 0 else ''
        table.add_row(
            f"{'BUY ' if probably_buying else ''}{'SELL ' if probably_selling else ''}{str(netuid)}",
            name,
            f"{stake_amount_str}",
            f"{max_stake_str}",
            f"{stake_perc_filled}",
            f"{stake_value:.2f}",
            get_price_arrow(netuid, price, 1),      # H (1 hour)
            get_price_arrow(netuid, price, 24),     # D (24 hours)
            get_price_arrow(netuid, price, 168),    # W (7 days)
            get_price_arrow(netuid, price, 720),    # M (30 days)
            delta_1h_str,
            delta_1d_str,
            delta_1w_str,
            f"{low_buy}",
            f"{buy_threshold}",
            f"{high_buy}",
            f"{price:.5f}{'b' if probably_buying else ''}{'s' if probably_selling else ''}",
            f"{low_sell}",
            f"{sell_threshold}",
            f"{high_sell}",
            f"{prox_bar}"
        )

    def _fmt_change(hours, label):
        if total_pct_count[hours] == 0:
            return f"[bold white]{label}:[/bold white] -"
        avg = total_pct[hours] / total_pct_count[hours]
        color = "green" if avg >= 0 else "red"
        return f"[bold white]{label}:[/bold white] [{color}]{avg:+.1f}%[/{color}]"

    def _fmt_total_delta(hours, label):
        if total_delta_count[hours] == 0:
            return f"[bold white]{label}:[/bold white] -"
        d = total_delta[hours]
        color = "green" if d >= 0 else "red"
        return f"[bold white]{label}:[/bold white] [{color}]{d:+.4f}[/{color}]"

    summary = (
        f"[bold green]Total:[/bold green] {balance+total_stake_value:.2f} TAO    "
        f"[bold cyan]Available:[/bold cyan] {balance:.4f} TAO    "
        f"[bold cyan]Stake Value:[/bold cyan] {total_stake_value:.4f} TAO    "
        f"{_fmt_change(1, 'H')}  {_fmt_change(24, 'D')}  {_fmt_change(168, 'W')}  {_fmt_change(720, 'M')}    "
        f"{_fmt_total_delta(1, '\u0394 1h')}  {_fmt_total_delta(24, '\u0394 1d')}  {_fmt_total_delta(168, '\u0394 1w')}"
    )
    console.print(Panel(summary, style="bold white"))
    console.print(table)
