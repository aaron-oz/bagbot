import json
import time
from pathlib import Path

HISTORY_FILE = Path(__file__).parent / "price_history.json"
MAX_AGE_DAYS = 32  # Keep slightly more than a month


def _load_history():
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text())
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_history(data):
    HISTORY_FILE.write_text(json.dumps(data))


def record_prices(stats):
    """Record current prices for all subnets."""
    history = _load_history()
    now = int(time.time())

    for netuid, subnet_data in stats.items():
        key = str(netuid)
        if key not in history:
            history[key] = []
        history[key].append({"t": now, "p": subnet_data["price"]})

    # Cleanup old entries
    cutoff = now - (MAX_AGE_DAYS * 24 * 3600)
    for key in history:
        history[key] = [e for e in history[key] if e["t"] > cutoff]

    _save_history(history)


def get_price_change(netuid, current_price, hours_ago, window_hours=None):
    """
    Get price direction compared to average price in a window starting at `hours_ago`.

    Args:
        netuid: Subnet ID
        current_price: Current price to compare against
        hours_ago: How far back to start the comparison window
        window_hours: Size of averaging window (defaults to hours_ago for matching windows)

    Returns:
        Float difference (current_price - avg_price), or None if no data.
    """
    if window_hours is None:
        window_hours = hours_ago

    history = _load_history()
    key = str(netuid)

    if key not in history or not history[key]:
        return None

    now = int(time.time())
    window_start = now - ((hours_ago + window_hours) * 3600)
    window_end = now - (hours_ago * 3600)
    entries = history[key]

    # Collect all prices within the window
    window_prices = [e["p"] for e in entries if window_start <= e["t"] <= window_end]

    if not window_prices:
        return None

    avg_price = sum(window_prices) / len(window_prices)

    return current_price - avg_price
