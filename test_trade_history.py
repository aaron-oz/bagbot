import unittest
import math
import time
import trade_history


class TestTradeHistory(unittest.TestCase):

    def setUp(self):
        trade_history.init_db(':memory:')

    def test_single_buy_cost_basis(self):
        trade_history.record_trade('buy', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        basis = trade_history.get_cost_basis(10)
        self.assertIsNotNone(basis)
        self.assertAlmostEqual(basis['avg_buy_price'], 0.01)
        self.assertAlmostEqual(basis['total_tao_invested'], 1.0)
        self.assertAlmostEqual(basis['total_alpha_held'], 100.0)
        self.assertAlmostEqual(basis['realized_pnl'], 0.0)

    def test_multiple_buys_weighted_average(self):
        # Buy 100 alpha at 0.01 (1 TAO), then 200 alpha at 0.02 (4 TAO)
        trade_history.record_trade('buy', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        trade_history.record_trade('buy', 10, 4.0, 200.0, 0.02, 0.001, 'hotkey1')
        basis = trade_history.get_cost_basis(10)
        # Total: 5 TAO invested, 300 alpha held, avg = 5/300 = 0.01667
        self.assertAlmostEqual(basis['total_tao_invested'], 5.0)
        self.assertAlmostEqual(basis['total_alpha_held'], 300.0)
        self.assertAlmostEqual(basis['avg_buy_price'], 5.0 / 300.0, places=6)

    def test_sell_reduces_basis_proportionally(self):
        # Buy 100 alpha for 1 TAO (avg cost 0.01)
        trade_history.record_trade('buy', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        # Sell 50 alpha for 0.75 TAO (price went up to 0.015)
        trade_history.record_trade('sell', 10, 0.75, 50.0, 0.015, 0.001, 'hotkey1')
        basis = trade_history.get_cost_basis(10)
        # Sold 50% of holdings: cost_of_sold = 1.0 * 0.5 = 0.5
        # realized_pnl = 0.75 - 0.5 = 0.25
        # remaining: 0.5 TAO invested, 50 alpha held
        self.assertAlmostEqual(basis['realized_pnl'], 0.25)
        self.assertAlmostEqual(basis['total_tao_invested'], 0.5)
        self.assertAlmostEqual(basis['total_alpha_held'], 50.0)
        self.assertAlmostEqual(basis['avg_buy_price'], 0.01)
        self.assertAlmostEqual(basis['total_tao_received'], 0.75)

    def test_sell_all_zeros_out(self):
        trade_history.record_trade('buy', 10, 2.0, 200.0, 0.01, 0.001, 'hotkey1')
        # Sell all at same price
        trade_history.record_trade('sell', 10, 2.0, 200.0, 0.01, 0.001, 'hotkey1')
        basis = trade_history.get_cost_basis(10)
        self.assertAlmostEqual(basis['total_alpha_held'], 0.0)
        self.assertAlmostEqual(basis['total_tao_invested'], 0.0)
        self.assertAlmostEqual(basis['realized_pnl'], 0.0)
        self.assertAlmostEqual(basis['avg_buy_price'], 0.0)

    def test_multiple_subnets_independent(self):
        trade_history.record_trade('buy', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        trade_history.record_trade('buy', 20, 2.0, 50.0, 0.04, 0.001, 'hotkey1')

        basis_10 = trade_history.get_cost_basis(10)
        basis_20 = trade_history.get_cost_basis(20)

        self.assertAlmostEqual(basis_10['total_tao_invested'], 1.0)
        self.assertAlmostEqual(basis_10['total_alpha_held'], 100.0)
        self.assertAlmostEqual(basis_20['total_tao_invested'], 2.0)
        self.assertAlmostEqual(basis_20['total_alpha_held'], 50.0)

    def test_no_trades_returns_none(self):
        basis = trade_history.get_cost_basis(99)
        self.assertIsNone(basis)

    def test_portfolio_summary(self):
        trade_history.record_trade('buy', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        trade_history.record_trade('buy', 20, 2.0, 50.0, 0.04, 0.001, 'hotkey1')
        trade_history.record_trade('sell', 10, 0.75, 50.0, 0.015, 0.001, 'hotkey1')

        summary = trade_history.get_portfolio_summary()
        # sn10: 0.5 TAO still invested (sold half), sn20: 2.0 TAO invested
        self.assertAlmostEqual(summary['total_invested'], 2.5)
        self.assertAlmostEqual(summary['total_received'], 0.75)
        self.assertAlmostEqual(summary['realized_pnl'], 0.25)

    def test_get_all_cost_bases(self):
        trade_history.record_trade('buy', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        trade_history.record_trade('buy', 20, 2.0, 50.0, 0.04, 0.001, 'hotkey1')

        bases = trade_history.get_all_cost_bases()
        self.assertIn(10, bases)
        self.assertIn(20, bases)
        self.assertEqual(len(bases), 2)

    def test_sell_with_no_prior_holdings(self):
        """Selling when we have no tracked buys (pre-existing stake)."""
        trade_history.record_trade('sell', 10, 0.5, 50.0, 0.01, 0.001, 'hotkey1')
        basis = trade_history.get_cost_basis(10)
        self.assertAlmostEqual(basis['realized_pnl'], 0.5)
        self.assertAlmostEqual(basis['total_tao_received'], 0.5)

    def test_realized_pnl_loss(self):
        """Selling at a loss should give negative realized P&L."""
        trade_history.record_trade('buy', 10, 2.0, 100.0, 0.02, 0.001, 'hotkey1')
        # Sell all at half the price
        trade_history.record_trade('sell', 10, 1.0, 100.0, 0.01, 0.001, 'hotkey1')
        basis = trade_history.get_cost_basis(10)
        # cost_of_sold = 2.0, received = 1.0, pnl = -1.0
        self.assertAlmostEqual(basis['realized_pnl'], -1.0)

    def test_portfolio_snapshot_and_delta(self):
        """Portfolio delta tracks total bag value change minus transfers."""
        now = int(time.time())
        # For 1h lookback: earlier window centers at now-3600 (±450s)
        # recent window = [now-900, now], trade_flow window = between them
        # Snapshot in earlier window
        trade_history.record_portfolio_snapshot(10.0, 90.0, timestamp=now - 3600)
        # Snapshot in recent window
        trade_history.record_portfolio_snapshot(8.0, 95.0, timestamp=now)
        # Buy happened between windows (explains the balance drop)
        trade_history.record_trade('buy', 10, 2.0, 100.0, 0.02, 0.001, 'hotkey1',
                                   timestamp=now - 2000)

        delta, pct = trade_history.get_portfolio_delta(1)
        # total_now=103, total_then=100, raw_delta=3
        # balance_change = 8 - 10 = -2
        # trade_flow = -2 (buy between windows)
        # net_transfers = -2 - (-2) = 0
        # delta = 3 - 0 = 3
        self.assertAlmostEqual(delta, 3.0)
        self.assertAlmostEqual(pct, 3.0)  # 3/100 * 100 = 3%

    def test_portfolio_delta_with_transfer_between_windows(self):
        """TAO transferred in between windows should be subtracted."""
        now = int(time.time())
        trade_history.record_portfolio_snapshot(10.0, 90.0, timestamp=now - 3600)
        # Balance jumped by 10 between windows (transfer in)
        trade_history.record_portfolio_snapshot(20.0, 90.0, timestamp=now)

        delta, pct = trade_history.get_portfolio_delta(1)
        # raw_delta = 110 - 100 = 10
        # balance_change = 20 - 10 = 10, trade_flow = 0
        # net_transfers = 10, delta = 10 - 10 = 0
        self.assertAlmostEqual(delta, 0.0)

    def test_portfolio_delta_with_transfer_out(self):
        """TAO transferred out between windows should not count as a loss."""
        now = int(time.time())
        trade_history.record_portfolio_snapshot(10.0, 90.0, timestamp=now - 3600)
        trade_history.record_portfolio_snapshot(5.0, 90.0, timestamp=now)

        delta, pct = trade_history.get_portfolio_delta(1)
        # raw_delta = -5, net_transfers = -5, delta = 0
        self.assertAlmostEqual(delta, 0.0)

    def test_portfolio_delta_trade_in_window_not_double_counted(self):
        """Trades within an averaging window don't leak into trade_flow."""
        now = int(time.time())
        # Two snapshots in earlier window: pre and post buy
        trade_history.record_portfolio_snapshot(10.0, 90.0, timestamp=now - 3700)
        trade_history.record_portfolio_snapshot(8.0, 92.0, timestamp=now - 3500)
        # Buy happened INSIDE the earlier window
        trade_history.record_trade('buy', 10, 2.0, 100.0, 0.02, 0.001, 'hotkey1',
                                   timestamp=now - 3600)
        # Recent window
        trade_history.record_portfolio_snapshot(8.0, 95.0, timestamp=now)

        delta, pct = trade_history.get_portfolio_delta(1)
        # The buy is within the earlier window so trade_flow between windows = 0
        # Earlier avg: bal=9, staked=91, total=100
        # Recent: bal=8, staked=95, total=103
        # raw_delta=3, bal_change=-1, trade_flow=0, net_transfers=-1
        # delta = 3 - (-1) = 4
        self.assertIsNotNone(delta)
        self.assertGreater(delta, 0)

    def test_portfolio_delta_no_data(self):
        """Returns None when no snapshots exist."""
        delta, pct = trade_history.get_portfolio_delta(1)
        self.assertIsNone(delta)

    def test_snapshots_bulk(self):
        """Bulk snapshot recording works."""
        now = int(time.time())
        entries = [
            (10, 100.0, 0.01, 0.8),
            (20, 50.0, 0.02, 0.5),
        ]
        trade_history.record_snapshots_bulk(entries, timestamp=now)
        conn = trade_history._get_conn()
        count = conn.execute("SELECT COUNT(*) FROM position_snapshots").fetchone()[0]
        self.assertEqual(count, 2)

    def test_cleanup_removes_old_data(self):
        """Cleanup removes both position and portfolio snapshots."""
        now = int(time.time())
        old = now - 15 * 86400  # 15 days ago
        trade_history.record_snapshot(10, 100.0, 0.01, 0.8, timestamp=old)
        trade_history.record_snapshot(10, 100.0, 0.02, 0.8, timestamp=now)
        trade_history.record_portfolio_snapshot(10.0, 90.0, timestamp=old)
        trade_history.record_portfolio_snapshot(10.0, 95.0, timestamp=now)

        trade_history.cleanup_old_snapshots(max_age_days=10)

        conn = trade_history._get_conn()
        pos_count = conn.execute("SELECT COUNT(*) FROM position_snapshots").fetchone()[0]
        port_count = conn.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0]
        self.assertEqual(pos_count, 1)  # only recent survives
        self.assertEqual(port_count, 1)


if __name__ == '__main__':
    unittest.main()
