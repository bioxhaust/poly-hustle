import time, unittest, threading
import mm_bot, merge

Y, N, C = "y", "n", "0x" + "11" * 32


def bot(yes_lots, no_lots, balances=None):
    b = mm_bot.Maker.__new__(mm_bot.Maker)
    b.market = {"yes": Y, "no": N, "condition": C, "min_order": 5, "question": "q", "neg_risk": False}
    b.lots = {Y: [list(l) for l in yes_lots], N: [list(l) for l in no_lots]}
    b.pending_hedge, b.unresolved_orders = None, set()
    b.inventory_ready, b.merge_job, b.merge_failed_at = True, None, 0.0
    b.dust_writeoff, b.client, b.data_at = None, None, None
    b.save_state = lambda: None
    tot = {Y: sum(l[0] for l in yes_lots), N: sum(l[0] for l in no_lots)}
    b.balances = lambda reported: dict(balances or tot)
    return b


class FakeMerger:
    def __init__(self): self.calls = []
    def available(self): return True
    def merge(self, cond, amount, **kw):
        self.calls.append(amount); return {"ok": True, "tx": "0xtx"}


class RepairMerge(unittest.TestCase):
    def setUp(self):
        self.saved = (mm_bot.MERGER, mm_bot.reported_positions, mm_bot.refresh_balances,
                      mm_bot.record_event, mm_bot.DRY_RUN)
        mm_bot.MERGER = FakeMerger()
        mm_bot.reported_positions = lambda m: {Y: (0, 0), N: (0, 0)}
        mm_bot.refresh_balances = lambda *a, **k: None
        mm_bot.record_event = lambda *a, **k: None
        mm_bot.DRY_RUN = False

    def tearDown(self):
        (mm_bot.MERGER, mm_bot.reported_positions, mm_bot.refresh_balances,
         mm_bot.record_event, mm_bot.DRY_RUN) = self.saved

    def run_merge(self, b, ours=()):
        self.assertTrue(b.repair_merge_step(list(ours)))
        b.merge_job["thread"].join(2)
        self.assertTrue(b.repair_merge_step(list(ours)))
        self.assertIsNone(b.merge_job)

    def test_merges_balanced_part_keeps_excess_and_newest_basis(self):
        b = bot([[22, 0.27], [18, 0.30]], [[22, 0.75]])
        b.client = object()
        cancelled = []
        b.cancel = lambda o: cancelled.append(o)
        self.assertTrue(b.repair_only())
        self.run_merge(b, ours=[{"id": "repair"}])
        self.assertEqual(mm_bot.MERGER.calls, [22])
        self.assertEqual(cancelled, [])               # repair bid kept
        self.assertEqual(b.lots[Y], [[18, 0.30]])     # excess keeps newest lot
        self.assertEqual(b.lots[N], [])
        self.assertAlmostEqual(b.inventory_net(), 18)

    def test_no_merge_when_balances_disagree_or_hedge_in_flight(self):
        b = bot([[40, 0.27]], [[22, 0.75]], balances={Y: 40, N: 10})
        self.assertFalse(b.repair_merge_step([]))
        b = bot([[40, 0.27]], [[22, 0.75]])
        b.pending_hedge = {"id": "x"}
        self.assertFalse(b.repair_merge_step([]))
        self.assertFalse(b.repair_only())

    def test_scanner_not_blocked_by_repair_only_and_slot_free(self):
        b = bot([[40, 0.27]], [[22, 0.75]])
        self.assertFalse(mm_bot._book_blocked([b]))
        b.pending_hedge = {"id": "x"}
        self.assertTrue(mm_bot._book_blocked([b]))
        rows = [{"condition": "c2", "size": 20}]
        old = mm_bot.quote_capital
        mm_bot.quote_capital = lambda row: 10.0
        try:
            got = mm_bot.pack_entries(rows, 1000, {C}, 0, repairing={C})
            self.assertEqual([r["condition"] for r in got], ["c2"])
        finally:
            mm_bot.quote_capital = old


if __name__ == "__main__":
    unittest.main()
