import unittest, mm_bot

class DupFill(unittest.TestCase):
    def test_same_order_charged_once_after_retrack(self):
        bot = mm_bot.Maker.__new__(mm_bot.Maker)
        self.assertIsNotNone(bot)
        calls = []
        bot.apply_fill = lambda token, side, price, delta: calls.append(delta) or 0.0
        bot._record_fill = lambda *a, **k: None
        bot._fill_text = lambda *a, **k: ""
        mm_bot.note = lambda *a, **k: None
        mk = lambda: {"token": "t", "side": "BUY", "price": 0.75, "matched": 0.0, "size": 22.0, "label": "x"}
        bot._account_fill(mk(), 22.0, order_id="0xabc")
        bot._account_fill(mk(), 22.0, order_id="0xabc")   # re-tracked with matched 0
        self.assertEqual(calls, [22.0])
        bot._account_fill(mk(), 30.0, order_id="0xabc")   # genuine extra fill
        self.assertEqual(calls, [22.0, 8.0])
        bot._account_fill(mk(), 5.0, order_id=None)       # trades path unaffected
        self.assertEqual(calls, [22.0, 8.0, 5.0])

if __name__ == "__main__":
    unittest.main()
