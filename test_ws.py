import asyncio
import json
import time
import unittest
from types import SimpleNamespace

import ws_feed


class BookTests(unittest.TestCase):
    def test_snapshot_and_deltas(self):
        b = ws_feed.Books()
        ws_feed.handle_market(b, [{"event_type": "book", "asset_id": "Y",
                                   "bids": [{"price": "0.26", "size": "100"}],
                                   "asks": [{"price": "0.29", "size": "50"}]}])
        ws_feed.handle_market(b, {"event_type": "price_change", "price_changes": [
            {"asset_id": "Y", "side": "BUY", "price": "0.27", "size": "10"},
            {"asset_id": "Y", "side": "SELL", "price": "0.29", "size": "0"},
            {"asset_id": "Y", "side": "SELL", "price": "0.30", "size": "5"}]})
        book = b.get("Y")
        self.assertEqual({l["price"] for l in book["bids"]}, {"0.26", "0.27"})
        self.assertEqual([l["price"] for l in book["asks"]], ["0.3"])

    def test_delta_without_snapshot_ignored(self):
        b = ws_feed.Books()
        ws_feed.handle_market(b, {"event_type": "price_change", "price_changes": [
            {"asset_id": "Y", "side": "BUY", "price": "0.27", "size": "10"}]})
        self.assertIsNone(b.get("Y"))

    def test_user_event_wakes(self):
        ws_feed.wake.clear()
        self.assertTrue(ws_feed.handle_user({"event_type": "trade", "id": "t"}))
        self.assertTrue(ws_feed.wake.is_set())
        ws_feed.wake.clear()
        self.assertFalse(ws_feed.handle_user({"event_type": "other"}))


class FakeWS:
    def __init__(self, messages):
        self.sent, self.q = [], list(messages)

    async def send(self, m):
        self.sent.append(m)

    async def recv(self):
        if self.q:
            return self.q.pop(0)
        await asyncio.sleep(0.05)
        return "PONG"

    async def close(self):
        pass


class FeedTests(unittest.TestCase):
    def test_feed_connects_subscribes_and_serves_book(self):
        snap = json.dumps([{"event_type": "book", "asset_id": "Y",
                            "bids": [{"price": "0.4", "size": "9"}], "asks": [{"price": "0.42", "size": "9"}]}])
        made = []

        async def connect(url):
            ws = FakeWS([snap] if url.endswith("market") else [json.dumps({"event_type": "trade"})])
            made.append((url, ws))
            return ws

        creds = SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")
        feed = ws_feed.Feed(creds=creds, log=lambda *_: None, connect=connect)
        feed.follow({"Y"}, {"C"})
        feed.start()
        deadline = time.time() + 5
        while time.time() < deadline and (feed.book("Y") is None or not feed.user_events):
            time.sleep(0.05)
        feed.stop()
        self.assertEqual(feed.book("Y")["bids"][0]["price"], "0.4")
        self.assertGreaterEqual(feed.user_events, 1)
        subs = {u.rsplit("/", 1)[1]: json.loads(ws.sent[0]) for u, ws in made}
        self.assertEqual(subs["market"]["assets_ids"], ["Y"])
        self.assertNotIn("markets", subs["user"])
        self.assertIsNone(feed.book("other"))

    def test_stale_feed_means_rest(self):
        feed = ws_feed.Feed(log=lambda *_: None)
        feed.follow({"Y"}, set())
        feed.books.snapshot("Y", [{"price": "0.4", "size": "1"}], [{"price": "0.5", "size": "1"}])
        feed.connected["market"] = True
        feed.alive["market"] = time.monotonic() - 100
        self.assertIsNone(feed.book("Y"))
        feed.alive["market"] = time.monotonic()
        self.assertIsNotNone(feed.book("Y"))


class LifecycleTests(unittest.TestCase):
    def test_new_market_and_resolved(self):
        b = ws_feed.Books()
        before = ws_feed.lifecycle["new_markets"]
        ws_feed.handle_market(b, {"event_type": "new_market", "market": "0xN"})
        ws_feed.handle_market(b, {"event_type": "market_resolved", "market": "0xR"})
        self.assertEqual(ws_feed.lifecycle["new_markets"], before + 1)
        self.assertIn("0xR", ws_feed.lifecycle["resolved"])

    def test_subscription_enables_custom_features(self):
        f = ws_feed.Feed(log=lambda *_: None)
        f.follow({"Y"}, set())
        self.assertTrue(f._subscription("market")["custom_feature_enabled"])


class FindFillsGrace(unittest.TestCase):
    def test_null_get_order_for_fresh_post_is_not_unresolved(self):
        from unittest.mock import MagicMock, patch
        from test_mm_bot import bot, market
        maker = bot.Maker.__new__(bot.Maker)
        maker.market = market()
        maker.client = MagicMock()
        maker.client.get_order.return_value = None
        maker.unresolved_orders = set()
        maker.cancelled_ids = set()
        maker.pending_hedge = None
        maker.tracked = {"o1": {"label": "BUY Yes 25 @ 0.3", "matched": 0.0, "posted_at": time.time()}}
        with patch.object(bot, "log") as log:
            self.assertFalse(maker.find_fills([]))
        self.assertEqual(maker.unresolved_orders, set())
        maker.tracked["o1"]["posted_at"] = time.time() - 120
        with patch.object(bot, "log"), patch.object(maker, "save_state"):
            maker.find_fills([])
        self.assertIn("o1", maker.unresolved_orders)


class FreshBook(unittest.TestCase):
    def test_uses_ws_then_rest(self):
        from unittest.mock import patch
        from test_mm_bot import bot
        feed = ws_feed.Feed(log=lambda *_: None)
        feed.follow({"Y"}, set())
        feed.books.snapshot("Y", [{"price": "0.3", "size": "5"}], [{"price": "0.34", "size": "5"}])
        feed.connected["market"] = True
        feed.alive["market"] = time.monotonic()
        with patch.object(ws_feed, "FEED", feed), patch.object(bot, "get") as g:
            self.assertEqual(bot.fresh_book("Y")["bids"][0]["price"], "0.3")
            self.assertAlmostEqual(bot.live_mid("Y"), 0.32)
            g.assert_not_called()
            feed.connected["market"] = False
            bot.fresh_book("Y")
            g.assert_called_once()


class OrderStateFallback(unittest.TestCase):
    def test_ws_order_state_used_when_rest_null(self):
        from unittest.mock import MagicMock, patch
        from test_mm_bot import bot, market
        ws_feed.handle_user({"event_type": "order", "id": "oX", "type": "UPDATE",
                             "status": "MATCHED", "size_matched": "25", "original_size": "25"})
        self.assertEqual(ws_feed.order_state("oX")["size_matched"], "25.0")
        maker = bot.Maker.__new__(bot.Maker)
        maker.market = market()
        maker.client = MagicMock()
        maker.client.get_order.return_value = None
        maker.unresolved_orders, maker.cancelled_ids, maker.pending_hedge = set(), set(), None
        maker.tracked = {"oX": {"label": "BUY Yes 25 @ 0.3", "matched": 0.0, "posted_at": 0}}
        with patch.object(maker, "_account_fill") as acc, patch.object(bot, "log"), \
                patch.object(maker, "save_state"), patch.object(maker, "_record_order_gone"):
            self.assertTrue(maker.find_fills([]))
        acc.assert_called_once()
        ws_feed.handle_user({"event_type": "order", "id": "oY", "type": "CANCELLATION", "size_matched": "0"})
        self.assertEqual(ws_feed.order_state("oY")["status"], "CANCELED")


def test_trade_event_records_maker_fill_once():
    import ws_feed
    msg = {"event_type": "trade", "id": "t-lag", "status": "MATCHED", "size": "20",
           "taker_order_id": "0xtaker", "maker_orders": [{"order_id": "0xmaker-lag", "matched_amount": "20"}]}
    ws_feed.handle_user([msg])
    ws_feed.handle_user([dict(msg, status="MINED")])
    ws_feed.handle_user([dict(msg, status="CONFIRMED")])
    assert ws_feed.ws_matched("0xmaker-lag") == 20.0
    ws_feed.handle_user([dict(msg, id="t-lag2", maker_orders=[{"order_id": "0xmaker-lag", "matched_amount": "5"}])])
    assert ws_feed.ws_matched("0xmaker-lag") == 25.0
    assert ws_feed.wake.is_set()


def test_failed_trade_ignored():
    import ws_feed
    ws_feed.handle_user([{"event_type": "trade", "id": "t-f", "status": "FAILED",
                          "maker_orders": [{"order_id": "0xm-f", "matched_amount": "9"}]}])
    assert ws_feed.ws_matched("0xm-f") == 0.0
