import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import llm_risk

def rows():
    return [{"condition": "a", "slug": "a", "q": "A?", "est_daily": 10},
            {"condition": "b", "slug": "b", "q": "B?", "est_daily": 5}]

class T(unittest.TestCase):
    def setUp(self):
        llm_risk._cache.clear(); llm_risk.ENABLED = True
    def test_disabled_noop(self):
        llm_risk.ENABLED = False
        r = rows(); self.assertIs(llm_risk.apply(r, caller=lambda *a: 1/0), r)
    def test_fail_open(self):
        out = llm_risk.apply(rows(), caller=lambda *a: 1/0, log=lambda m: None)
        self.assertEqual([x["slug"] for x in out], ["a", "b"])
        self.assertNotIn("llm_mult", out[0])
    def test_veto_and_downrank(self):
        v = {"A?": {"risk": 0.9, "veto": True, "reason": "x"}, "B?": {"risk": 0.5, "veto": False, "reason": ""}}
        out = llm_risk.apply(rows(), caller=lambda q, e, m, c=None: v[q], log=lambda m: None)
        self.assertEqual([x["slug"] for x in out], ["b"])
        self.assertAlmostEqual(out[0]["llm_mult"], 1 - llm_risk.PENALTY * 0.5)
    def test_held_vetoed_by_default(self):
        out = llm_risk.apply(rows(), keep={"a"}, caller=lambda *a: {"risk": 1.0, "veto": True, "reason": ""}, log=lambda m: None)
        self.assertEqual(out, [])
    def test_held_not_vetoed_when_flag_off(self):
        llm_risk.HELD_VETO = False
        self.addCleanup(setattr, llm_risk, "HELD_VETO", True)
        out = llm_risk.apply(rows(), keep={"a"}, caller=lambda *a: {"risk": 1.0, "veto": True, "reason": ""}, log=lambda m: None)
        self.assertEqual([x["slug"] for x in out], ["a"])
    def test_rank_uses_mult(self):
        import rewards_scan as rs
        a = {"est_daily": 10, "vol": 0, "slug": "a", "llm_mult": 0.3}
        b = {"est_daily": 5, "vol": 0, "slug": "b"}
        self.assertEqual(sorted([a, b], key=rs.rank_key)[0]["slug"], "b")

class Wiring(unittest.TestCase):
    def test_skip_confirmed(self):
        import rewards_scan as rs
        self.assertTrue(rs.skip_is_confirmed("llm_veto", holding=True))

class Ctx(unittest.TestCase):
    def test_price_context(self):
        import json
        def f(url, timeout=None):
            if "/markets/" in url: return json.dumps({"tokens": [{"token_id": "1"}]}).encode()
            return json.dumps({"history": [{"p": 0.5}, {"p": 0.58}, {"p": 0.55}]}).encode()
        txt = llm_risk.price_context("c", fetch=f)
        self.assertIn("change +0.050", txt); self.assertIn("max step 0.080", txt)
    def test_context_fail_open(self):
        bad = lambda *a, **k: 1/0
        self.assertEqual(llm_risk.price_context("c", fetch=bad), "price history unavailable")
        self.assertEqual(llm_risk.news_context("Will X?", fetch=bad), "news unavailable")
    def test_news_parse(self):
        rss = b"<rss><channel><item><title>Big news</title><pubDate>Thu, 29 Oct 2026 10:00:00 GMT</pubDate></item></channel></rss>"
        self.assertIn("Big news", llm_risk.news_context("Will Anthropic IPO?", fetch=lambda *a, **k: rss))
        self.assertEqual(llm_risk.news_query("Will Anthropic IPO on November 18, 2026?"), "Anthropic IPO November 18 2026")

if __name__ == "__main__":
    unittest.main()
