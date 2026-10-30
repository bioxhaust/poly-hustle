import rewards_scan as r

def test_missing_end_date_skips_not_crash(monkeypatch):
    def fake_get(url, tries=4):
        return {"accepting_orders": True, "closed": False, "end_date_iso": None, "tokens": []}
    monkeypatch.setattr(r, "get", fake_get)
    cfg = {"condition_id": "c", "rewards_min_size": 50, "rewards_max_spread": 3, "total_daily_rate": 100}
    assert r.score(cfg, 100, 25) == (None, "no end date")

