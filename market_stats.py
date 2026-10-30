"""Per-market P&L: reward accruals vs hedge/exit losses, persisted as JSON."""

import json
import os
import threading
import time

PATH = os.getenv("MARKET_STATS_PATH", "market_stats.json")
_lock = threading.Lock()


def load(path=None):
    try:
        with open(path or PATH) as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def record(condition, slug, rewards=0.0, loss=0.0, path=None, now=None):
    """Add reward dollars and/or loss dollars to one market's running totals."""
    if not condition or (abs(rewards) < 1e-9 and abs(loss) < 1e-9):
        return None
    path = path or PATH
    with _lock:
        data = load(path)
        row = data.setdefault(condition, {"slug": slug, "rewards": 0.0, "loss": 0.0,
                                          "first": now or time.time()})
        row["slug"] = slug or row.get("slug")
        row["rewards"] = round(row.get("rewards", 0.0) + rewards, 6)
        row["loss"] = round(row.get("loss", 0.0) + loss, 6)
        row["last"] = now or time.time()
        tmp = f"{path}.tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh, indent=1, sort_keys=True)
        os.replace(tmp, path)
        return row


def summary(path=None, top=5):
    data = load(path)
    if not data:
        return "stats: no data yet"
    rows = sorted(data.values(), key=lambda r: -(r.get("rewards", 0) - r.get("loss", 0)))
    total_r = sum(r.get("rewards", 0) for r in rows)
    total_l = sum(r.get("loss", 0) for r in rows)
    parts = [f"{(r.get('slug') or '?')[:40]} +{r.get('rewards', 0):.2f}/-{r.get('loss', 0):.2f}"
             for r in rows[:top]]
    return (f"stats: rewards ${total_r:.2f} - losses ${total_l:.2f} = net ${total_r - total_l:+.2f} | "
            + " | ".join(parts))


def _meta_path(path=None):
    return f"{path or PATH}.meta"


def spent_seen(path=None):
    """Loss counter value already attributed, persisted across restarts."""
    try:
        with open(_meta_path(path)) as fh:
            return float(json.load(fh)["spent_seen"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def set_spent_seen(value, path=None):
    tmp = f"{_meta_path(path)}.tmp"
    with open(tmp, "w") as fh:
        json.dump({"spent_seen": value}, fh)
    os.replace(tmp, _meta_path(path))
