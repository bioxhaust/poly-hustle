# poly-hustle experiments

Each variant: settings, start time, then results (rewards, hedge losses, net, fills, markets) when it ends.

## Variant A — VOL_MAX 0.10 + midpoint range 0.10–0.90
- Start: 2026-10-29 09:28 UTC
- Settings: VOL_MAX=0.10 (drop-in vol-max.conf), MIN_MID=0.10 / MAX_MID=0.90 (rewards_scan.py default, b8d7a190d733), OFFSET=0.01, SWITCH_GAIN=1.15, MAX_EXIT_LOSS_USD=1.5, LOSS_LIMIT off, whole-share sizes.
- Baseline before A (Oct 26 to Oct 29 morning, from earnings API + hedge log): volatile markets (6h move >8c) rewards $11.44, losses $13.78, net -$2.34 over ~31h; calm markets rewards $2.22, losses $0.36, net +$1.85 over ~10h.
- Account at start: loss counter 19.97, cash ~$27.94.
- Results: (pending)

## Variant B — start 2026-10-29 11:07:51 UTC (14:07:51 MSK)
Ends variant A (A ran 2026-10-29 09:28 UTC -> 2026-10-29 11:07 UTC). Keeps VOL_MAX=0.10 (6h), mid 0.10-0.90, OFFSET 0.01.
Changes: reject no-history markets (except current); hedge FAK only within MAX_PAIR 1.02 (old OR-branch with exit-loss removed), MAX_EXIT_LOSS_USD=0.5;
repair bid max 60s then emergency cheapest exit (cap EMERGENCY_EXIT_USD=3.0) + 24h blacklist; 1h range VOL1H_MAX=0.03 at scan + every 60s live (1h ban);
fast-move pull JUMP=0.02/30s, pause 600s; SWITCH_GAIN=1.3 + SWITCH_MIN_GAIN=0.5 $/day, MIN_DWELL=6h, EST_CALIB=0.5, MIN_EST_DAY=1.5.
Hashes: mm_bot.py 942215384a44, rewards_scan.py 3b0a2d199edc. Backup: bak-20261029-1407. Baseline at start: loss 19.97, cash 27.94.
- B.1 hotfix 2026-10-29 11:20:25 UTC: first B event (White House full-lid, BUY Yes 24@0.16 filled in an 8.5c drop, emergency SELL 23.07@0.04, loss $3.00) -> EMERGENCY_EXIT_USD 3.0->0.5 (no exit within cap: hold + keep repair bid), sub-min dust written off, fast pull: live WS mid >=1.5c from quoted mid pulls within ~1s (FAST_PULL_MOVE=0.015; WS wakes loop on >=1c moves). Backup bak-20261029-1420.

### Variant A — results (logged 2026-10-29 14:35 MSK by the 14:00 experiments run)
- Window: 12:28 → 14:07 MSK (~1.65 h). Markets: will-yisrael-beiteinu (till 13:12) then white-house full-lid Nov-1.
- Rewards (earnings API "today"): $1.79 → $2.03 = +$0.24. Hedge losses: $0. Fills: 0. Net +$0.24 ≈ +$0.15/h.
- Caveat: very short, no fills — noisy. The white-house market A picked crashed 2 min after A ended (see B).

### Variant B — first event
- 14:09:56 MSK fill BUY Yes 24@0.16 on white-house full-lid while mid fell 0.165→0.07 (news jump). Repair bid No@0.86 (pair cap) for 60s, then emergency SELL Yes 23.07@0.04, loss $3.00 (cap hit), market banned 24h.
- Bug found: 0.93 Yes fee remainder (< min_order 5) left the bot in a "hedge blocked" loop (no quotes, no switch) ~10 min.
  Fix: excess below exchange minimum after the repair window expires is written off as dust (mm_bot.py dust_writeoff, test_dust.py 4 tests; other suites unchanged vs baseline). Deployed 14:20 MSK (hash 0b8bf8d40c2e), backups bak-20261029-1420 / -1429.
- Since 14:24 MSK on will-the-democrats-be-in-the-next-israeli-government, BUY Yes 21@0.26 / No 21@0.73. Cash 24.98, loss counter 22.97, stats net -$3.66 (rewards 4.20 / losses 7.87).
- B baseline for its own numbers: today $2.03 at 14:07, loss counter 19.97. B runs until the 16:00 run; next: VOL_MAX 0.08 on top of B.

### Variant "wide entry" — start 2026-10-29 11:55 UTC (14:55 MSK), user request (more markets)
New drop-in wide-entry.conf overrides variant-b.conf entry gates (variant-b.conf/vol-max.conf untouched): VOL1H_MAX 0.03->1 (off), MIN_EST_DAY 1.5->0, EST_CALIB 0.5->1, MIN_DWELL 6h->0, SWITCH_MIN_GAIN 0.5->0, REJECT_NOHISTORY 1->0.
Kept: VOL_MAX 0.10 (experiment), mid 0.10-0.90, SWITCH_GAIN 1.3, dust write-off, FAST_PULL_MOVE 0.015, JUMP pull, EMERGENCY_EXIT_USD 0.5, hedge pair<=1.02, no loss limit. No code change (rewards_scan 3b0a2d199edc, mm_bot 0b8bf8d40c2e). Risk/reward + min-volume gate drafted then dropped per user, never deployed.
Live scan ($25 budget): before 3 candidates pass, after 11. Baseline: loss 23.18, today $2.08, cash 24.77. Backup bak-20261029-1454.

### Capital change + multi-market — 2026-10-29 12:11 UTC (15:11 MSK)
User deposited ~$75: cash 24.77 -> 101.02 (bot log). CONFOUNDER: results after this point are not comparable with earlier variants (more cash, bigger min_size markets, 2-3 markets).
New drop-in multi-market.conf: MAX_MARKETS 4->3, HEDGE_RESERVE_PCT 0.20->0.25, MAX_ORDER_SIZE 200->30 (min_size still overrides). wide-entry + safety unchanged. Backup bak-20261029-1511.
Scan crash with $100 (AttributeError on markets without end_date_iso, newly affordable) was fixed by a parallel edit (rewards_scan a1282baeb7a7, restart 12:16 UTC); test_noend.py added.
12:23 UTC: portfolio slots 3, open 2: yisrael-beiteinu 25/25 (Yes .34/No .65) + will-anthropic-ipo-on-november-18-2026 50/50 (Yes .22/No .76), reserve $25.

### Multi-market v2 — 2026-10-29 12:29 UTC (15:29 MSK)
Verified: hedge() cancels our resting orders first (mm_bot.py:1614-1620, "after conflicting orders are confirmed gone"), then free_cash() = balance minus BUYs held in OTHER markets (mm_bot.py:2282-2291), so the cancelled opposite bid funds the hedge; reserve only covers pair overrun (<=2c/share) + emergency <=$0.5 per simultaneous episode.
multi-market.conf: MAX_MARKETS 4, HEDGE_RESERVE_PCT 0.25->0.08 ($8), SIZE 25->20 (slots=(cash-reserve)//SIZE = 4), MAX_ORDER_SIZE 30. Backup bak-20261029-1529.
15:30:46 MSK fill on yisrael-beiteinu: Yes 20@0.34 (mid fell to 0.30); repair bid No@0.68 60s, emergency FAK No 10@0.71 ($0.50 cap), holding 10 Yes + repair bid No 10@0.68; blacklisted 24h. NOTE: WS MATCHED at 15:30:46 but fill processed 15:32:37 (~2 min lag) - investigate.
15:36: portfolio slots 4, open 2, adding 1 -> anthropic-ipo 50/50, cz-binance-tweets 27/27, plus yisrael repair. 4th blocked by cash.

### Monitor 2026-10-29 15:10 UTC (18:10 MSK) — no change
Service active since 14:17 UTC, 0 restarts; one transient CLOB ConnectionTerminated at 15:07:20 (orders pulled, re-posted within 2s). Quoting 2 markets: tyreek-hill-chiefs (Yes .27/No .72, 30/30), cz-binance-tweets 20-39 (Yes .67/No .31, 21/21); yisrael-beiteinu still holding 10 Yes @.34 + repair bid No 10@.68 (pair cap), mid .245, by design.
Today (UTC): rewards $3.12, loss counter 16.72 -> 25.81 (+9.09), net ~ -$5.97 (mostly earlier variants). Since wide-entry (11:55 UTC): rewards +$1.04, losses +$2.63 (two-sided fills at pair 1.02, e.g. exactly-2-elections 41.67 sh, plus yisrael emergency). LLM layer working (vetoes on Israeli-politics/Iran/elections, no errors). Decision: leave as is, sample too small for another variant.

### Monitor 2026-10-29 17:07 UTC (20:07 MSK) — no change, daily summary sent
Service active since 15:56 UTC. Quoting 3 markets: houthis-taiz (50/50 Yes .29/No .70), cz-binance-tweets 20-39 (20/20 Yes .68/No .31), plus a 20/20 market at mid .275 (Yes .27/No .72). Portfolio $97.42, reserve $8.
Since 18:10 MSK: rewards $3.12 -> $3.80 (+$0.68), loss counter 25.81 unchanged (gemini-carbon partial fill 5.03 No@.83 hedged by SELL @.83, loss 0). Transient CLOB heartbeat/EAGAIN errors at 16:37 UTC, recovered.
Today UTC: rewards $3.80, losses $9.09, net ~ -$5.29. Stats cumulative: rewards $5.70 - losses $10.71 = -$5.01. LLM vetoing Iran/Israel-politics/elections, no LLM errors.

## 2026-10-29 19:15 UTC: uncertain-submit stall fix
503 {"error":"trading is disabled"} was treated as ambiguous; pending_hedge stuck forever, all 3 books froze silently 18:30-19:15 UTC. Now order_rejected() treats it as a definite reject, and an uncertain submit with no open order and no trades is dropped after UNCERTAIN_GIVEUP_SECONDS (300). Backup bak-20261029-1914-uncertain. test_ws 3 failures are pre-existing.

## 2026-10-30 03:03 UTC (06:03 MSK): duplicate-fill bug fix
anthropic-ipo-nov-18: BUY No 22@0.75 filled 01:39:01 UTC; REST still listed the order, track() re-added it with matched 0, and find_fills charged the same trade (cb786536) again -> lots No 44. Immediate hedge bought Yes 40@0.265 (real: Yes 40 / No 22, 18 Yes over-hedged). Balance check (HEDGE_BAL_TOL) then froze ALL quoting 01:40-03:03 UTC ("hedge balances not confirmed").
Fix: _account_fill dedupes by order_id (per-process charged map), test_dupfill.py. state.json anthropic No lots [[22,.75],[22,.75]] -> [[22,.75]]. Backup bak-20261030-0615-dupfill. Restarted 03:03:38 UTC: bot sees Yes 40/No 22, repair bid BUY No 18@0.75 (pair cap 1.02), mid 0.205. test_ws 3 pre-existing failures unchanged.

### Monitor 2026-10-30 09:10 UTC (12:10 MSK) — no change
Restart 07:53 UTC after manual close of anthropic/pickleball leftovers (backup state.json.bak-pre-close2-20261030-0752). 08:01 UTC adverse fill on dicks-sporting-goods-q3 comparable-sales 3-4% (thin book, touch 165/170): BUY Yes 24@0.53, mid fell to ~0.42; repair bid No 24@0.49 (pair cap 1.02), emergency close would exceed $0.5 cap -> holding, blacklisted 24h (~$2.6 unrealized). Other slots quoting: bank-of-canada-jan-2027 no-change (22/22 Yes .45/No .54) and umich-sentiment 46-48.9 (21/21; one two-sided fill merged 20.95 at 08:49). Stats net -$5.69 (rewards $8.97 / losses $14.66). LLM vetoes working, one transient telegram URLError, 429 on data-api once.
