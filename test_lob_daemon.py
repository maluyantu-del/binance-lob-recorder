"""
lob_daemon.py 的离线测试。不连网, 用一个假的"模拟交易所"生成完全可控的行情。
运行: python3 -m unittest -v test_lob_daemon
"""
import csv
import importlib
import json
import lzma
import os
import random
import shutil
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

TMP = Path(tempfile.mkdtemp(prefix="lobtest_"))
os.environ.update({
    "LOB_DATA_DIR": str(TMP / "data"),
    "LOB_STATE_DIR": str(TMP / "state"),
    "LOB_SYMBOLS": "BTCUSDT",
    "LOB_LEVELS": "5",
    "LOB_ROTATE_SEC": "2",          # 每 2 秒交易所时间换一个文件, 测试换文件和压缩
    "LOB_MIN_FREE_GB": "0",
    "LOB_BUDGET_GB": "100",
})
sys.path.insert(0, str(Path(__file__).resolve().parent))
D = importlib.import_module("lob_daemon")

SYM = "BTCUSDT"
E0 = 1_790_000_000_000


class FakeMarket:
    """真实订单簿的"标准答案"。每次 step 改一个价位, 编号 +1; 增量事件 = 一段编号区间内所有变动价位的最终数量。"""

    def __init__(self, seed=7):
        self.rng = random.Random(seed)
        self.bids = {round(100.00 - k * 0.01, 2): self._q() for k in range(300)}
        self.asks = {round(100.01 + k * 0.01, 2): self._q() for k in range(300)}
        self.uid = 1000
        self.E = E0
        self.snaps = {}

    def _q(self):
        return round(self.rng.uniform(0.001, 5), 5)

    def _step(self, changed):
        self.uid += 1
        side, name = (self.bids, "b") if self.rng.random() < 0.5 else (self.asks, "a")
        k = int(self.rng.expovariate(1 / 6)) % 300           # 大多改在盘口附近
        p = round(100.00 - k * 0.01, 2) if name == "b" else round(100.01 + k * 0.01, 2)
        if self.rng.random() < 0.3 and len(side) > 50:
            side.pop(p, None)
            q = 0.0
        else:
            q = self._q()
            side[p] = q
        changed[(name, p)] = q

    def event(self, n=None, dt=250):
        n = n or self.rng.randint(1, 8)
        U = self.uid + 1
        changed = {}
        for _ in range(n):
            self._step(changed)
        self.E += dt
        self.snaps[self.uid] = self.snapshot()
        return {"e": "depthUpdate", "E": self.E, "s": SYM, "U": U, "u": self.uid,
                "b": [[f"{p:.2f}", f"{q:.5f}"] for (s, p), q in changed.items() if s == "b"],
                "a": [[f"{p:.2f}", f"{q:.5f}"] for (s, p), q in changed.items() if s == "a"]}

    def snapshot(self, limit=5000):
        return {"lastUpdateId": self.uid,
                "bids": [[f"{p:.2f}", f"{q:.5f}"] for p, q in sorted(self.bids.items(), reverse=True)[:limit]],
                "asks": [[f"{p:.2f}", f"{q:.5f}"] for p, q in sorted(self.asks.items())[:limit]]}

    def top(self, n):
        return sorted(self.bids.items(), reverse=True)[:n], sorted(self.asks.items())[:n]


class FakeRest:
    def __init__(self, market):
        self.m = market
        self.override = None
        self.fail = 0
        self.calls = 0

    def depth(self, symbol, limit):
        self.calls += 1
        if self.fail:
            self.fail -= 1
            raise RuntimeError("模拟: 快照接口不通")
        if self.override is not None:
            s, self.override = self.override, None
            return s
        return self.m.snapshot(limit)


class SyncWorker:
    """测试用: 请求一交就当场算出结果, 下次 poll_results 取走。行为和真的后台线程一致, 只是不用等。"""

    def __init__(self, rest):
        self.rest = rest
        self.out = []
        self._busy = set()

    def busy(self, sym, purpose):
        return (sym, purpose) in self._busy

    def request(self, sym, purpose, gen, limit):
        if (sym, purpose) in self._busy:
            return False
        self._busy.add((sym, purpose))
        try:
            snap, err = self.rest.depth(sym, limit), None
        except Exception as e:
            snap, err = None, e
        self.out.append((sym, purpose, gen, limit, snap, err, 0.01))
        return True

    def results(self):
        out, self.out = self.out, []
        for it in out:
            self._busy.discard((it[0], it[1]))
        return out


def raw(ev):
    return json.dumps({"stream": "btcusdt@depth@100ms", "data": ev})


LIVE = []          # 本测试里建过的记录器, 收尾时统一关文件、等后台压缩线程结束


def close_all():
    while LIVE:
        rec = LIVE.pop()
        for w in rec.writers.values():
            w.close(compress=False)
        rec.compressor.drain()


def fresh_dirs():
    close_all()
    for sub in ("data", "state"):
        shutil.rmtree(TMP / sub, ignore_errors=True)


def new_recorder(market):
    fresh_dirs()
    rec = D.Recorder()
    rec.worker = SyncWorker(FakeRest(market))
    LIVE.append(rec)
    return rec


def read_rows():
    rows = []
    for path in D.data_files(D.DATA_DIR / SYM):
        with D.open_text(path) as f:
            r = csv.reader(f)
            next(r)
            rows += [row for row in r]
    return rows


def report_rows():
    D.cmd_report()
    lines = D.REPORT_FILE.read_text(encoding="utf-8").strip().split("\n")
    hdr = lines[0].split("\t")
    return [dict(zip(hdr, l.split("\t"))) for l in lines[1:]]


def breaks(rep):
    return sum(int(r["断号"]) for r in rep)


# ============================================================
class TestBookSync(unittest.TestCase):
    def test_in_order_events_match_truth(self):
        m = FakeMarket(1)
        b = D.BookSync(SYM)
        first = m.event()
        b.feed(first)                               # 没快照时先缓冲
        self.assertEqual(b.pending, [first])
        b.load_snapshot(m.snapshot(), 5000)         # 快照比缓冲的这条新
        self.assertEqual(b.feed(first), "stale")
        for _ in range(500):
            self.assertEqual(b.feed(m.event()), "applied")
        self.assertEqual(b.top(50), m.top(50))
        self.assertEqual(b.bids, m.bids)
        self.assertEqual(b.asks, m.asks)

    def test_first_event_must_straddle_snapshot(self):
        m = FakeMarket(2)
        m.event(n=3)
        e = m.event(n=10)                            # 一条覆盖 10 个编号的事件
        b = D.BookSync(SYM)                          # 快照编号落在这条事件的区间中间
        b.load_snapshot({"lastUpdateId": e["U"] + 3, "bids": [], "asks": []}, 5000)
        self.assertEqual(b.feed(e), "applied")      # U <= L+1 <= u 成立
        self.assertTrue(b.live)

    def test_snapshot_too_old_is_gap(self):
        m = FakeMarket(3)
        old = m.snapshot()
        m.event(); m.event()
        b = D.BookSync(SYM)
        b.load_snapshot(old, 5000)
        self.assertEqual(b.feed(m.event()), "gap")  # 中间缺了两条

    def test_live_gap_detected(self):
        m = FakeMarket(4)
        b = D.BookSync(SYM)
        b.load_snapshot(m.snapshot(), 5000)
        self.assertEqual(b.feed(m.event()), "applied")
        m.event()                                   # 这条"丢了"
        self.assertEqual(b.feed(m.event()), "gap")

    def test_zero_removes_and_unknown_removal_ok(self):
        b = D.BookSync(SYM)
        b.load_snapshot({"lastUpdateId": 10, "bids": [["1.00", "2"]], "asks": [["1.01", "3"]]}, 5000)
        b.feed({"U": 11, "u": 11, "b": [["1.00", "0"], ["0.50", "0"]], "a": []})
        self.assertEqual(b.bids, {})

    def test_coverage_bounds_only_when_snapshot_full(self):
        m = FakeMarket(5)
        b = D.BookSync(SYM)
        b.load_snapshot(m.snapshot(30), 30)          # 给满 30 档 -> 有边界
        self.assertEqual(b.lo_bid, sorted(m.bids, reverse=True)[29])
        b.load_snapshot(m.snapshot(30), 1000)        # 要 1000 只给了 30 -> 这一侧全在里面, 不设边界
        self.assertEqual(b.lo_bid, float("-inf"))


class TestRecorder(unittest.TestCase):
    def tearDown(self):
        close_all()

    def feed(self, rec, m, n, **kw):
        for _ in range(n):
            rec.handle(raw(m.event(**kw)), time.time())
            rec.poll_results()

    def finish(self, rec):
        for w in rec.writers.values():
            w.close(compress=True)
        rec.compressor.drain()

    def test_rows_only_on_top_change_and_continuity(self):
        m = FakeMarket(10)
        rec = new_recorder(m)
        self.feed(rec, m, 400)
        self.finish(rec)
        rows = read_rows()
        st = rec.stats[SYM]
        self.assertGreater(st["applied"], 390)
        self.assertEqual(len(rows), st["rows"])
        self.assertLess(len(rows), st["applied"])     # 前 5 档没变的事件不写
        for a, b in zip(rows, rows[1:]):              # 每行的编号区间首尾相接, 一条不漏
            self.assertEqual(int(b[2]), int(a[3]) + 1)
        bids, asks = m.top(5)                          # 最后一行 = 标准答案的前 5 档
        last = rows[-1]
        self.assertEqual([float(x) for x in last[4:14:2]], [p for p, _ in bids])
        self.assertEqual([float(x) for x in last[14:24:2]], [p for p, _ in asks])
        rep = report_rows()
        self.assertEqual(breaks(rep), 0)
        self.assertEqual(sum(int(r["行数"]) for r in rep), len(rows))
        self.assertEqual(sum(int(r["买卖交叉"]) for r in rep), 0)

    def test_files_rotate_and_compress(self):
        m = FakeMarket(11)
        rec = new_recorder(m)
        self.feed(rec, m, 200)                        # 200 x 250ms = 50 秒 -> 约 25 个 2 秒文件
        self.finish(rec)
        files = list((D.DATA_DIR / SYM).rglob("*"))
        xz = [f for f in files if f.name.endswith(".csv.xz")]
        self.assertGreater(len(xz), 10)
        self.assertFalse([f for f in files if f.name.endswith((".csv", ".tmp"))])
        with lzma.open(xz[0], "rt") as f:
            self.assertEqual(next(csv.reader(f))[:4], ["exch_ms", "local_ms", "U", "u"])

    def test_dropped_message_triggers_resync_and_shows_as_break(self):
        m = FakeMarket(12)
        rec = new_recorder(m)
        self.feed(rec, m, 100)
        m.event()                                     # 丢一条
        self.feed(rec, m, 100)
        self.finish(rec)
        self.assertEqual(rec.stats[SYM]["resyncs"], 1)
        self.assertEqual(rec.books[SYM].bids, m.bids)  # 重对齐后回到标准答案
        self.assertEqual(breaks(report_rows()), 1)     # 文件里如实留下一个断点

    def test_snapshot_older_than_buffer_is_refetched(self):
        m = FakeMarket(13)
        rec = new_recorder(m)
        old = m.snapshot()
        m.event(); m.event()
        rec.worker.rest.override = old                # 第一次给一张过旧的快照
        rec.handle(raw(m.event()), time.time())
        rec.poll_results()
        self.assertFalse(rec.books[SYM].has_snapshot)
        rec.next_sync_at[SYM] = 0
        self.feed(rec, m, 50)                         # 之后拿到正常快照
        self.assertTrue(rec.books[SYM].live)
        self.assertEqual(rec.books[SYM].bids, m.bids)

    def test_snapshot_failure_backs_off_without_overflow(self):
        m = FakeMarket(14)
        rec = new_recorder(m)
        rec.worker.rest.fail = 1
        rec.sync_fails[SYM] = 5000                    # 连续失败很多次也不能算爆
        rec.handle(raw(m.event()), time.time())
        rec.poll_results()
        self.assertFalse(rec.books[SYM].has_snapshot)
        self.assertGreater(rec.next_sync_at[SYM], time.time() + 50)
        self.assertIn("模拟", rec.stats[SYM]["last_snap_err"])

    def test_gap_inside_replayed_buffer_is_visible_in_files(self):
        """审查复现的场景: 缓冲里还有第二个洞时, 文件必须露出两个断点, 不能只露一个。"""
        m = FakeMarket(113)
        rec = new_recorder(m)
        self.feed(rec, m, 60)
        m.event()                                     # 丢失 1
        rec.worker.rest.fail = 1                      # 这次取快照失败, 事件只好先缓冲
        e1 = m.event()
        rec.handle(raw(e1), time.time()); rec.poll_results()
        for _ in range(3):                            # e2..e4 进缓冲
            rec.handle(raw(m.event(n=1)), time.time())
        m.event()                                     # 丢失 2, 洞落在缓冲里
        rec.handle(raw(m.event()), time.time())       # e5 进缓冲
        rec.worker.rest.override = m.snaps[e1["u"]]   # 快照恰好落在 e1 之后 -> e2..e4 会先被回放
        rec.next_sync_at[SYM] = 0
        rec.maybe_sync(SYM, time.time()); rec.poll_results()
        self.assertFalse(rec.books[SYM].has_snapshot)  # 回放到 e5 发现洞, 重新等快照
        rec.next_sync_at[SYM] = 0
        self.feed(rec, m, 80)
        self.finish(rec)
        self.assertTrue(rec.books[SYM].live)
        self.assertEqual(rec.books[SYM].bids, m.bids)
        self.assertEqual(breaks(report_rows()), 2)

    def test_verifier_ok_with_older_and_newer_snapshot(self):
        m = FakeMarket(14)
        rec = new_recorder(m)
        self.feed(rec, m, 60)
        older_uid = sorted(m.snaps)[-20]              # 比当前旧 20 条事件的快照 -> 靠环形缓存回放
        rec.worker.rest.override = m.snaps[older_uid]
        rec.request_verify(SYM); rec.poll_results()
        self.assertEqual(rec.stats[SYM]["verify_ok"], 1)
        rec.request_verify(SYM); rec.poll_results()   # 与当前同编号的快照 -> 等下一条事件再比
        self.assertIn(SYM, rec.verifiers)
        self.feed(rec, m, 1)
        self.assertEqual(rec.stats[SYM]["verify_ok"], 2)
        self.assertEqual(rec.stats[SYM]["verify_mismatch"], 0)

    def test_verifier_catches_and_fixes_corruption(self):
        m = FakeMarket(15)
        rec = new_recorder(m)
        self.feed(rec, m, 60)
        book = rec.books[SYM]
        b = sorted(book.bids, reverse=True)
        fake = round((b[3] + b[4]) / 2, 3)            # 夹在两个真实价位之间, 以后永远不会被增量改到
        book.bids[fake] = 123.456
        self.feed(rec, m, 20)
        self.assertIn(fake, book.bids)                # 增量自己修不好它
        rec.request_verify(SYM); rec.poll_results()
        self.feed(rec, m, 1)
        self.assertEqual(rec.stats[SYM]["verify_mismatch"], 1)
        self.assertNotIn(fake, rec.books[SYM].bids)   # 已用新建簿替换
        self.assertEqual(rec.books[SYM].bids, m.bids)
        rec.request_verify(SYM); rec.poll_results()
        self.feed(rec, m, 1)
        self.assertEqual(rec.stats[SYM]["verify_ok"], 1)

    def test_verify_result_after_reset_is_discarded(self):
        m = FakeMarket(16)
        rec = new_recorder(m)
        self.feed(rec, m, 30)
        rec.worker.request(SYM, "verify", rec.books[SYM].gen, D.VERIFY_LIMIT)   # 请求已发出
        rec.on_disconnect()                                                      # 结果回来前断线了
        rec.poll_results()
        self.assertEqual(rec.stats[SYM]["verify_skip"], 1)
        self.assertEqual(rec.stats[SYM]["verify_ok"], 0)

    def test_crossed_book_triggers_resync_and_is_not_written(self):
        m = FakeMarket(17)
        rec = new_recorder(m)
        self.feed(rec, m, 40)
        rows_before = rec.stats[SYM]["rows"]
        best_ask = min(rec.books[SYM].asks)
        bad = {"e": "depthUpdate", "E": m.E + 100, "s": SYM, "U": m.uid + 1, "u": m.uid + 1,
               "b": [[f"{best_ask + 0.05:.2f}", "1.0"]], "a": []}   # 买价高过卖一
        m.uid += 1
        rec.handle(raw(bad), time.time())
        self.assertEqual(rec.stats[SYM]["rows"], rows_before)      # 这一行没写
        self.assertEqual(rec.stats[SYM]["resyncs"], 1)
        self.assertFalse(rec.books[SYM].live)

    def test_price_leaving_snapshot_coverage_triggers_resync(self):
        m = FakeMarket(18)
        rec = new_recorder(m)
        old = D.SYNC_LIMIT
        D.SYNC_LIMIT = 30                                           # 让快照"给满", 产生覆盖下界
        try:
            self.feed(rec, m, 5)
        finally:
            D.SYNC_LIMIT = old
        book = rec.books[SYM]
        self.assertGreater(book.lo_bid, float("-inf"))
        top_bids = sorted(book.bids, reverse=True)[:26]
        ev = {"e": "depthUpdate", "E": m.E + 100, "s": SYM, "U": m.uid + 1, "u": m.uid + 1,
              "b": [[f"{p:.2f}", "0"] for p in top_bids], "a": []}  # 撤掉最好的 26 档买单
        for p in top_bids:
            m.bids.pop(p, None)
        m.uid += 1
        rec.handle(raw(ev), time.time())
        self.assertEqual(rec.stats[SYM]["resyncs"], 1)             # 前 5 档已落到边界之外 -> 重对齐

    def test_pause_leaves_visible_break(self):
        m = FakeMarket(19)
        rec = new_recorder(m)
        self.feed(rec, m, 80)
        rec.paused = True
        self.feed(rec, m, 80)
        rec.paused = False
        self.feed(rec, m, 80)
        self.finish(rec)
        self.assertEqual(breaks(report_rows()), 1)

    def test_repair_tail_and_startup_cleanup(self):
        fresh_dirs()
        d = D.DATA_DIR / SYM / "2026-09-21"
        d.mkdir(parents=True)
        p = d / f"{SYM}_20260921_1300_p00.csv"
        p.write_text(",".join(D.header_for(5)) + "\n" + "1,2,3,4\n" + "5,6,7", encoding="ascii")
        (d / "junk.csv.xz.tmp").write_bytes(b"x")
        rec = D.Recorder()
        LIVE.append(rec)
        rec.startup_cleanup()
        rec.compressor.drain()
        self.assertFalse(p.exists())
        self.assertFalse((d / "junk.csv.xz.tmp").exists())
        with lzma.open(d / f"{SYM}_20260921_1300_p00.csv.xz", "rt") as f:
            self.assertEqual(f.read().splitlines()[-1], "1,2,3,4")   # 半截行被截掉

    def test_repair_tail_long_last_line(self):
        fresh_dirs()
        D.DATA_DIR.mkdir(parents=True)
        p = D.DATA_DIR / "x.csv"
        p.write_bytes(b"ok\n" + b"z" * 200_000)       # 半截行比 64KB 还长
        D.repair_tail(p)
        self.assertEqual(p.read_bytes(), b"ok\n")

    def test_writer_never_overwrites_existing_part(self):
        fresh_dirs()
        comp = D.Compressor()
        w = D.HourlyWriter(SYM, comp)
        w.write(["x"], E0)
        first = w.path
        w.close(compress=False)
        w2 = D.HourlyWriter(SYM, comp)
        w2.write(["y"], E0)
        self.assertNotEqual(first, w2.path)
        self.assertTrue(w2.path.name.endswith("_p01.csv"))
        w2.close(compress=False)


class TestReport(unittest.TestCase):
    def setUp(self):
        fresh_dirs()

    def write_day(self, day, start_ms, n, first_u, step_ms=1000, jump_at=None, jump_ms=0):
        d = D.DATA_DIR / SYM / day
        d.mkdir(parents=True, exist_ok=True)
        t = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        p = d / f"{SYM}_{t.strftime('%Y%m%d_%H%M')}_p00.csv"
        u, ms = first_u, start_ms
        with open(p, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(D.header_for(5))
            for i in range(n):
                if i == jump_at:
                    ms += jump_ms
                w.writerow([ms, ms, u, u] + ["100.0", "1"] * 5 + ["100.01", "1"] * 5)
                u += 1
                ms += step_ms
        return u

    def test_outage_across_midnight_does_not_distort_next_day(self):
        day1 = int(datetime(2026, 9, 20, 23, 59, 50, tzinfo=timezone.utc).timestamp() * 1000)
        day2 = int(datetime(2026, 9, 21, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        u = self.write_day("2026-09-20", day1, 10, 1)
        self.write_day("2026-09-21", day2, 100, u, jump_at=50, jump_ms=10_000)
        rep = {r["日期(UTC)"]: r for r in report_rows()}
        self.assertEqual(rep["2026-09-20"]["时段覆盖率"], "100.00%")
        self.assertEqual(rep["2026-09-21"]["时间缺口(>5秒)"], "1")          # 只有当天内部那次 10 秒跳空
        self.assertEqual(rep["2026-09-21"]["缺口秒数"], "11")
        self.assertEqual(rep["2026-09-21"]["断号"], "0")                    # 编号连续, 没丢数据

    def test_file_compressed_mid_report_is_still_read(self):
        day = int(datetime(2026, 9, 21, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.write_day("2026-09-21", day, 20, 1)
        csv_path = D.data_files(D.DATA_DIR / SYM)[0]
        D.compress_file(csv_path)                     # 列出来之后、读之前被压缩掉
        size, f = D._read_rows(csv_path)
        self.assertIsNotNone(f)
        with f:
            self.assertEqual(len(f.read().splitlines()), 21)

    def test_data_files_prefers_compressed_copy(self):
        day = int(datetime(2026, 9, 21, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.write_day("2026-09-21", day, 20, 1)
        p = D.data_files(D.DATA_DIR / SYM)[0]
        shutil.copy(p, p.with_name(p.name + ".xz.tmp"))
        with open(p, "rb") as src, lzma.open(p.with_name(p.name + ".xz"), "wb") as dst:
            dst.write(src.read())                     # 压缩一半: .csv 和 .csv.xz 同时在
        files = D.data_files(D.DATA_DIR / SYM)
        self.assertEqual(len(files), 1)
        self.assertTrue(files[0].name.endswith(".csv.xz"))


class TestHealth(unittest.TestCase):
    def status(self, **kw):
        s = {"pid": os.getpid(), "stopped": False, "updated_ts": time.time(), "connected": True,
             "paused": False, "pause_reason": "",
             "symbols": {"BTCUSDT": {"live": True, "event_age_s": 0.3, "last_snap_err": ""}}}
        s.update(kw)
        return s

    def test_healthy(self):
        self.assertEqual(D.health(self.status(), time.time())[0], True)

    def test_connected_but_not_synced_is_not_healthy(self):
        s = self.status(symbols={"BTCUSDT": {"live": False, "event_age_s": None,
                                             "last_snap_err": "快照获取失败, 经 直连 (代理没开?)"}})
        ok, text = D.health(s, time.time())
        self.assertFalse(ok)
        self.assertIn("未对齐", text)
        self.assertIn("代理", text)

    def test_stale_status_is_not_healthy(self):
        ok, text = D.health(self.status(updated_ts=time.time() - 300), time.time())
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main(verbosity=2)
