#!/usr/bin/env python3
"""
lob_daemon.py  订单簿长跑记录器 (Limit Order Book recorder, long-running)

它做什么
  连币安公开行情, 实时重建 BTCUSDT / ETHUSDT 的订单簿,
  买卖各前 10 档一有变化就记一行, 按小时存成压缩表格。
  断线自动重连, 漏包自动重新对齐, 每 10 分钟用一张全新快照独立重建一次做逐档比对。

怎么用
  python3 lob_daemon.py run       前台跑 (调试用, Ctrl+C 停)
  python3 lob_daemon.py status    看实时健康状态
  python3 lob_daemon.py report    从已落盘数据重算完整性报告 (每天每品种一行)
  后台常驻用同目录的 service.sh: start / stop / restart / status / log / report

网络: 行情推送直连 data-stream.binance.vision; 快照接口若所在网络无法直连, 经系统代理访问,
      代理没开时记录器会一直重试并在 status 里报"未对齐", 不会写任何可疑数据。
只用公开接口, 不需要任何密钥。
"""
from __future__ import annotations

import csv
import fcntl
import gzip
import heapq
import json
import logging
import logging.handlers
import lzma
import math
import os
import queue
import re
import shutil
import signal
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


# ============================ 配置 ============================
# 环境变量可以覆盖, 平时不用管; 测试时用来把数据写到临时目录、把时间调短。
def _env(name, default, cast=str):
    v = os.environ.get(name)
    return cast(v) if v not in (None, "") else default


BASE = Path(__file__).resolve().parent
SYMBOLS = [s.strip().upper() for s in _env("LOB_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
LEVELS = _env("LOB_LEVELS", 10, int)                     # 每侧记录几档
DATA_DIR = Path(_env("LOB_DATA_DIR", str(BASE / "data")))
STATE_DIR = Path(_env("LOB_STATE_DIR", str(BASE)))       # status.json / logs 放这里

WS_HOST = "wss://data-stream.binance.vision"             # 实测直连可用; stream.binance.com 在部分网络不通
REST_HOSTS = ["https://api.binance.com", "https://api-gcp.binance.com", "https://api1.binance.com"]
SYNC_LIMIT = 1000            # 重新对齐用小快照 (约 64KB), 下得快, 断档短
VERIFY_LIMIT = 5000          # 比对用大快照 (约 320KB), 顺便把覆盖范围扩到买卖各约 800 美元
REST_MIN_GAP = 2.0           # 两次快照请求至少隔 2 秒, 防止触发交易所限频
REST_TIMEOUT = 8.0           # 单次请求超时
REST_DEADLINE = 20.0         # 一次取快照 (轮换几个地址) 最多花这么久

STALL_SEC = _env("LOB_STALL_SEC", 30.0, float)           # 整条连接 30 秒没消息 = 僵尸连接, 重连
SYMBOL_STALL_SEC = _env("LOB_SYMBOL_STALL_SEC", 120.0, float)  # 单个品种 2 分钟没消息 = 订阅丢了, 重连
VERIFY_SEC = _env("LOB_VERIFY_SEC", 600.0, float)        # 每 10 分钟独立重建比对一次
VERIFY_AFTER_SYNC = 60.0     # 每次对齐后 60 秒先比对一次, 顺便换成大快照
VERIFY_DEPTH = 20            # 比对前 20 档
VERIFY_TIMEOUT = 90.0
ROTATE_SEC = _env("LOB_ROTATE_SEC", 3600, int)           # 每小时换一个文件
RING_MAX = 3000              # 每个品种保留最近几分钟已应用的事件, 给比对器回放
PENDING_MAX = 5000           # 等快照期间最多缓冲多少条

MIN_FREE_GB = _env("LOB_MIN_FREE_GB", 10.0, float)       # 磁盘剩余低于它就暂停写盘, 保护本机其他程序
DATA_BUDGET_GB = _env("LOB_BUDGET_GB", 5.0, float)       # 本记录器数据总量上限
COMPRESS = _env("LOB_COMPRESS", "xz")                    # 关闭的小时文件怎么压: xz (小三成) 或 gz

FLUSH_SEC, STATUS_SEC, DISK_SEC, SIZE_WALK_SEC = 2.0, 15.0, 60.0, 300.0
TIME_GAP_MS = 5000           # 报告里, 同一天相邻两行隔超过 5 秒算一次时间缺口

# ---- 仅供测试的故障注入开关, 平时全是 0 ----
TEST_DROP_AFTER = _env("LOB_TEST_DROP_AFTER", 0, int)            # 第 N 条事件故意丢掉一次 -> 应触发断号重对齐
TEST_DISCONNECT_SEC = _env("LOB_TEST_DISCONNECT_SEC", 0.0, float)  # 连上 N 秒后主动断线一次 -> 应自动重连
TEST_FREEZE_SEC = _env("LOB_TEST_FREEZE_SEC", 0.0, float)        # 连上 N 秒后假装收不到消息 -> 应判僵尸并重连
TEST_CORRUPT_AFTER = _env("LOB_TEST_CORRUPT_AFTER", 0, int)      # 第 N 条事件后往簿里塞一档假挂单 -> 应被抓到
TEST_WS_PROXY_FIRST = _env("LOB_TEST_WS_PROXY_FIRST", 0, int)    # 行情推送先走代理 (测试备用线路)

LABEL = "com.deepml.lob-recorder"
LOG_DIR = STATE_DIR / "logs"
STATUS_FILE = STATE_DIR / "status.json"
EVENTS_FILE = LOG_DIR / "events.tsv"
REPORT_FILE = STATE_DIR / "integrity_report.tsv"
LOCK_FILE = STATE_DIR / ".lock"

log = logging.getLogger("lob")


def setup_logging(foreground: bool):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.handlers.RotatingFileHandler(LOG_DIR / "daemon.log", maxBytes=5_000_000,
                                              backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    if foreground:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        log.addHandler(sh)


def utc_str(ts: float | None = None) -> str:
    ts = time.time() if ts is None else ts
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def record_event(symbol: str, kind: str, detail: str = ""):
    """往 logs/events.tsv 追加一行。report 会按天汇总这些事件。"""
    detail = re.sub(r"[\t\r\n]+", " ", str(detail))
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    new = not EVENTS_FILE.exists()
    with open(EVENTS_FILE, "a", encoding="utf-8") as f:
        if new:
            f.write("utc_time\tsymbol\tkind\tdetail\n")
        f.write(f"{utc_str()}\t{symbol}\t{kind}\t{detail}\n")
    log.info("[%s] %s %s", symbol, kind, detail)


def current_proxy() -> str | None:
    """每次现读系统代理 (代理开关、换端口都能立刻生效)。"""
    p = urllib.request.getproxies()
    return p.get("https") or p.get("http")


# ============================ 订单簿与对齐状态机 ============================
def apply_levels(side: dict, levels):
    """把一批 [价格, 数量] 贴到一侧簿上。数量是该价位的最新总量, 不是增减量; 0 表示撤光。"""
    for p, q in levels:
        p, q = float(p), float(q)
        if q == 0.0:
            side.pop(p, None)          # 删一个本地没有的价位是正常的, 不报错
        else:
            side[p] = q


class BookSync:
    """一个品种的本地订单簿, 严格按币安官方的"本地订单簿维护"规则对齐。

      1. 先订阅增量流并缓冲, 再取快照, 顺序不能反
      2. 快照编号记为 L, 丢掉 u <= L 的旧增量
      3. 第一条要应用的增量必须满足 U <= L+1 <= u, 否则说明快照太旧, 要重取
      4. 之后每条都必须 U == 上一条的 u + 1, 否则就是断号, 从第 1 步重来
    另外记下快照的覆盖边界: 快照只给了最好的 N 档, 边界以外、快照之后一直没变过的挂单本地是不知道的。
    这里只管判断和贴数, 出了问题怎么办由外面的 Recorder 决定。
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.gen = 0                      # 每重置一次加 1, 用来认出"过期"的后台快照结果
        self.reset()

    def reset(self):
        self.gen += 1
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_u: int | None = None
        self.has_snapshot = False
        self.live = False                 # 已经应用过第一条合规增量
        self.pending: list[dict] = []     # 还没快照时缓冲的增量
        self.lo_bid = -math.inf           # 买侧可信下界: 低于它的价位本地可能缺
        self.hi_ask = math.inf            # 卖侧可信上界

    def load_snapshot(self, snap: dict, limit: int):
        self.bids = {float(p): float(q) for p, q in snap["bids"] if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in snap["asks"] if float(q) > 0}
        self.last_u = int(snap["lastUpdateId"])
        self.has_snapshot = True
        self.live = False
        # 快照给满了 limit 档, 说明后面可能还有; 没给满说明这一侧全在里面了, 不设边界
        self.lo_bid = min(self.bids) if len(snap["bids"]) >= limit and self.bids else -math.inf
        self.hi_ask = max(self.asks) if len(snap["asks"]) >= limit and self.asks else math.inf

    def feed(self, ev: dict) -> str:
        """喂一条增量。返回 applied / buffered / stale / gap 四种结果之一。"""
        U, u = ev["U"], ev["u"]
        if not self.has_snapshot:
            self.pending.append(ev)
            return "buffered"
        if not self.live:
            if u <= self.last_u:
                return "stale"
            if not (U <= self.last_u + 1 <= u):
                return "gap"
            self.live = True
        elif U != self.last_u + 1:
            return "gap"
        apply_levels(self.bids, ev["b"])
        apply_levels(self.asks, ev["a"])
        self.last_u = u
        return "applied"

    def top(self, n: int):
        return heapq.nlargest(n, self.bids.items()), heapq.nsmallest(n, self.asks.items())

    def trustworthy(self, bids, asks, n: int) -> str:
        """前 n 档是否都落在快照覆盖范围内、且没有买卖交叉。返回空串表示可信, 否则返回原因。"""
        if bids and asks and bids[0][0] >= asks[0][0]:
            return f"买一 {bids[0][0]} 不低于卖一 {asks[0][0]} (真实订单簿不可能这样)"
        if self.lo_bid > -math.inf and (len(bids) < n or bids[-1][0] < self.lo_bid):
            return f"买侧前 {n} 档跌出快照覆盖下界 {self.lo_bid}"
        if self.hi_ask < math.inf and (len(asks) < n or asks[-1][0] > self.hi_ask):
            return f"卖侧前 {n} 档涨出快照覆盖上界 {self.hi_ask}"
        return ""


# ============================ 快照接口 (后台线程) ============================
class RestClient:
    """取快照。自带限频保护: 两次请求至少隔 REST_MIN_GAP 秒, 被限频就按交易所要求的时间停。
    每次请求都新建连接器, 现读系统代理。只在后台线程里用。"""

    def __init__(self):
        self.last_call = 0.0
        self.blocked_until = 0.0
        self.host_idx = 0
        # 证书只加载一次反复用: 每次新建要约 300 毫秒纯计算且一直占着 Python 全局锁,
        # 会饿死负责回心跳的线程, 导致交易所判定心跳超时断线 (实测踩过)
        self.ssl_ctx = ssl.create_default_context()

    def depth(self, symbol: str, limit: int) -> dict:
        now = time.time()
        if now < self.blocked_until:
            raise RuntimeError(f"交易所限频中, {self.blocked_until - now:.0f} 秒后再试")
        wait = self.last_call + REST_MIN_GAP - now
        if wait > 0:
            time.sleep(wait)
        via = current_proxy() or "直连"
        # 代理每次现读 (代理开关、换端口立刻生效), 证书上下文复用
        opener = urllib.request.build_opener(urllib.request.ProxyHandler(urllib.request.getproxies()),
                                             urllib.request.HTTPSHandler(context=self.ssl_ctx))
        start = time.time()
        errors = []
        for i in range(len(REST_HOSTS)):
            if time.time() - start > REST_DEADLINE:
                break
            idx = (self.host_idx + i) % len(REST_HOSTS)
            url = f"{REST_HOSTS[idx]}/api/v3/depth?symbol={symbol}&limit={limit}"
            self.last_call = time.time()
            try:
                with opener.open(url, timeout=REST_TIMEOUT) as r:
                    data = json.load(r)
                if "lastUpdateId" not in data:
                    raise RuntimeError(f"快照格式不对: {str(data)[:120]}")
                self.host_idx = idx
                return data
            except urllib.error.HTTPError as e:
                if e.code in (418, 429):
                    ra = int(e.headers.get("Retry-After") or 120)
                    self.blocked_until = time.time() + ra
                    raise RuntimeError(f"HTTP {e.code} 被限频, 暂停 {ra} 秒") from e
                errors.append(f"HTTP {e.code}")
            except Exception as e:  # 网络错误就换下一个地址
                errors.append(f"{type(e).__name__}: {e}")
        hint = " (代理没开? 本机快照接口必须经代理)" if via == "直连" or any("refused" in x for x in errors) else ""
        raise RuntimeError(f"快照获取失败, 经 {via}{hint}: {errors[-1] if errors else '超时'}")


class SnapshotWorker:
    """后台线程取快照, 主循环只管交请求、收结果, 永远不用等网络。"""

    def __init__(self, rest: RestClient):
        self.rest = rest
        self.q_in: queue.Queue = queue.Queue()
        self.q_out: queue.Queue = queue.Queue()
        self._busy: set = set()           # 只在主线程里读写
        threading.Thread(target=self._loop, daemon=True, name="snapshot").start()

    def busy(self, sym: str, purpose: str) -> bool:
        return (sym, purpose) in self._busy

    def request(self, sym: str, purpose: str, gen: int, limit: int) -> bool:
        if (sym, purpose) in self._busy:
            return False
        self._busy.add((sym, purpose))
        self.q_in.put((sym, purpose, gen, limit))
        return True

    def _loop(self):
        while True:
            sym, purpose, gen, limit = self.q_in.get()
            t0 = time.time()
            try:
                snap, err = self.rest.depth(sym, limit), None
            except Exception as e:
                snap, err = None, e
            self.q_out.put((sym, purpose, gen, limit, snap, err, time.time() - t0))

    def results(self) -> list:
        out = []
        while True:
            try:
                item = self.q_out.get_nowait()
            except queue.Empty:
                return out
            self._busy.discard((item[0], item[1]))
            out.append(item)


# ============================ 落盘 ============================
def header_for(levels: int) -> list[str]:
    h = ["exch_ms", "local_ms", "U", "u"]
    for side in ("bid", "ask"):
        for i in range(1, levels + 1):
            h += [f"{side}{i}_p", f"{side}{i}_q"]
    return h


def repair_tail(path: Path):
    """进程被硬杀时最后一行可能只写了一半, 把它截掉 (往回找最近的换行, 找不到就只留空)。"""
    size = path.stat().st_size
    if size == 0:
        return
    with open(path, "rb+") as f:
        f.seek(size - 1)
        if f.read(1) == b"\n":
            return
        pos = size
        while pos > 0:
            back = min(pos, 1 << 16)
            f.seek(pos - back)
            chunk = f.read(back)
            cut = chunk.rfind(b"\n")
            if cut >= 0:
                f.truncate(pos - back + cut + 1)
                return
            pos -= back
        f.truncate(0)


def _fsync_dir(d: Path):
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def compress_file(path: Path):
    """把关掉的 .csv 压成 .csv.xz。先写临时文件、落盘、改名, 最后才删原文件;
    任何一步被打断, 下次启动都能从原文件重来, 不会丢数据。"""
    try:
        if not path.exists():
            return
        repair_tail(path)
        ext = ".csv.xz" if COMPRESS == "xz" else ".csv.gz"
        final = path.with_name(path.name[:-4] + ext)
        tmp = final.with_name(final.name + ".tmp")
        with open(path, "rb") as src, open(tmp, "wb") as raw:
            dst = lzma.open(raw, "wb", preset=6) if COMPRESS == "xz" else gzip.open(raw, "wb", compresslevel=6)
            with dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(tmp, final)
        _fsync_dir(final.parent)
        path.unlink()
    except Exception:
        log.exception("压缩失败 %s", path)


class Compressor:
    """后台线程压缩。用守护线程: 进程退出时不等它, 没压完的下次启动再压。"""

    def __init__(self):
        self.q: queue.Queue = queue.Queue()
        threading.Thread(target=self._loop, daemon=True, name="compress").start()

    def submit(self, path: Path):
        self.q.put(path)

    def _loop(self):
        while True:
            p = self.q.get()
            compress_file(p)
            self.q.task_done()

    def drain(self):
        """等队列里的都压完 (只在测试里用)。"""
        self.q.join()


PART_RE = re.compile(r"_p(\d+)\.csv(\.xz|\.gz)?(\.tmp)?$")


class HourlyWriter:
    """一个品种一个。按交易所时间每 ROTATE_SEC 秒换一个文件, 关掉的文件交给后台线程压缩。
    文件名: data/BTCUSDT/2026-09-21/BTCUSDT_20260921_1300_p00.csv(.xz)
    同一时段如果因为重启出现多个文件, 用 p00 p01 ... 区分, 按名字排序就是时间顺序。"""

    def __init__(self, symbol: str, compressor: Compressor):
        self.symbol = symbol
        self.compressor = compressor
        self.f = None
        self.w = None
        self.bucket = None
        self.path: Path | None = None

    def _open(self, bucket: int):
        start = datetime.fromtimestamp(bucket * ROTATE_SEC, tz=timezone.utc)
        day_dir = DATA_DIR / self.symbol / start.strftime("%Y-%m-%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        stem = f"{self.symbol}_{start.strftime('%Y%m%d_%H%M')}"
        used = {int(m.group(1)) for p in day_dir.glob(stem + "_p*")
                if (m := PART_RE.search(p.name))}
        part = max(used) + 1 if used else 0
        self.path = day_dir / f"{stem}_p{part:02d}.csv"
        self.f = open(self.path, "w", newline="", encoding="ascii")
        self.w = csv.writer(self.f)
        self.w.writerow(header_for(LEVELS))
        self.bucket = bucket

    def write(self, row: list, exch_ms: int):
        bucket = exch_ms // (ROTATE_SEC * 1000)
        if bucket != self.bucket:
            self.close(compress=True)
            self._open(bucket)
        self.w.writerow(row)

    def flush(self):
        if self.f:
            self.f.flush()

    def close(self, compress: bool):
        if self.f:
            self.f.close()
            p = self.path
            self.f = self.w = self.bucket = None
            if compress:
                self.compressor.submit(p)


def fmt_num(x: float) -> str:
    return repr(x)


# ============================ 比对器 ============================
class Verifier:
    """拿一张全新快照 + 回放最近事件, 独立建一本簿, 在同一个更新编号上和长跑簿逐档比较。"""

    def __init__(self, symbol: str, snap: dict, limit: int):
        self.book = BookSync(symbol)
        self.book.load_snapshot(snap, limit)
        self.snap_id = self.book.last_u
        self.started = time.time()


def describe_diff(a, b, name):
    for i in range(max(len(a), len(b))):
        x = a[i] if i < len(a) else None
        y = b[i] if i < len(b) else None
        if x != y:
            return f"{name}{i + 1}: 长跑簿={x} 新建簿={y}"
    return ""


# ============================ 主循环 ============================
class Recorder:
    def __init__(self):
        self.books = {s: BookSync(s) for s in SYMBOLS}
        self.ring = {s: deque(maxlen=RING_MAX) for s in SYMBOLS}
        self.compressor = Compressor()
        self.writers = {s: HourlyWriter(s, self.compressor) for s in SYMBOLS}
        self.worker = SnapshotWorker(RestClient())
        self.verifiers: dict[str, Verifier] = {}
        now = time.time()
        self.next_verify = {s: now + VERIFY_SEC for s in SYMBOLS}
        self.next_sync_at = {s: 0.0 for s in SYMBOLS}
        self.sync_fails = {s: 0 for s in SYMBOLS}
        self.last_top = {s: None for s in SYMBOLS}
        self.carry_U = {s: None for s in SYMBOLS}
        self.stats = {s: dict(applied=0, rows=0, stale=0, resyncs=0, verify_ok=0, verify_mismatch=0,
                              verify_skip=0, last_local=None, last_msg=None, last_snap_s=None,
                              last_snap_err="") for s in SYMBOLS}
        self.reconnects = 0
        self.stalls = 0
        self.connect_fails = 0
        self.ws_via_proxy = bool(TEST_WS_PROXY_FIRST)
        self.ws_route = ""
        self.stop = False
        self.paused = False
        self.pause_reason = ""
        self.connected = False
        self.conn_since = None
        self.started = now
        self.free_gb = None
        self.data_gb = 0.0
        self.next_flush = self.next_status = self.next_disk = 0.0
        self.next_size_walk = 0.0
        # 测试开关的一次性标记
        self._t_drop_count = 0
        self._t_drop_done = False
        self._t_disc_done = False
        self._t_freeze_done = False
        self._t_freeze_until = 0.0
        self._t_corrupt_done = False

    # ---------- 生命周期 ----------
    def _on_signal(self, signum, _frame):
        log.info("收到信号 %s, 准备退出", signum)
        self.stop = True

    def startup_cleanup(self):
        """清理上次异常退出留下的半截文件: 删临时文件, 把没压缩的 .csv 修好尾巴再压缩。"""
        if not DATA_DIR.exists():
            return
        for p in DATA_DIR.rglob("*.tmp"):
            p.unlink(missing_ok=True)
        leftovers = sorted(DATA_DIR.rglob("*.csv"))
        for p in leftovers:
            self.compressor.submit(p)
        if leftovers:
            log.info("启动清理: %d 个未压缩文件已排队压缩", len(leftovers))

    def run(self):
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._on_signal)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.startup_cleanup()
        record_event("*", "start", f"pid={os.getpid()} 品种={','.join(SYMBOLS)} 档数={LEVELS} "
                                   f"快照代理={current_proxy() or '无'}")
        self.check_disk(time.time(), force_walk=True)
        self.write_status()
        attempt = 0
        while not self.stop:
            t0 = time.time()
            try:
                self.session()
            except Exception as e:
                if self.connected:
                    record_event("*", "disconnect", f"{type(e).__name__}: {e}")
                else:
                    self.connect_fails += 1
                    record_event("*", "connect_fail", f"{self.ws_route}: {type(e).__name__}: {e}")
                    # 连续连不上就在直连和代理之间切换线路
                    if attempt >= 1 and current_proxy():
                        self.ws_via_proxy = not self.ws_via_proxy
            finally:
                self.connected = False
                self.on_disconnect()
            if self.stop:
                break
            if time.time() - t0 > 60:
                attempt = 0
            delay = min(60, 2 ** min(attempt, 6))
            attempt += 1
            self.reconnects += 1
            log.info("%d 秒后重连 (第 %d 次)", delay, attempt)
            self.write_status()
            end = time.time() + delay
            while time.time() < end and not self.stop:
                time.sleep(0.5)
        self.shutdown()

    def shutdown(self):
        # 不等压缩线程 (大文件压缩要十几秒, 会被系统强杀); 没压完的下次启动时补压
        for w in self.writers.values():
            w.close(compress=False)
        self.write_status(stopped=True)
        record_event("*", "stop", "正常退出")

    # ---------- 一次连接 ----------
    def session(self):
        from websockets.sync.client import connect
        streams = "/".join(f"{s.lower()}@depth@100ms" for s in SYMBOLS)
        url = f"{WS_HOST}/stream?streams={streams}"
        proxy = current_proxy() if self.ws_via_proxy else None
        self.ws_route = f"经代理 {proxy}" if proxy else "直连"
        with connect(url, open_timeout=15, close_timeout=5, ping_interval=20, ping_timeout=20,
                     max_queue=4096, max_size=1 << 22, proxy=proxy) as ws:
            self.connected = True
            self.conn_since = time.time()
            last_msg = time.time()
            for s in SYMBOLS:
                self.stats[s]["last_msg"] = time.time()
            record_event("*", "connect", f"{WS_HOST} ({self.ws_route})")
            while not self.stop:
                try:
                    raw = ws.recv(timeout=1.0)
                except TimeoutError:
                    raw = None
                now = time.time()
                if raw is not None and now < self._t_freeze_until:
                    raw = None                      # 测试: 假装没收到
                if raw is not None:
                    last_msg = now
                    self.handle(raw, now)
                elif now - last_msg > STALL_SEC:
                    self.stalls += 1
                    record_event("*", "stall", f"{STALL_SEC:.0f} 秒没有任何消息, 判定僵尸连接, 重连")
                    return
                if self.symbol_stalled(now):
                    return
                self.test_hooks(now, ws)
                self.periodic(now)

    def symbol_stalled(self, now: float) -> bool:
        """整条连接活着但某个品种一直没消息 = 订阅丢了 (重连不重订), 也要重连。"""
        if now - self.conn_since < SYMBOL_STALL_SEC:
            return False
        for s in SYMBOLS:
            lm = self.stats[s]["last_msg"]
            if lm is not None and now - lm > SYMBOL_STALL_SEC:
                self.stalls += 1
                record_event(s, "stall", f"{SYMBOL_STALL_SEC:.0f} 秒没有该品种消息, 重连")
                return True
        return False

    def test_hooks(self, now: float, ws):
        if TEST_DISCONNECT_SEC and not self._t_disc_done and now - self.conn_since > TEST_DISCONNECT_SEC:
            self._t_disc_done = True
            record_event("*", "test_disconnect", "测试: 主动断线")
            ws.close()
        if TEST_FREEZE_SEC and not self._t_freeze_done and now - self.conn_since > TEST_FREEZE_SEC:
            self._t_freeze_done = True
            self._t_freeze_until = now + STALL_SEC + 10
            record_event("*", "test_freeze", "测试: 假装收不到消息")

    def on_disconnect(self):
        for s in SYMBOLS:
            self._reset_symbol(s, why="断线")
            self.next_sync_at[s] = 0.0
            self.writers[s].flush()
        self._t_freeze_until = 0.0

    # ---------- 断档统一处理 ----------
    def _reset_symbol(self, sym: str, why: str):
        """所有"数据接不上了"的情况都走这里: 清簿、清缓存、清比对、清行累计。
        清掉 carry_U 保证文件里下一行的编号一定接不上上一行, 断点在报告里看得见。"""
        v = self.verifiers.pop(sym, None)
        if v is not None:
            self.stats[sym]["verify_skip"] += 1
            record_event(sym, "verify_skip", f"比对进行中遇到{why}, 作废")
        self.books[sym].reset()
        self.ring[sym].clear()
        self.carry_U[sym] = None
        self.last_top[sym] = None

    def resync(self, sym: str, detail: str, buffer_ev: dict | None = None):
        self.stats[sym]["resyncs"] += 1
        record_event(sym, "resync", detail)
        self._reset_symbol(sym, why="重对齐")
        if buffer_ev is not None:
            self.books[sym].pending = [buffer_ev]
        self.maybe_sync(sym, time.time())

    # ---------- 处理一条消息 ----------
    def handle(self, raw, now: float):
        msg = json.loads(raw)
        ev = msg.get("data", msg)
        if ev.get("e") != "depthUpdate":
            return
        sym = ev.get("s")
        if sym not in self.books:
            return
        ev["_local"] = int(now * 1000)
        self.stats[sym]["last_msg"] = now
        if TEST_DROP_AFTER and not self._t_drop_done and sym == SYMBOLS[0]:
            self._t_drop_count += 1
            if self._t_drop_count == TEST_DROP_AFTER:
                self._t_drop_done = True
                record_event(sym, "test_drop", f"测试: 故意丢掉 U={ev['U']} u={ev['u']}")
                return
        book = self.books[sym]
        r = book.feed(ev)
        if r == "applied":
            self.on_applied(sym, ev)
        elif r == "buffered":
            if len(book.pending) > PENDING_MAX:
                del book.pending[:len(book.pending) - PENDING_MAX]
            self.maybe_sync(sym, now)
        elif r == "stale":
            self.stats[sym]["stale"] += 1
        else:
            if book.live:
                detail = f"断号: 期望 U={book.last_u + 1}, 实到 U={ev['U']}"
            else:
                detail = f"快照过旧: 快照编号 {book.last_u}, 流上第一条 U={ev['U']}"
            self.resync(sym, detail, buffer_ev=ev)

    def maybe_sync(self, sym: str, now: float):
        """没快照 + 已缓冲至少一条 + 到了允许重试的时间 -> 交给后台线程取快照。"""
        book = self.books[sym]
        if book.has_snapshot or not book.pending or now < self.next_sync_at[sym] \
                or self.worker.busy(sym, "sync") or self.stop:
            return
        self.worker.request(sym, "sync", book.gen, SYNC_LIMIT)

    def poll_results(self):
        """收后台线程取回的快照。对齐状态在请求之后变过 (代号 gen 不同) 的结果直接作废。"""
        for sym, purpose, gen, limit, snap, err, dt in self.worker.results():
            st = self.stats[sym]
            book = self.books[sym]
            if purpose == "sync":
                if gen != book.gen or book.has_snapshot:
                    continue
                if err is not None:
                    self.sync_fails[sym] += 1
                    backoff = min(60.0, 5.0 * 2 ** min(self.sync_fails[sym] - 1, 4))
                    self.next_sync_at[sym] = time.time() + backoff
                    st["last_snap_err"] = str(err)
                    record_event(sym, "snapshot_error", f"{err}; {backoff:.0f} 秒后重试")
                    continue
                self.sync_fails[sym] = 0
                st["last_snap_s"] = round(dt, 2)
                st["last_snap_err"] = ""
                self.apply_sync_snapshot(sym, snap, limit, dt)
            else:
                if gen != book.gen or not book.live:
                    st["verify_skip"] += 1
                    record_event(sym, "verify_skip", "取快照期间对齐状态变了, 作废")
                    continue
                if err is not None:
                    st["verify_skip"] += 1
                    st["last_snap_err"] = str(err)
                    record_event(sym, "verify_skip", f"取快照失败: {err}")
                    continue
                st["last_snap_s"] = round(dt, 2)
                self.begin_verify(sym, snap, limit)

    def apply_sync_snapshot(self, sym: str, snap: dict, limit: int, dt: float):
        book = self.books[sym]
        book.load_snapshot(snap, limit)
        gen = book.gen
        pend, book.pending = book.pending, []
        for i, ev in enumerate(pend):
            r = book.feed(ev)
            if r == "applied":
                self.on_applied(sym, ev)
                if book.gen != gen:                  # on_applied 里触发了重对齐
                    book.pending = pend[i + 1:] + book.pending
                    return
            elif r == "stale":
                self.stats[sym]["stale"] += 1
            elif r == "gap":
                was_live = book.live
                detail = (f"断号(缓冲内): 期望 U={book.last_u + 1}, 实到 U={ev['U']}" if was_live
                          else f"快照过旧: 快照编号 {snap['lastUpdateId']}, 缓冲里第一条 U={ev['U']}")
                self.stats[sym]["resyncs"] += 1
                record_event(sym, "resync", detail)
                self._reset_symbol(sym, why="重对齐")
                book.pending = pend[i:]
                self.next_sync_at[sym] = time.time() + REST_MIN_GAP
                return
        record_event(sym, "synced", f"快照编号 {snap['lastUpdateId']} ({limit} 档, 耗时 {dt:.1f} 秒), "
                                    f"回放缓冲 {len(pend)} 条")
        idx = SYMBOLS.index(sym)
        self.next_verify[sym] = min(self.next_verify[sym], time.time() + VERIFY_AFTER_SYNC + 15 * idx)

    def on_applied(self, sym: str, ev: dict):
        st = self.stats[sym]
        st["applied"] += 1
        st["last_local"] = ev["_local"]
        self.ring[sym].append(ev)
        book = self.books[sym]
        if TEST_CORRUPT_AFTER and not self._t_corrupt_done and sym == SYMBOLS[0] \
                and st["applied"] >= TEST_CORRUPT_AFTER:
            self._t_corrupt_done = True
            b = sorted(book.bids, reverse=True)
            if len(b) > 4:
                fake = round((b[2] + b[3]) / 2, 6)
                if fake not in book.bids:
                    book.bids[fake] = 123.456
                    record_event(sym, "test_corrupt", f"测试: 在 {fake} 塞入一档假买单 123.456")
        if sym in self.verifiers:
            self.step_verifier(sym, ev)
        bids, asks = book.top(LEVELS)
        why = book.trustworthy(bids, asks, LEVELS)
        if why:
            self.resync(sym, why)                   # 簿已不可信: 不写这一行, 重新对齐
            return
        if self.carry_U[sym] is None:
            self.carry_U[sym] = ev["U"]
        if self.paused:
            # 暂停期间不写盘; 清掉累计, 恢复后第一行会露出断号, 报告里看得见
            self.carry_U[sym] = None
            self.last_top[sym] = None
            return
        top = (bids, asks)
        if top == self.last_top[sym]:
            return                                  # 前 10 档没变, 不写, 编号区间累计到下一行
        row = [ev["E"], ev["_local"], self.carry_U[sym], ev["u"]]
        for side in (bids, asks):
            for p, q in side:
                row += [fmt_num(p), fmt_num(q)]
            row += ["", ""] * (LEVELS - len(side))
        self.writers[sym].write(row, ev["E"])
        st["rows"] += 1
        self.carry_U[sym] = None
        self.last_top[sym] = top

    # ---------- 比对 ----------
    def request_verify(self, sym: str):
        book = self.books[sym]
        self.next_verify[sym] = time.time() + VERIFY_SEC
        if not book.live or sym in self.verifiers:
            return
        self.worker.request(sym, "verify", book.gen, VERIFY_LIMIT)

    def begin_verify(self, sym: str, snap: dict, limit: int):
        main = self.books[sym]
        v = Verifier(sym, snap, limit)
        for ev in list(self.ring[sym]):
            r = v.book.feed(ev)
            if r == "gap":
                self.stats[sym]["verify_skip"] += 1
                record_event(sym, "verify_skip", f"快照编号 {v.snap_id} 早于缓存的最早事件, 无法回放")
                return
        if v.book.live and v.book.last_u == main.last_u:
            self.finish_verify(sym, v)
        elif v.book.live:
            self.stats[sym]["verify_skip"] += 1
            record_event(sym, "verify_skip", f"编号错位 新建={v.book.last_u} 长跑={main.last_u}")
        else:
            self.verifiers[sym] = v                 # 快照比当前还新, 等后面的事件跟上

    def step_verifier(self, sym: str, ev: dict):
        v = self.verifiers[sym]
        r = v.book.feed(ev)
        if r == "gap":
            self.verifiers.pop(sym, None)
            self.stats[sym]["verify_skip"] += 1
            record_event(sym, "verify_skip", f"比对簿断号 (快照编号 {v.snap_id})")
        elif v.book.live and v.book.last_u == self.books[sym].last_u:
            self.verifiers.pop(sym, None)
            self.finish_verify(sym, v)

    def finish_verify(self, sym: str, v: Verifier):
        main = self.books[sym]
        mb, ma = main.top(VERIFY_DEPTH)
        vb, va = v.book.top(VERIFY_DEPTH)
        if mb == vb and ma == va:
            self.stats[sym]["verify_ok"] += 1
            record_event(sym, "verify_ok", f"编号 {main.last_u} 前 {VERIFY_DEPTH} 档逐档一致")
        else:
            self.stats[sym]["verify_mismatch"] += 1
            d = describe_diff(mb, vb, "bid") or describe_diff(ma, va, "ask")
            record_event(sym, "verify_mismatch", f"编号 {main.last_u} 不一致, 已用新建簿替换. {d}")
        # 无论一致与否都换成新建的簿: 它的覆盖范围以当前价为中心, 防止长跑后覆盖范围跑偏
        main.bids, main.asks = v.book.bids, v.book.asks
        main.lo_bid, main.hi_ask = v.book.lo_bid, v.book.hi_ask
        self.next_verify[sym] = time.time() + VERIFY_SEC

    # ---------- 定时杂事 ----------
    def periodic(self, now: float):
        self.poll_results()
        if self.stop:
            return
        if now >= self.next_flush:
            for w in self.writers.values():
                w.flush()
            self.next_flush = now + FLUSH_SEC
        if now >= self.next_disk:
            self.check_disk(now)
            self.next_disk = now + DISK_SEC
        for s in SYMBOLS:
            book = self.books[s]
            if not book.has_snapshot and book.pending:
                self.maybe_sync(s, now)
            elif book.live and s not in self.verifiers and now >= self.next_verify[s] \
                    and not self.worker.busy(s, "verify"):
                self.request_verify(s)
            v = self.verifiers.get(s)
            if v and now - v.started > VERIFY_TIMEOUT:
                self.verifiers.pop(s, None)
                self.stats[s]["verify_skip"] += 1
                record_event(s, "verify_skip", "比对超时")
        if now >= self.next_status:
            self.write_status()
            self.next_status = now + STATUS_SEC

    def check_disk(self, now: float, force_walk: bool = False):
        self.free_gb = shutil.disk_usage(DATA_DIR).free / 2 ** 30
        if force_walk or now >= self.next_size_walk:
            total = 0
            for root, _, files in os.walk(DATA_DIR):
                for fn in files:
                    try:
                        total += os.path.getsize(os.path.join(root, fn))
                    except OSError:
                        pass
            self.data_gb = total / 2 ** 30
            self.next_size_walk = now + SIZE_WALK_SEC
        reasons = []
        if self.free_gb < MIN_FREE_GB:
            reasons.append(f"磁盘只剩 {self.free_gb:.1f}GB, 低于保护线 {MIN_FREE_GB:.0f}GB")
        if self.data_gb >= DATA_BUDGET_GB:
            reasons.append(f"数据已有 {self.data_gb:.2f}GB, 达到上限 {DATA_BUDGET_GB:.0f}GB")
        if reasons and not self.paused:
            self.paused = True
            self.pause_reason = "; ".join(reasons)
            record_event("*", "pause", self.pause_reason + " -> 暂停写盘, 订单簿照常维护")
            for s in SYMBOLS:
                self.writers[s].close(compress=True)
                self.carry_U[s] = None
                self.last_top[s] = None
        elif self.paused and not reasons and self.free_gb >= MIN_FREE_GB + 1 \
                and self.data_gb < DATA_BUDGET_GB * 0.9:
            # 恢复要留余量, 否则压缩一下体积变小又马上恢复、再暂停, 来回抖
            self.paused = False
            record_event("*", "resume", f"磁盘 {self.free_gb:.1f}GB, 数据 {self.data_gb:.2f}GB, 恢复写盘")
            self.pause_reason = ""

    def write_status(self, stopped: bool = False):
        now = time.time()
        syms = {}
        for s in SYMBOLS:
            b, st, w = self.books[s], self.stats[s], self.writers[s]
            bids, asks = b.top(1) if b.live else ([], [])
            syms[s] = {
                "live": b.live, "last_u": b.last_u,
                "event_age_s": round(now - st["last_local"] / 1000, 1) if st["last_local"] else None,
                "applied": st["applied"], "rows": st["rows"], "stale": st["stale"], "resyncs": st["resyncs"],
                "verify_ok": st["verify_ok"], "verify_mismatch": st["verify_mismatch"],
                "verify_skip": st["verify_skip"], "last_snap_s": st["last_snap_s"],
                "last_snap_err": st["last_snap_err"],
                "bid1": bids[0][0] if bids else None, "ask1": asks[0][0] if asks else None,
                "file": w.path.name if (w.path and w.f) else None,
            }
        status = {
            "pid": os.getpid(), "stopped": stopped,
            "updated_ts": now, "updated_utc": utc_str(now), "started_utc": utc_str(self.started),
            "uptime_h": round((now - self.started) / 3600, 2),
            "connected": self.connected, "ws_route": self.ws_route, "rest_proxy": current_proxy(),
            "reconnects": self.reconnects, "stalls": self.stalls, "connect_fails": self.connect_fails,
            "paused": self.paused, "pause_reason": self.pause_reason,
            "disk_free_gb": round(self.free_gb, 2) if self.free_gb is not None else None,
            "data_gb": round(self.data_gb, 3),
            "limits": {"min_free_gb": MIN_FREE_GB, "budget_gb": DATA_BUDGET_GB},
            "data_dir": str(DATA_DIR), "symbols": syms,
        }
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATUS_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(status, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, STATUS_FILE)


# ============================ status 命令 ============================
def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def launchd_state() -> str:
    try:
        out = subprocess.run(["launchctl", "list"], capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return "查不到"
    for line in out.splitlines():
        if line.endswith(LABEL):
            pid = line.split()[0]
            return f"已挂后台 (pid {pid})" if pid != "-" else "已挂后台但当前没在运行"
    return "没有挂后台"


def health(s: dict, now: float) -> tuple[bool, str]:
    """一句话判断现在到底有没有在记数据。返回 (是否正常记录, 说明)。"""
    age = now - s["updated_ts"]
    if s.get("stopped"):
        return False, "已正常停止"
    if age > 60 or not pid_alive(s["pid"]):
        return False, f"可能已停止 (状态 {age:.0f} 秒没更新)"
    if not s["connected"]:
        return False, "运行中, 但行情推送断开, 正在重连"
    if s["paused"]:
        return False, f"运行中, 但已暂停写盘: {s['pause_reason']}"
    bad = []
    for sym, d in s["symbols"].items():
        if not d["live"]:
            hint = f" ({d['last_snap_err'][:60]})" if d.get("last_snap_err") else ""
            bad.append(f"{sym} 未对齐{hint}")
        elif d["event_age_s"] is None or d["event_age_s"] + age > 30:
            bad.append(f"{sym} 超过 30 秒没有新数据")
    if bad:
        return False, "运行中, 但没在正常记录: " + "; ".join(bad)
    return True, "运行中, 正在正常记录"


def cmd_status() -> int:
    print(f"后台服务: {launchd_state()}")
    if not STATUS_FILE.exists():
        print("还没有状态文件: 记录器从没跑过。")
        return 1
    s = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    ok, text = health(s, time.time())
    print(f"记录器:   {'✅' if ok else '⚠️'} {text}")
    print(f"          已运行 {s['uptime_h']} 小时   断线重连 {s['reconnects']} 次   僵尸检测触发 {s['stalls']} 次"
          f"   行情线路 {s.get('ws_route') or '-'}   快照代理 {s.get('rest_proxy') or '无 (本机快照必须经代理)'}")
    print(f"磁盘:     剩余 {s['disk_free_gb']}GB (保护线 {s['limits']['min_free_gb']:.0f}GB)   "
          f"本记录器数据 {s['data_gb']}GB (上限 {s['limits']['budget_gb']:.0f}GB)")
    print()
    print("品种\t对齐\t最近数据\t本次写入行数\t重对齐\t比对通过\t比对不一致\t比对跳过\t买一\t卖一\t当前文件")
    for sym, d in s["symbols"].items():
        age_s = "-" if d["event_age_s"] is None else f"{d['event_age_s']}秒前"
        print(f"{sym}\t{'是' if d['live'] else '否'}\t{age_s}\t{d['rows']}\t{d['resyncs']}\t"
              f"{d['verify_ok']}\t{d['verify_mismatch']}\t{d['verify_skip']}\t{d['bid1']}\t{d['ask1']}\t{d['file']}")
    return 0 if ok else 2


# ============================ report 命令 ============================
FILE_RE = re.compile(r"_(\d{8})_(\d{4})_p(\d+)\.csv(\.xz|\.gz)?$")


def open_text(path: Path):
    n = path.name
    if n.endswith(".xz"):
        return lzma.open(path, "rt", encoding="ascii", newline="")
    if n.endswith(".gz"):
        return gzip.open(path, "rt", encoding="ascii", newline="")
    return open(path, "r", encoding="ascii", newline="")


def data_files(sym_dir: Path):
    """按时间顺序列出一个品种的数据文件。同一段同时有 .csv 和压缩版时 (压缩刚好进行到一半), 只取压缩版。"""
    best: dict = {}
    for p in sym_dir.rglob("*"):
        m = FILE_RE.search(p.name)
        if not (m and p.is_file()):
            continue
        key = (m.group(1), m.group(2), int(m.group(3)))
        compressed = m.group(4) is not None
        if key not in best or (compressed and not best[key][0]):
            best[key] = (compressed, p)
    return [best[k][1] for k in sorted(best)]


def load_events():
    counts = {}
    if not EVENTS_FILE.exists():
        return counts
    with open(EVENTS_FILE, encoding="utf-8") as f:
        next(f, None)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            day, sym, kind = parts[0][:10], parts[1], parts[2]
            counts.setdefault((day, sym), {}).setdefault(kind, 0)
            counts[(day, sym)][kind] += 1
    return counts


def _read_rows(path: Path):
    """读一个数据文件; 如果读之前它刚被压缩掉了, 改读压缩版。"""
    candidates = [path]
    if path.name.endswith(".csv"):
        candidates += [path.with_name(path.name + ".xz"), path.with_name(path.name + ".gz")]
    for p in candidates:
        try:
            size = p.stat().st_size
            f = open_text(p)
        except FileNotFoundError:
            continue
        return size, f
    return 0, None


def cmd_report() -> int:
    if not DATA_DIR.exists():
        print("还没有数据目录。")
        return 1
    ev = load_events()
    cols = ["日期(UTC)", "品种", "行数", "首条", "末条", "录制小时", "时段覆盖率", "断号", "时间缺口(>5秒)",
            "缺口秒数", "买卖交叉", "重对齐", "断线或僵尸", "比对通过", "比对不一致", "比对跳过", "数据MB"]
    lines = ["\t".join(cols)]
    for sym_dir in sorted(p for p in DATA_DIR.iterdir() if p.is_dir()):
        sym = sym_dir.name
        days: dict[str, dict] = {}
        prev_u = prev_ms = prev_day = None
        for path in data_files(sym_dir):
            day = path.parent.name
            d = days.setdefault(day, dict(rows=0, first=None, last=None, breaks=0, gaps=0, gap_ms=0,
                                          crossed=0, bytes=0))
            size, f = _read_rows(path)
            if f is None:
                continue
            d["bytes"] += size
            try:
                with f:
                    r = csv.reader(f)
                    hdr = next(r, None)
                    if not hdr:
                        continue
                    iE, iU, iu = hdr.index("exch_ms"), hdr.index("U"), hdr.index("u")
                    ib, ia = hdr.index("bid1_p"), hdr.index("ask1_p")
                    for row in r:
                        if len(row) != len(hdr):
                            continue
                        try:
                            ms, U, u = int(row[iE]), int(row[iU]), int(row[iu])
                        except ValueError:
                            continue
                        d["rows"] += 1
                        d["first"] = ms if d["first"] is None else d["first"]
                        d["last"] = ms
                        if prev_u is not None and U != prev_u + 1:
                            d["breaks"] += 1
                        # 时间缺口只算同一天内部的; 跨午夜的停机看两天的"末条/首条"就知道
                        if prev_day == day and ms - prev_ms > TIME_GAP_MS:
                            d["gaps"] += 1
                            d["gap_ms"] += ms - prev_ms
                        if row[ib] and row[ia] and float(row[ib]) >= float(row[ia]):
                            d["crossed"] += 1
                        prev_u, prev_ms, prev_day = u, ms, day
            except (EOFError, OSError, lzma.LZMAError) as e:
                print(f"读文件出错 {path.name}: {e}", file=sys.stderr)
        for day, d in sorted(days.items()):
            if not d["rows"]:
                continue
            span = max(1, d["last"] - d["first"])
            cov = 1 - min(d["gap_ms"], span) / span
            e1 = ev.get((day, sym), {})
            e2 = ev.get((day, "*"), {})
            lines.append("\t".join(str(x) for x in [
                day, sym, d["rows"],
                datetime.fromtimestamp(d["first"] / 1000, tz=timezone.utc).strftime("%H:%M:%S"),
                datetime.fromtimestamp(d["last"] / 1000, tz=timezone.utc).strftime("%H:%M:%S"),
                round(span / 3.6e6, 2), f"{cov:.2%}", d["breaks"], d["gaps"], round(d["gap_ms"] / 1000),
                d["crossed"], e1.get("resync", 0),
                e2.get("disconnect", 0) + e2.get("stall", 0) + e1.get("stall", 0),
                e1.get("verify_ok", 0), e1.get("verify_mismatch", 0), e1.get("verify_skip", 0),
                round(d["bytes"] / 2 ** 20, 1)]))
    text = "\n".join(lines) + "\n"
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(text, encoding="utf-8")
    print(text, end="")
    print(f"\n(已存 {REPORT_FILE})")
    return 0


# ============================ 入口 ============================
def cmd_run() -> int:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("已经有一个记录器在跑了 (同一个数据目录只允许一个), 这次不启动。60 秒后退出。", flush=True)
        time.sleep(60)          # 放慢后台服务的反复拉起
        return 0
    setup_logging(foreground=sys.stdout.isatty())
    try:
        Recorder().run()
    except Exception:
        log.exception("记录器异常退出, 后台服务会自动拉起")
        return 1
    return 0


def main() -> int:
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "run":
        return cmd_run()
    if cmd == "status":
        return cmd_status()
    if cmd == "report":
        return cmd_report()
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
