"""
订单簿重建最小可运行版 (Limit Order Book reconstruction, minimal).
数据源: Binance 公开增量深度流 diff depth stream, 无需 API key.
做的事: 快照 + 增量 -> 本地维护完整买卖档 -> 序列号校验 -> 落 CSV.
"""
import socket, ssl, base64, os, json, time, struct, urllib.request, bisect, csv, sys

WS_HOST = "data-stream.binance.vision"   # 实测可用; stream.binance.com 在本机被墙/超时
REST     = "https://api.binance.com/api/v3/depth?symbol={sym}&limit=1000"

def ws_connect(host, path):
    s = socket.create_connection((host, 443), timeout=10)
    s = ssl.create_default_context().wrap_socket(s, server_hostname=host)
    key = base64.b64encode(os.urandom(16)).decode()
    s.send(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
           f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
           f"Sec-WebSocket-Version: 13\r\n\r\n".encode())
    hdr = b""
    while b"\r\n\r\n" not in hdr:
        hdr += s.recv(4096)
    assert b"101" in hdr.split(b"\r\n")[0], hdr[:200]
    return s, hdr.split(b"\r\n\r\n", 1)[1]

def ws_frames(sock, prebuf=b""):
    """极简 WebSocket 解帧: 只处理服务端文本帧(不掩码)与 ping."""
    buf = bytearray(prebuf)
    while True:
        while len(buf) < 2:
            d = sock.recv(65536)
            if not d: return
            buf += d
        b0, b1 = buf[0], buf[1]
        opcode = b0 & 0x0F
        ln = b1 & 0x7F
        off = 2
        if ln == 126:
            while len(buf) < 4: buf += sock.recv(65536)
            ln = struct.unpack(">H", buf[2:4])[0]; off = 4
        elif ln == 127:
            while len(buf) < 10: buf += sock.recv(65536)
            ln = struct.unpack(">Q", buf[2:10])[0]; off = 10
        while len(buf) < off + ln:
            d = sock.recv(65536)
            if not d: return
            buf += d
        payload = bytes(buf[off:off+ln]); del buf[:off+ln]
        if opcode == 0x9:                      # ping -> pong
            mask = os.urandom(4)
            masked = bytes(p ^ mask[i % 4] for i, p in enumerate(payload))
            sock.send(bytes([0x8A, 0x80 | len(payload)]) + mask + masked)
        elif opcode == 0x1:
            yield payload

class Book:
    """价格 -> 数量。数量为 0 表示该档撤销。用 dict + 排序取前 N 档。"""
    def __init__(self): self.bids, self.asks = {}, {}
    def apply(self, side, levels):
        for p, q in levels:
            p, q = float(p), float(q)
            if q == 0.0: side.pop(p, None)
            else: side[p] = q
    def top(self, n):
        b = sorted(self.bids.items(), key=lambda x: -x[0])[:n]
        a = sorted(self.asks.items(), key=lambda x:  x[0])[:n]
        return b, a

def run(symbol="btcusdt", seconds=60, levels=5, out="lob.csv"):
    sock, pre = ws_connect(WS_HOST, f"/ws/{symbol}@depth@100ms")
    frames = ws_frames(sock, pre)
    buffered, snap = [], None
    t0 = time.time()

    # 1) 先缓冲增量, 再取快照 —— 顺序不能反, 否则会漏掉快照之后、订阅之前的更新
    for raw in frames:
        buffered.append(json.loads(raw))
        if len(buffered) >= 5: break
    with urllib.request.urlopen(REST.format(sym=symbol.upper()), timeout=15) as r:
        snap = json.load(r)
    last_id = snap["lastUpdateId"]

    book = Book(); book.apply(book.bids, snap["bids"]); book.apply(book.asks, snap["asks"])

    # 2) 丢掉快照之前的旧增量; 第一条有效包必须满足 U <= lastUpdateId+1 <= u
    stats = dict(applied=0, dropped_stale=0, gaps=0, rows=0, first_ok=False)
    def apply_ev(e):
        nonlocal last_id
        if e["u"] <= last_id:
            stats["dropped_stale"] += 1; return False
        if not stats["first_ok"]:
            if not (e["U"] <= last_id + 1 <= e["u"]):
                stats["dropped_stale"] += 1; return False
            stats["first_ok"] = True
        elif e["U"] != last_id + 1:
            stats["gaps"] += 1                      # 断号 -> 真实系统要重取快照
        book.apply(book.bids, e["b"]); book.apply(book.asks, e["a"])
        last_id = e["u"]; stats["applied"] += 1; return True

    f = open(out, "w", newline=""); w = csv.writer(f)
    w.writerow(["ts_ms","lastUpdateId"] + sum([[f"bid{i}_p",f"bid{i}_q"] for i in range(1,levels+1)],[])
                                        + sum([[f"ask{i}_p",f"ask{i}_q"] for i in range(1,levels+1)],[]))
    def write_row(e):
        b, a = book.top(levels)
        if len(b) < levels or len(a) < levels: return
        row = [e["E"], last_id]
        for p, q in b: row += [p, q]
        for p, q in a: row += [p, q]
        w.writerow(row); stats["rows"] += 1

    for e in buffered:
        if apply_ev(e): write_row(e)
    for raw in frames:
        e = json.loads(raw)
        if apply_ev(e): write_row(e)
        if time.time() - t0 > seconds: break
    f.close(); sock.close()

    b, a = book.top(levels)
    spread = a[0][0] - b[0][0]
    print(f"symbol={symbol.upper()} 采集 {seconds}s")
    print(f"  应用增量 {stats['applied']}  丢弃过期 {stats['dropped_stale']}  断号 {stats['gaps']}  写出行数 {stats['rows']}")
    print(f"  最终簿: 买档 {len(book.bids)}  卖档 {len(book.asks)}")
    print(f"  盘口: bid1={b[0][0]:.2f}x{b[0][1]:.4f}  ask1={a[0][0]:.2f}x{a[0][1]:.4f}  价差={spread:.2f}")
    assert spread > 0, "买一必须低于卖一, 否则重建逻辑有错"
    return stats

if __name__ == "__main__":
    run(seconds=int(sys.argv[1]) if len(sys.argv) > 1 else 60,
        out=sys.argv[2] if len(sys.argv) > 2 else "lob.csv")
