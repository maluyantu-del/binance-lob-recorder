#!/bin/bash
# 订单簿记录器 后台服务开关
#   bash service.sh start     开始后台常驻 (登录后自动启动, 崩溃 30 秒后自动拉起)
#   bash service.sh stop      停止, 并取消自动启动
#   bash service.sh restart   改过代码后重启
#   bash service.sh status    看状态
#   bash service.sh log       看最近日志
#   bash service.sh report    重算数据完整性报告
set -u
LABEL="com.deepml.lob-recorder"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DIR="$(cd "$(dirname "$0")" && pwd)"
PY="${LOB_PYTHON:-$(command -v python3)}"   # 可用环境变量 LOB_PYTHON 指定解释器 (须为绝对路径)
# launchd 打不开含中文的日志路径 (会报 rc=78 启动失败), 所以系统层日志放纯英文路径;
# 记录器自己的日志仍在本目录 logs/ 下。
LDIR="$HOME/Library/Logs/deepml"
DOMAIN="gui/$(id -u)"

is_loaded() { launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; }

case "${1:-status}" in
  start)
    if [ ! -x "$PY" ]; then echo "找不到 Python: $PY"; exit 1; fi
    "$PY" -c "import websockets" 2>/dev/null || { echo "缺少 websockets 库, 先运行: $PY -m pip install websockets"; exit 1; }
    mkdir -p "$LDIR" "$HOME/Library/LaunchAgents"
    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array><string>/bin/bash</string><string>-c</string><string>cd "$DIR" &amp;&amp; exec /usr/bin/caffeinate -i "$PY" -u lob_daemon.py run</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>30</integer>
  <key>ExitTimeOut</key><integer>30</integer>
  <key>StandardOutPath</key><string>$LDIR/lob-recorder.log</string>
  <key>StandardErrorPath</key><string>$LDIR/lob-recorder.log</string>
</dict></plist>
EOF
    plutil -lint "$PLIST" >/dev/null || { echo "配置文件格式错误: $PLIST"; exit 1; }
    if is_loaded; then launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null; fi
    for i in 1 2 3 4 5; do
      if launchctl bootstrap "$DOMAIN" "$PLIST" 2>/dev/null; then break; fi
      sleep 1
    done
    # 核验: 后台那份进程真的起来了, 而且就是它在写状态 (不是别的终端里跑着的另一份)
    for i in $(seq 1 20); do
      sleep 1
      LPID=$(launchctl list | awk -v l="$LABEL" '$3==l {print $1}')
      SPID=$("$PY" -c "import json;print(json.load(open('$DIR/status.json'))['pid'])" 2>/dev/null)
      if [ -n "$LPID" ] && [ "$LPID" != "-" ] && [ "$LPID" = "$SPID" ]; then
        echo "已启动后台记录 (pid $LPID)。以后开机登录会自动开始, 崩溃会自动拉起。"
        echo "看状态: bash service.sh status     停止: bash service.sh stop"
        exit 0
      fi
    done
    if [ -n "$SPID" ] && kill -0 "$SPID" 2>/dev/null && [ "$SPID" != "$LPID" ]; then
      echo "⚠️ 已经有另一个记录器 (pid $SPID, 多半是你在终端里手动跑的) 占着, 后台这份起不来。"
      echo "   先关掉那个 (在它的终端按 Ctrl+C), 后台这份 1 分钟内会自动接上。"
    else
      echo "⚠️ 20 秒内还没确认启动成功。电脑很忙时启动会慢 (实测负载高时要一分多钟),"
      echo "   过一两分钟再运行 bash service.sh status 看; 还不行就看日志: bash service.sh log"
    fi
    exit 1
    ;;
  stop)
    if is_loaded; then launchctl bootout "$DOMAIN/$LABEL" && echo "已停止。"; else echo "本来就没在后台跑。"; fi
    rm -f "$PLIST"
    echo "已取消开机自动启动。已录的数据都还在 $DIR/data"
    ;;
  restart)
    bash "$DIR/service.sh" stop >/dev/null; exec bash "$DIR/service.sh" start
    ;;
  status)
    "$PY" "$DIR/lob_daemon.py" status
    ;;
  log)
    tail -n "${2:-40}" "$DIR/logs/daemon.log" 2>/dev/null || echo "还没有日志"
    echo "---- 系统层日志 (启动失败时看这里) ----"
    tail -n 10 "$LDIR/lob-recorder.log" 2>/dev/null || echo "(空)"
    ;;
  report)
    "$PY" "$DIR/lob_daemon.py" report
    ;;
  *)
    sed -n '2,9p' "$DIR/service.sh"
    ;;
esac
