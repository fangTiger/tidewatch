#!/usr/bin/env bash
# 安装观潮 launchd 服务：开机自启、崩溃自动重启、运行期间阻止空闲睡眠
# 用法：deploy/install.sh [--dry-run]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.captain.tidewatch"
TARGET="$HOME/Library/LaunchAgents/$LABEL.plist"
ARGS=""
if [[ "${1:-}" == "--dry-run" ]]; then
  ARGS="<string>--dry-run</string>"
fi

mkdir -p "$REPO/log" "$HOME/Library/LaunchAgents"
sed -e "s#__REPO__#$REPO#g" -e "s#__ARGS__#$ARGS#g" "$REPO/deploy/$LABEL.plist" > "$TARGET"
plutil -lint "$TARGET"

# 已安装则先卸载，等旧进程完全退出（最长约 20 秒）后再加载
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
for _ in $(seq 1 20); do
  launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || break
  sleep 1
done
launchctl bootstrap "gui/$(id -u)" "$TARGET"
echo "已安装并启动：$LABEL"
echo "查看状态：launchctl print gui/$(id -u)/$LABEL | head -20"
echo "查看日志：tail -f $REPO/log/tidewatch_\$(TZ=Asia/Shanghai date +%F).log"
