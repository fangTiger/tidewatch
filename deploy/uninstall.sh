#!/usr/bin/env bash
# 卸载观潮 launchd 服务
set -euo pipefail

LABEL="com.captain.tidewatch"
TARGET="$HOME/Library/LaunchAgents/$LABEL.plist"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
rm -f "$TARGET"
echo "已停止并卸载：$LABEL"
