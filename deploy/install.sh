#!/usr/bin/env bash
# 安装观潮 launchd 服务：开机自启、崩溃自动重启、运行期间阻止空闲睡眠
# 用法：deploy/install.sh [--dry-run]
set -euo pipefail

usage() {
  echo "用法：$0 [--dry-run]" >&2
  echo "  （无参数）  正式安装并启动，推送到飞书" >&2
  echo "  --dry-run   演练模式安装：不发送飞书，只写日志" >&2
}

# 只接受「无参数」或单个 --dry-run
ARGS=""
case "$#" in
  0) ;;
  1)
    if [[ "$1" == "--dry-run" ]]; then
      ARGS="<string>--dry-run</string>"
    else
      echo "错误：不支持的参数：$1" >&2
      usage
      exit 2
    fi
    ;;
  *)
    echo "错误：参数过多" >&2
    usage
    exit 2
    ;;
esac

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.captain.tidewatch"
TEMPLATE="$REPO/deploy/$LABEL.plist"
TARGET_DIR="$HOME/Library/LaunchAgents"
TARGET="$TARGET_DIR/$LABEL.plist"

# 仓库路径会被写进 sed 替换文本与 plist（XML），含这些字符会破坏替换或 XML 结构
for bad in '&' '#' '\' '<' '>' '"' "'" $'\n'; do
  if [[ "$REPO" == *"$bad"* ]]; then
    echo "错误：仓库路径含有不支持的字符（& # \\ < > \" ' 或换行），请把仓库移到不含这些字符的路径后重试：" >&2
    echo "  $REPO" >&2
    exit 1
  fi
done

# 预检：缺少虚拟环境或配置时，服务会反复启动失败
if [[ ! -f "$TEMPLATE" ]]; then
  echo "错误：找不到 plist 模板：$TEMPLATE" >&2
  exit 1
fi
if [[ ! -x "$REPO/.venv/bin/python" ]]; then
  echo "错误：找不到虚拟环境：$REPO/.venv/bin/python" >&2
  echo "请先创建虚拟环境并安装依赖：python3 -m venv .venv && .venv/bin/pip install -e ." >&2
  exit 1
fi
if [[ ! -f "$REPO/.env" ]]; then
  echo "错误：找不到密钥配置：$REPO/.env" >&2
  echo "请先参考 README 创建 .env（数据库连接串等），再安装服务。" >&2
  exit 1
fi

mkdir -p "$REPO/log" "$TARGET_DIR"

# 先生成到同目录的临时文件，plutil 校验通过后再原子替换，避免半成品覆盖已安装的配置
TMP=""
cleanup() {
  if [[ -n "$TMP" ]]; then
    rm -f "$TMP"
  fi
}
trap cleanup EXIT
trap 'exit 1' INT TERM
TMP="$(mktemp "$TARGET_DIR/$LABEL.XXXXXX")"
sed -e "s#__REPO__#$REPO#g" -e "s#__ARGS__#$ARGS#g" "$TEMPLATE" > "$TMP"
if ! plutil -lint "$TMP" >/dev/null; then
  echo "错误：生成的 plist 未通过 plutil 校验，未做任何修改" >&2
  exit 1
fi
chmod 644 "$TMP"
mv -f "$TMP" "$TARGET"
TMP=""
echo "已生成：$TARGET"

# 已安装则先卸载，等旧进程完全退出（最长约 20 秒）后再加载
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
for _ in $(seq 1 20); do
  launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || break
  sleep 1
done
if ! launchctl bootstrap "gui/$(id -u)" "$TARGET"; then
  echo "加载服务失败，可用 launchctl print \"gui/$(id -u)/$LABEL\" 查看原因" >&2
  exit 1
fi
echo "已安装并启动：$LABEL"
echo "查看状态：launchctl print \"gui/$(id -u)/$LABEL\" | head -20"
echo "查看日志：tail -f \"$REPO/log/tidewatch_\$(TZ=Asia/Shanghai date +%F).log\""
