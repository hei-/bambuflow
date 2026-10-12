#!/bin/sh
# 运行时配置：设了 BF_CONSOLE_CONFIG（整份 JSON）就覆盖镜像里的默认 config.json，换环境不用重新构建。
set -eu
if [ -n "${BF_CONSOLE_CONFIG:-}" ]; then
    printf '%s\n' "$BF_CONSOLE_CONFIG" > /usr/share/nginx/html/config.json
fi
