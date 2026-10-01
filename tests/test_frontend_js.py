"""用纯 Node DOM 桩验证前端 SSE 更新，不依赖浏览器或第三方包。"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
NODE_TESTS = sorted((Path(__file__).parent / "js").glob("*.test.js"))


@pytest.mark.parametrize("script", NODE_TESTS, ids=lambda path: path.name)
def test_frontend_node_script(script: Path) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("未安装 Node.js")
    result = subprocess.run(
        [node, str(script)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
