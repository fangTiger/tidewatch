"""deploy/install.sh 的测试

把脚本复制到临时目录模拟一个仓库，HOME 指向临时目录，并在 PATH 最前面放一个只记录调用的假 launchctl，
因此测试绝不会真正操作 launchd，也不会写入真实的 ~/Library/LaunchAgents。
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
LABEL = "com.captain.tidewatch"
needs_plutil = pytest.mark.skipif(shutil.which("plutil") is None, reason="需要 macOS 的 plutil")


class Sandbox:
    def __init__(self, root: Path, repo_name: str = "repo", *, fail_bootstrap: bool = False) -> None:
        self.root = root
        self.repo = root / repo_name
        self.home = root / "home"
        self.bin = root / "bin"
        self.log = root / "launchctl.log"
        (self.repo / "deploy").mkdir(parents=True)
        (self.repo / ".venv" / "bin").mkdir(parents=True)
        shutil.copy2(DEPLOY / "install.sh", self.repo / "deploy" / "install.sh")
        shutil.copy2(DEPLOY / f"{LABEL}.plist", self.repo / "deploy" / f"{LABEL}.plist")
        python = self.repo / ".venv" / "bin" / "python"
        python.write_text("#!/bin/sh\n", encoding="utf-8")
        python.chmod(0o755)
        (self.repo / ".env").write_text("TIDEWATCH_DATABASE_URL=unused\n", encoding="utf-8")
        self.home.mkdir()
        self.bin.mkdir()
        fake = self.bin / "launchctl"
        # print 子命令返回非零，让脚本认为旧服务不存在、不用等待
        bootstrap_status = 1 if fail_bootstrap else 0
        fake.write_text(
            f'#!/bin/bash\necho "$@" >> "{self.log}"\n'
            f'[ "$1" = "print" ] && exit 1\n[ "$1" = "bootstrap" ] && exit {bootstrap_status}\nexit 0\n',
            encoding="utf-8",
        )
        fake.chmod(0o755)

    @property
    def target(self) -> Path:
        return self.home / "Library" / "LaunchAgents" / f"{LABEL}.plist"

    def launchctl_calls(self) -> list[str]:
        return self.log.read_text(encoding="utf-8").splitlines() if self.log.exists() else []

    def run(self, *args: str) -> subprocess.CompletedProcess:
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "HOME": str(self.home)}
        return subprocess.run(["bash", str(self.repo / "deploy" / "install.sh"), *args], env=env,
                              capture_output=True, text=True, timeout=30, check=False)


@pytest.fixture
def sandbox(tmp_path) -> Sandbox:
    return Sandbox(tmp_path)


def test_scripts_pass_bash_syntax_check():
    for name in ("install.sh", "uninstall.sh"):
        result = subprocess.run(["bash", "-n", str(DEPLOY / name)], capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("args", [("--bogus",), ("dry-run",), ("-h",), ("--dry-run", "--dry-run"), ("",)])
def test_rejects_unknown_arguments(sandbox, args):
    result = sandbox.run(*args)
    assert result.returncode != 0
    assert "用法" in result.stderr
    assert sandbox.launchctl_calls() == [] and not sandbox.target.exists()


def test_requires_venv(sandbox):
    shutil.rmtree(sandbox.repo / ".venv")
    result = sandbox.run()
    assert result.returncode != 0 and ".venv" in result.stderr and "虚拟环境" in result.stderr
    assert sandbox.launchctl_calls() == [] and not sandbox.target.exists()


def test_requires_env_file(sandbox):
    (sandbox.repo / ".env").unlink()
    result = sandbox.run()
    assert result.returncode != 0 and ".env" in result.stderr
    assert sandbox.launchctl_calls() == [] and not sandbox.target.exists()


@pytest.mark.parametrize("char", ["&", "#", "\\", "<", ">", '"', "'"])
def test_rejects_repo_path_with_special_characters(tmp_path, char):
    box = Sandbox(tmp_path, repo_name=f"re{char}po")
    result = box.run()
    assert result.returncode != 0 and "仓库路径" in result.stderr
    assert box.launchctl_calls() == [] and not box.target.exists()


@needs_plutil
def test_installs_valid_plist_and_loads_it(sandbox):
    result = sandbox.run()
    assert result.returncode == 0, result.stderr
    content = sandbox.target.read_text(encoding="utf-8")
    assert str(sandbox.repo) in content and "__REPO__" not in content and "__ARGS__" not in content
    assert "--dry-run" not in content
    assert subprocess.run(["plutil", "-lint", str(sandbox.target)], capture_output=True, timeout=30).returncode == 0
    assert any(call.startswith("bootstrap") and str(sandbox.target) in call for call in sandbox.launchctl_calls())
    assert [p.name for p in sandbox.target.parent.iterdir()] == [f"{LABEL}.plist"]  # 没有残留的临时文件


@needs_plutil
def test_dry_run_adds_argument(sandbox):
    result = sandbox.run("--dry-run")
    assert result.returncode == 0, result.stderr
    assert "<string>--dry-run</string>" in sandbox.target.read_text(encoding="utf-8")


@needs_plutil
def test_installs_from_repo_path_with_spaces_and_quotes_displayed_paths(tmp_path):
    box = Sandbox(tmp_path, repo_name="repo with spaces")
    result = box.run()
    assert result.returncode == 0, result.stderr
    assert str(box.repo) in box.target.read_text(encoding="utf-8")
    assert f'launchctl print "gui/{os.getuid()}/{LABEL}"' in result.stdout
    assert f'tail -f "{box.repo}/log/tidewatch_$(TZ=Asia/Shanghai date +%F).log"' in result.stdout


@needs_plutil
def test_bootstrap_failure_prints_chinese_hint_and_exits_nonzero(tmp_path):
    box = Sandbox(tmp_path, fail_bootstrap=True)
    result = box.run()
    assert result.returncode != 0
    assert "加载服务失败" in result.stderr
    assert "launchctl print" in result.stderr


@needs_plutil
def test_failed_lint_keeps_existing_plist_and_cleans_up(sandbox):
    sandbox.target.parent.mkdir(parents=True)
    sandbox.target.write_text("旧的配置", encoding="utf-8")
    template = sandbox.repo / "deploy" / f"{LABEL}.plist"
    template.write_text(template.read_text(encoding="utf-8").replace("<dict>", "<dict"), encoding="utf-8")  # 破坏 XML
    result = sandbox.run()
    assert result.returncode != 0 and "plutil" in result.stderr
    assert sandbox.target.read_text(encoding="utf-8") == "旧的配置"  # 已安装的配置不受影响
    assert [p.name for p in sandbox.target.parent.iterdir()] == [f"{LABEL}.plist"]  # 临时文件已清理
    assert sandbox.launchctl_calls() == []
