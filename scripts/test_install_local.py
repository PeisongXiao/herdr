from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class InstallLocalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.checkout = self.root / "checkout"
        self.checkout.mkdir()
        self.installer = self.checkout / "install-local.sh"
        shutil.copy2(PROJECT_ROOT / "install-local.sh", self.installer)

        self.tool_dir = self.root / "fake-bin"
        self.tool_dir.mkdir()
        self.cargo_log = self.root / "cargo.log"
        self.herdr_log = self.root / "herdr.log"
        self.launch_dir = self.root / "launch"
        self.launch_dir.mkdir()
        self.home = self.root / "home"
        self.home.mkdir()
        self.cargo_home = self.root / "cargo-home"
        self.cargo_home.mkdir()
        self.cache_home = self.root / "cache"

        self.bash = shutil.which("bash")
        if self.bash is None:
            self.fail("bash is required to test install-local.sh")

        self.write_tool(
            "cargo",
            r"""
            #!/bin/sh
            set -eu
            if [ "${1:-}" = "--version" ]; then
              if [ "${FAKE_CARGO_BROKEN:-0}" = "1" ]; then
                exit 72
              fi
              printf 'cargo 1.90.0\n'
              exit 0
            fi
            printf '%s\n' "$*" >> "$FAKE_CARGO_LOG"
            printf 'zig=%s\n' "${ZIG:-}" >> "$FAKE_CARGO_LOG"
            printf 'target=%s\n' "${CARGO_TARGET_DIR:-}" >> "$FAKE_CARGO_LOG"

            profile=debug
            for arg in "$@"; do
              if [ "$arg" = "--release" ]; then
                profile=release
              fi
            done

            target_dir="${CARGO_TARGET_DIR:-target}"
            case "$target_dir" in
              /*) ;;
              *) target_dir="$PWD/$target_dir" ;;
            esac
            mkdir -p "$target_dir/$profile"
            printf '%s' "${FAKE_BINARY_CONTENT:-fake herdr binary}" > "$target_dir/$profile/herdr"
            chmod 755 "$target_dir/$profile/herdr"
            """,
        )
        self.write_tool(
            "rustc",
            r"""
            #!/bin/sh
            if [ "${1:-}" = "--version" ] && [ "${FAKE_RUSTC_BROKEN:-0}" = "1" ]; then
              exit 73
            fi
            printf 'rustc 1.90.0\n'
            exit 0
            """,
        )
        self.write_tool(
            "zig",
            r"""
            #!/bin/sh
            if [ "${1:-}" = "version" ]; then
              printf '%s\n' "${FAKE_ZIG_VERSION:-0.15.2}"
              exit 0
            fi
            exit 2
            """,
        )
        self.write_tool(
            "cc",
            r"""
            #!/bin/sh
            if [ "${FAKE_CC_BROKEN:-0}" = "1" ]; then
              exit 74
            fi
            exit 0
            """,
        )
        self.write_tool(
            "uname",
            r"""
            #!/bin/sh
            case "${1:-}" in
              -s) printf '%s\n' "${FAKE_UNAME_S:-Linux}" ;;
              -m) printf '%s\n' "${FAKE_UNAME_M:-x86_64}" ;;
              *) exit 2 ;;
            esac
            """,
        )
        self.write_tool(
            "herdr",
            r"""
            #!/bin/sh
            printf '%s\n' "$*" >> "$FAKE_HERDR_LOG"
            exit 97
            """,
        )

        self.base_env = os.environ.copy()
        for name in (
            "CARGO_BUILD_TARGET",
            "CARGO_TARGET_DIR",
            "CC",
            "PREFIX",
            "XDG_CACHE_HOME",
            "ZIG",
        ):
            self.base_env.pop(name, None)
        self.base_env.update(
            {
                "PATH": f"{self.tool_dir}{os.pathsep}{self.base_env.get('PATH', '')}",
                "HOME": str(self.home),
                "CARGO_HOME": str(self.cargo_home),
                "XDG_CACHE_HOME": str(self.cache_home),
                "FAKE_CARGO_LOG": str(self.cargo_log),
                "FAKE_HERDR_LOG": str(self.herdr_log),
                "FAKE_UNAME_S": "Linux",
                "FAKE_UNAME_M": "x86_64",
                "FAKE_ZIG_VERSION": "0.15.2",
                "ZIG": "zig",
            }
        )

    def write_tool(self, name: str, body: str) -> Path:
        path = self.tool_dir / name
        path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
        path.chmod(0o755)
        return path

    def run_installer(
        self, *args: str, extra_env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        env = self.base_env.copy()
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            [self.bash, str(self.installer), *args],
            cwd=self.launch_dir,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def assert_success(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(
            result.returncode,
            0,
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )

    def default_target_dir(self, platform: str = "linux-x86_64") -> Path:
        return Path(
            f"{self.cache_home / 'herdr' / 'install-local'}{self.checkout}/{platform}"
        )

    def test_check_mode_checks_dependencies_without_building_or_writing(self) -> None:
        prefix = self.root / "check-prefix"
        result = self.run_installer(
            "--check",
            "--prefix",
            str(prefix),
            extra_env={"CARGO_TARGET_DIR": "check-target"},
        )

        self.assert_success(result)
        self.assertIn("dependencies ok", result.stdout)
        self.assertIn(f"install destination: {prefix}/bin/herdr", result.stdout)
        self.assertFalse(prefix.exists())
        self.assertFalse((self.checkout / "check-target").exists())
        self.assertFalse(self.cargo_log.exists())

    def test_check_mode_accepts_all_supported_platforms(self) -> None:
        supported = (
            ("Linux", "x86_64", "linux-x86_64"),
            ("Linux", "aarch64", "linux-aarch64"),
            ("Darwin", "x86_64", "macOS-x86_64"),
            ("Darwin", "arm64", "macOS-aarch64"),
        )

        for os_name, arch_name, label in supported:
            with self.subTest(platform=label):
                result = self.run_installer(
                    "--check",
                    "--bin-dir",
                    str(self.root / "unused-bin"),
                    extra_env={"FAKE_UNAME_S": os_name, "FAKE_UNAME_M": arch_name},
                )
                self.assert_success(result)
                self.assertIn(f"supported platform: {label}", result.stdout)

        self.assertFalse(self.cargo_log.exists())

    def test_release_install_uses_locked_release_build(self) -> None:
        bin_dir = self.root / "release-bin"
        result = self.run_installer(
            "--bin-dir",
            str(bin_dir),
            extra_env={"FAKE_BINARY_CONTENT": "release binary"},
        )

        self.assert_success(result)
        installed = bin_dir / "herdr"
        self.assertEqual(installed.read_text(encoding="utf-8"), "release binary")
        self.assertEqual(installed.stat().st_mode & 0o777, 0o755)
        self.assertEqual(
            self.cargo_log.read_text(encoding="utf-8").splitlines()[0],
            "build --release --locked",
        )
        cargo_log = self.cargo_log.read_text(encoding="utf-8").splitlines()
        self.assertIn(f"zig={self.tool_dir / 'zig'}", cargo_log)
        self.assertIn(f"target={self.default_target_dir()}", cargo_log)
        self.assertIn("source upgrades require rerunning this script", result.stdout)
        self.assertIn("restart or hand off", result.stdout)
        self.assertIn("herdr integration install <agent>", result.stdout)
        self.assertFalse(self.herdr_log.exists())

    def test_debug_install_uses_locked_debug_build(self) -> None:
        bin_dir = self.root / "debug-bin"
        result = self.run_installer(
            "--debug",
            "--bin-dir",
            str(bin_dir),
            extra_env={"FAKE_BINARY_CONTENT": "debug binary"},
        )

        self.assert_success(result)
        self.assertEqual((bin_dir / "herdr").read_text(encoding="utf-8"), "debug binary")
        self.assertEqual(
            self.cargo_log.read_text(encoding="utf-8").splitlines()[0],
            "build --locked",
        )
        self.assertTrue((self.default_target_dir() / "debug" / "herdr").exists())

    def test_default_build_cache_ignores_checkout_target_artifacts(self) -> None:
        stale = self.checkout / "target" / "release" / "herdr"
        stale.parent.mkdir(parents=True)
        stale.write_text("stale container binary", encoding="utf-8")
        stale.chmod(0o755)
        bin_dir = self.root / "isolated-bin"

        result = self.run_installer(
            "--bin-dir",
            str(bin_dir),
            extra_env={"FAKE_BINARY_CONTENT": "native local binary"},
        )

        self.assert_success(result)
        self.assertEqual(
            (bin_dir / "herdr").read_text(encoding="utf-8"),
            "native local binary",
        )
        self.assertEqual(stale.read_text(encoding="utf-8"), "stale container binary")
        self.assertTrue((self.default_target_dir() / "release" / "herdr").exists())

    def test_relative_cargo_target_dir_is_resolved_from_checkout(self) -> None:
        bin_dir = self.root / "custom-target-bin"
        result = self.run_installer(
            "--bin-dir",
            str(bin_dir),
            extra_env={
                "CARGO_TARGET_DIR": "artifacts/cargo-target",
                "FAKE_BINARY_CONTENT": "custom target binary",
            },
        )

        self.assert_success(result)
        self.assertEqual(
            (bin_dir / "herdr").read_text(encoding="utf-8"),
            "custom target binary",
        )
        self.assertTrue(
            (self.checkout / "artifacts" / "cargo-target" / "release" / "herdr").exists()
        )
        self.assertFalse((self.launch_dir / "artifacts").exists())
        self.assertFalse((self.checkout / "target").exists())

    def test_rerun_replaces_binary_and_failed_replace_cleans_up(self) -> None:
        bin_dir = self.root / "rerun-bin"
        first = self.run_installer(
            "--bin-dir",
            str(bin_dir),
            extra_env={"FAKE_BINARY_CONTENT": "first binary"},
        )
        self.assert_success(first)

        real_mv = shutil.which("mv", path=os.environ.get("PATH"))
        if real_mv is None:
            self.fail("mv is required to test atomic replacement")
        self.write_tool(
            "mv",
            f"""
            #!/bin/sh
            if [ "${{FAKE_MV_FAIL:-0}}" = "1" ]; then
              exit 71
            fi
            exec {shlex.quote(real_mv)} "$@"
            """,
        )

        failed = self.run_installer(
            "--bin-dir",
            str(bin_dir),
            extra_env={"FAKE_BINARY_CONTENT": "second binary", "FAKE_MV_FAIL": "1"},
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("could not atomically replace", failed.stderr)
        self.assertEqual((bin_dir / "herdr").read_text(encoding="utf-8"), "first binary")
        self.assertEqual(list(bin_dir.glob(".herdr.tmp.*")), [])

        rerun = self.run_installer(
            "--bin-dir",
            str(bin_dir),
            extra_env={"FAKE_BINARY_CONTENT": "second binary"},
        )
        self.assert_success(rerun)
        self.assertEqual((bin_dir / "herdr").read_text(encoding="utf-8"), "second binary")
        self.assertEqual(list(bin_dir.glob(".herdr.tmp.*")), [])

    def test_wrong_zig_version_is_rejected(self) -> None:
        result = self.run_installer(
            "--check", extra_env={"FAKE_ZIG_VERSION": "0.15.1"}
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsupported Zig version: 0.15.1", result.stderr)
        self.assertIn("requires Zig 0.15.2", result.stderr)
        self.assertFalse(self.cargo_log.exists())

    def test_present_but_broken_cargo_is_rejected(self) -> None:
        result = self.run_installer(
            "--check", extra_env={"FAKE_CARGO_BROKEN": "1"}
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unusable dependency: cargo", result.stderr)
        self.assertIn("command failed: cargo --version", result.stderr)
        self.assertIn("Install or repair the dependencies", result.stderr)
        self.assertFalse(self.cargo_log.exists())

    def test_present_but_broken_rustc_is_rejected(self) -> None:
        result = self.run_installer(
            "--check", extra_env={"FAKE_RUSTC_BROKEN": "1"}
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unusable dependency: rustc", result.stderr)
        self.assertIn("command failed: rustc --version", result.stderr)
        self.assertIn("Install or repair the dependencies", result.stderr)

    def test_present_but_broken_selected_c_compiler_is_rejected(self) -> None:
        result = self.run_installer(
            "--check", extra_env={"CC": "cc", "FAKE_CC_BROKEN": "1"}
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unusable dependency: C compiler/linker", result.stderr)
        self.assertIn("cc failed to compile and link a test program", result.stderr)
        self.assertIn("Install or repair the dependencies", result.stderr)

    def test_check_honors_cc_when_selecting_the_c_compiler(self) -> None:
        self.write_tool("custom-cc", "#!/bin/sh\nexit 0\n")

        result = self.run_installer(
            "--check", extra_env={"CC": "custom-cc", "FAKE_CC_BROKEN": "1"}
        )

        self.assert_success(result)
        self.assertIn("dependencies ok", result.stdout)

    def test_unsupported_platform_is_rejected_before_dependency_checks(self) -> None:
        result = self.run_installer(
            "--check",
            extra_env={"FAKE_UNAME_S": "FreeBSD", "FAKE_UNAME_M": "x86_64"},
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsupported platform: FreeBSD/x86_64", result.stderr)
        self.assertIn("Linux and macOS on x86_64 or aarch64", result.stderr)
        self.assertFalse(self.cargo_log.exists())

    def test_cargo_build_target_environment_is_rejected(self) -> None:
        result = self.run_installer(
            "--check",
            extra_env={"CARGO_BUILD_TARGET": "aarch64-unknown-linux-gnu"},
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CARGO_BUILD_TARGET is set", result.stderr)
        self.assertIn("target-specific output", result.stderr)
        self.assertFalse(self.cargo_log.exists())

    def test_cargo_config_build_target_is_rejected(self) -> None:
        cargo_config = self.checkout / ".cargo" / "config.toml"
        cargo_config.parent.mkdir()
        cargo_config.write_text(
            '[build]\ntarget = "aarch64-unknown-linux-gnu"\n', encoding="utf-8"
        )

        result = self.run_installer("--check")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"Cargo build.target is configured in {cargo_config}", result.stderr)
        self.assertIn("target-specific output", result.stderr)
        self.assertFalse(self.cargo_log.exists())


if __name__ == "__main__":
    unittest.main()
