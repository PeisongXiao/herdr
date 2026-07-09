#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: ./install-local.sh [--prefix DIR] [--bin-dir DIR] [--debug] [--check]

Build Herdr from this checkout and install the binary locally.

Options:
  --prefix DIR   install under DIR (default: $HOME/.local)
  --bin-dir DIR  install the herdr binary directly into DIR
  --debug        build target/debug/herdr instead of target/release/herdr
  --check        only check dependencies and print the install destination
USAGE
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

have() {
  command -v "$1" >/dev/null 2>&1
}

trim_whitespace() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

cargo_config_sets_build_target() {
  local file="$1"
  local line
  local section=""

  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%%#*}"
    line="$(trim_whitespace "$line")"
    [ -n "$line" ] || continue

    if [[ "$line" == \[*\] ]]; then
      section="${line#\[}"
      section="${section%\]}"
      section="$(trim_whitespace "$section")"
      continue
    fi

    if [[ "$line" =~ ^build[[:space:]]*\.[[:space:]]*target[[:space:]]*= ]]; then
      return 0
    fi
    if [ "$section" = "build" ] && [[ "$line" =~ ^target[[:space:]]*= ]]; then
      return 0
    fi
  done < "$file"

  return 1
}

reject_configured_build_target() {
  local file="$1"
  [ -f "$file" ] || return 0
  if cargo_config_sets_build_target "$file"; then
    die "Cargo build.target is configured in $file; cross-target layouts are unsupported because the installer cannot safely locate target-specific output; remove that setting and rerun"
  fi
}

prefix="${PREFIX:-$HOME/.local}"
bin_dir=""
profile="release"
check_only=0
zig_cmd="${ZIG:-zig}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix)
      [ "$#" -ge 2 ] || die "missing value for --prefix"
      prefix="$2"
      shift 2
      ;;
    --prefix=*)
      prefix="${1#--prefix=}"
      shift
      ;;
    --bin-dir)
      [ "$#" -ge 2 ] || die "missing value for --bin-dir"
      bin_dir="$2"
      shift 2
      ;;
    --bin-dir=*)
      bin_dir="${1#--bin-dir=}"
      shift
      ;;
    --debug)
      profile="debug"
      shift
      ;;
    --check)
      check_only=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

[ -n "$prefix" ] || die "--prefix must not be empty"
if [ -z "$bin_dir" ]; then
  bin_dir="$prefix/bin"
fi
[ -n "$bin_dir" ] || die "--bin-dir must not be empty"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

os_name="$(uname -s 2>/dev/null)" || die "could not determine the operating system with uname -s"
arch_name="$(uname -m 2>/dev/null)" || die "could not determine the CPU architecture with uname -m"

case "$os_name" in
  Linux) platform="linux" ;;
  Darwin) platform="macOS" ;;
  *)
    die "unsupported platform: $os_name/$arch_name; supported platforms are Linux and macOS on x86_64 or aarch64"
    ;;
esac

case "$arch_name" in
  x86_64) architecture="x86_64" ;;
  aarch64|arm64) architecture="aarch64" ;;
  *)
    die "unsupported platform: $os_name/$arch_name; supported platforms are Linux and macOS on x86_64 or aarch64"
    ;;
esac

if [ -n "${CARGO_BUILD_TARGET:-}" ]; then
  die "CARGO_BUILD_TARGET is set to '$CARGO_BUILD_TARGET'; cross-target layouts are unsupported because the installer cannot safely locate target-specific output; unset it and rerun"
fi

config_dir="$script_dir"
while :; do
  reject_configured_build_target "$config_dir/.cargo/config.toml"
  reject_configured_build_target "$config_dir/.cargo/config"
  [ "$config_dir" = "/" ] && break
  config_dir="${config_dir%/*}"
  [ -n "$config_dir" ] || config_dir="/"
done

cargo_home="${CARGO_HOME:-${HOME:-}/.cargo}"
if [ -n "$cargo_home" ]; then
  case "$cargo_home" in
    /*) ;;
    *) cargo_home="$script_dir/$cargo_home" ;;
  esac
  reject_configured_build_target "$cargo_home/config.toml"
  reject_configured_build_target "$cargo_home/config"
fi

if [ -n "${CARGO_TARGET_DIR:-}" ]; then
  cargo_target_dir_setting="$CARGO_TARGET_DIR"
else
  install_cache_root="${XDG_CACHE_HOME:-$HOME/.cache}"
  case "$install_cache_root" in
    /*) ;;
    *) install_cache_root="$script_dir/$install_cache_root" ;;
  esac
  cargo_target_dir_setting="$install_cache_root/herdr/install-local$script_dir/$platform-$architecture"
fi
case "$cargo_target_dir_setting" in
  /*) cargo_target_dir="$cargo_target_dir_setting" ;;
  *) cargo_target_dir="$script_dir/$cargo_target_dir_setting" ;;
esac

missing=0
for tool in cargo rustc; do
  if ! have "$tool"; then
    printf 'missing dependency: %s\n' "$tool" >&2
    missing=1
  elif ! "$tool" --version >/dev/null 2>&1; then
    printf 'unusable dependency: %s (command failed: %s --version)\n' "$tool" "$tool" >&2
    missing=1
  fi
done

zig_path="$(command -v "$zig_cmd" 2>/dev/null || true)"
if [ -z "$zig_path" ]; then
  printf 'missing dependency: zig 0.15.2 (%s)\n' "$zig_cmd" >&2
  missing=1
else
  case "$zig_path" in
    /*) ;;
    *)
      zig_path="$(cd -- "$(dirname -- "$zig_path")" && pwd -P)/$(basename -- "$zig_path")"
      ;;
  esac
  if ! zig_version="$("$zig_path" version 2>/dev/null)"; then
    printf 'unusable dependency: zig 0.15.2 (command failed: %s version)\n' "$zig_path" >&2
    missing=1
  elif [ "$zig_version" != "0.15.2" ]; then
    printf 'unsupported Zig version: %s\n' "${zig_version:-unknown}" >&2
    printf 'Herdr currently requires Zig 0.15.2 for the vendored terminal engine.\n' >&2
    missing=1
  fi
fi

c_compiler="${CC:-}"
if [ -z "$c_compiler" ]; then
  for candidate in cc clang gcc; do
    if have "$candidate"; then
      c_compiler="$candidate"
      break
    fi
  done
fi

if [ -z "$c_compiler" ]; then
  printf 'missing dependency: C compiler/linker (cc, clang, or gcc)\n' >&2
  missing=1
elif ! have "$c_compiler"; then
  printf 'missing dependency: C compiler/linker (%s selected by CC)\n' "$c_compiler" >&2
  missing=1
elif ! printf 'int main(void) { return 0; }\n' | "$c_compiler" -x c -o /dev/null - >/dev/null 2>&1; then
  printf 'unusable dependency: C compiler/linker (%s failed to compile and link a test program)\n' "$c_compiler" >&2
  missing=1
fi

if [ "$missing" -ne 0 ]; then
  cat >&2 <<'HELP'

Install or repair the dependencies reported above, then rerun this script. This
script does not run a package manager for you. Required tools:
  cargo, rustc, zig 0.15.2, and a C compiler/linker (cc, clang, or gcc)
HELP
  exit 1
fi

if [ "$check_only" -eq 1 ]; then
  printf 'dependencies ok\n'
  printf 'supported platform: %s-%s\n' "$platform" "$architecture"
  printf 'zig executable: %s\n' "$zig_path"
  printf 'build cache: %s\n' "$cargo_target_dir"
  printf 'install destination: %s/herdr\n' "$bin_dir"
  exit 0
fi

if [ "$profile" = "release" ]; then
  CC="$c_compiler" CARGO_TARGET_DIR="$cargo_target_dir_setting" ZIG="$zig_path" cargo build --release --locked
else
  CC="$c_compiler" CARGO_TARGET_DIR="$cargo_target_dir_setting" ZIG="$zig_path" cargo build --locked
fi
built="$cargo_target_dir/$profile/herdr"

[ -x "$built" ] || die "build did not produce $built"
mkdir -p "$bin_dir"
[ ! -d "$bin_dir/herdr" ] || die "install destination is a directory: $bin_dir/herdr"

tmp=""
cleanup_tmp() {
  if [ -n "$tmp" ] && [ -e "$tmp" ]; then
    rm -f "$tmp"
  fi
}
trap cleanup_tmp EXIT
trap 'exit 1' HUP INT TERM

if ! tmp="$(mktemp "$bin_dir/.herdr.tmp.XXXXXX")"; then
  die "could not create a temporary install file in $bin_dir"
fi
cp "$built" "$tmp"
chmod 755 "$tmp"
if ! mv -f "$tmp" "$bin_dir/herdr"; then
  die "could not atomically replace $bin_dir/herdr"
fi
tmp=""
trap - EXIT HUP INT TERM

printf 'installed Herdr to %s/herdr\n' "$bin_dir"
case ":$PATH:" in
  *":$bin_dir:"*) ;;
  *) printf 'note: %s is not on PATH; add it before running herdr from anywhere.\n' "$bin_dir" ;;
esac
cat <<'NOTE'
note: source upgrades require rerunning this script; restart or hand off any
already-running Herdr server to use the newly installed binary.
note: agent integrations remain separate; install one explicitly with:
  herdr integration install <agent>
NOTE
