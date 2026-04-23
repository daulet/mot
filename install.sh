#!/bin/sh
set -eu

REPO="${MOT_REPO:-daulet/mot}"
BINARY_NAME="${MOT_BINARY_NAME:-mot}"
INSTALL_DIR="${INSTALL_DIR:-${BIN_DIR:-$HOME/.local/bin}}"
VERSION="${MOT_VERSION:-latest}"
METHOD="tar"

usage() {
  cat <<'EOF'
Install mot from GitHub release artifacts.

Usage:
  install.sh [--tar|--deb] [--version vX.Y.Z] [--dir PATH]

Environment:
  INSTALL_DIR  install directory for --tar mode (default: ~/.local/bin)
  MOT_VERSION  release tag to install (default: latest)
  MOT_REPO     GitHub repository (default: daulet/mot)
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --tar)
      METHOD="tar"
      ;;
    --deb)
      METHOD="deb"
      ;;
    --version)
      shift
      VERSION="${1:?missing version after --version}"
      ;;
    --dir)
      shift
      INSTALL_DIR="${1:?missing directory after --dir}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

download() {
  url="$1"
  dest="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$dest"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$dest"
  else
    echo "required command not found: curl or wget" >&2
    exit 1
  fi
}

sha256_file() {
  path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  else
    echo "required command not found: sha256sum or shasum" >&2
    exit 1
  fi
}

target_arch() {
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os:$arch" in
    Linux:x86_64|Linux:amd64)
      echo "x86_64-unknown-linux-gnu"
      ;;
    Linux:aarch64|Linux:arm64)
      echo "aarch64-unknown-linux-gnu"
      ;;
    Darwin:x86_64)
      echo "x86_64-apple-darwin"
      ;;
    Darwin:arm64|Darwin:aarch64)
      echo "aarch64-apple-darwin"
      ;;
    *)
      echo "unsupported platform: $os $arch" >&2
      exit 1
      ;;
  esac
}

deb_arch() {
  os="$(uname -s)"
  arch="$(uname -m)"
  if [ "$os" != "Linux" ]; then
    echo "--deb is only supported on Linux" >&2
    exit 1
  fi
  case "$arch" in
    x86_64|amd64)
      echo "amd64"
      ;;
    aarch64|arm64)
      echo "arm64"
      ;;
    *)
      echo "unsupported Debian architecture: $arch" >&2
      exit 1
      ;;
  esac
}

latest_tag() {
  url="https://github.com/$REPO/releases/latest"
  if command -v curl >/dev/null 2>&1; then
    final_url="$(curl -fsIL -o /dev/null -w '%{url_effective}' "$url")"
  elif command -v wget >/dev/null 2>&1; then
    final_url="$(wget -qS --spider "$url" 2>&1 | awk 'tolower($1) == "location:" { loc = $2 } END { print loc }' | tr -d '\r')"
  else
    echo "required command not found: curl or wget" >&2
    exit 1
  fi
  tag="${final_url##*/}"
  if [ -z "$tag" ] || [ "$tag" = "latest" ]; then
    echo "failed to resolve latest release tag" >&2
    exit 1
  fi
  echo "$tag"
}

if [ "$VERSION" = "latest" ]; then
  TAG="latest"
  RESOLVED_TAG="$(latest_tag)"
else
  case "$VERSION" in
    v*) TAG="$VERSION" ;;
    *) TAG="v$VERSION" ;;
  esac
  RESOLVED_TAG="$TAG"
fi

VERSION_NUMBER="${RESOLVED_TAG#v}"
if [ "$TAG" = "latest" ]; then
  BASE_URL="https://github.com/$REPO/releases/latest/download"
else
  BASE_URL="https://github.com/$REPO/releases/download/$TAG"
fi
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM

CHECKSUMS="$TMP_DIR/checksums.txt"
download "$BASE_URL/checksums.txt" "$CHECKSUMS"

case "$METHOD" in
  tar)
    need_cmd install
    need_cmd tar
    TARGET="$(target_arch)"
    ASSET="$BINARY_NAME-$TARGET.tar.gz"
    ;;
  deb)
    ARCH="$(deb_arch)"
    ASSET="${BINARY_NAME}_${VERSION_NUMBER}_${ARCH}.deb"
    ;;
  *)
    echo "unsupported install method: $METHOD" >&2
    exit 2
    ;;
esac

ARCHIVE="$TMP_DIR/$ASSET"
download "$BASE_URL/$ASSET" "$ARCHIVE"

EXPECTED="$(awk -v name="$ASSET" '$2 == name {print $1}' "$CHECKSUMS" | tail -n1)"
if [ -z "$EXPECTED" ]; then
  echo "checksum for $ASSET not found in checksums.txt" >&2
  exit 1
fi

ACTUAL="$(sha256_file "$ARCHIVE")"
if [ "$EXPECTED" != "$ACTUAL" ]; then
  echo "checksum mismatch for $ASSET" >&2
  echo "expected: $EXPECTED" >&2
  echo "actual:   $ACTUAL" >&2
  exit 1
fi

if [ "$METHOD" = "deb" ]; then
  need_cmd apt-get
  if [ "$(id -u)" -eq 0 ]; then
    apt-get install -y "$ARCHIVE"
  else
    need_cmd sudo
    sudo apt-get install -y "$ARCHIVE"
  fi
  exit 0
fi

mkdir -p "$INSTALL_DIR"
tar -xzf "$ARCHIVE" -C "$TMP_DIR" "$BINARY_NAME"
install -m 755 "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "installed $BINARY_NAME to $INSTALL_DIR"
    echo "note: $INSTALL_DIR is not in PATH"
    exit 0
    ;;
esac

"$INSTALL_DIR/$BINARY_NAME" --version
