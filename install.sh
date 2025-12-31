#!/bin/bash
# Knowhere installer script
# Usage: curl -fsSL https://raw.githubusercontent.com/saivarunk/knowhere/main/install.sh | bash

set -e

REPO="saivarunk/knowhere"
BINARY_NAME="knowhere"
INSTALL_DIR="${HOME}/.local/bin"

# Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}" in
    Linux*)     OS_TYPE="linux";;
    Darwin*)    OS_TYPE="darwin";;
    *)          echo "Unsupported OS: ${OS}"; exit 1;;
esac

case "${ARCH}" in
    x86_64)     ARCH_TYPE="x86_64";;
    amd64)      ARCH_TYPE="x86_64";;
    arm64)      ARCH_TYPE="aarch64";;
    aarch64)    ARCH_TYPE="aarch64";;
    *)          echo "Unsupported architecture: ${ARCH}"; exit 1;;
esac

TARGET="${ARCH_TYPE}-unknown-${OS_TYPE}"
if [ "${OS_TYPE}" = "darwin" ]; then
    TARGET="${ARCH_TYPE}-apple-darwin"
fi

echo "Detected: ${OS_TYPE} ${ARCH_TYPE}"
echo "Target: ${TARGET}"

# Get latest release
echo "Fetching latest release..."
LATEST_RELEASE=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "${LATEST_RELEASE}" ]; then
    echo "Error: Could not fetch latest release"
    exit 1
fi

echo "Latest release: ${LATEST_RELEASE}"

# Download URL
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${LATEST_RELEASE}/${BINARY_NAME}-${LATEST_RELEASE}-${TARGET}.tar.gz"

echo "Downloading from: ${DOWNLOAD_URL}"

# Create temp directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf ${TEMP_DIR}" EXIT

# Download and extract
curl -fsSL "${DOWNLOAD_URL}" -o "${TEMP_DIR}/knowhere.tar.gz"
tar -xzf "${TEMP_DIR}/knowhere.tar.gz" -C "${TEMP_DIR}"

# Create install directory if needed
mkdir -p "${INSTALL_DIR}"

# Install binary
mv "${TEMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

echo ""
echo "Knowhere installed successfully to ${INSTALL_DIR}/${BINARY_NAME}"
echo ""

# Check if install dir is in PATH
if [[ ":${PATH}:" != *":${INSTALL_DIR}:"* ]]; then
    echo "Add the following to your shell configuration file (.bashrc, .zshrc, etc.):"
    echo ""
    echo "  export PATH=\"\${PATH}:${INSTALL_DIR}\""
    echo ""
fi

echo "Run 'knowhere --help' to get started."
