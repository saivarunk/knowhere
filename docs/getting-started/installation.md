# Installation

## Desktop GUI

### macOS (Homebrew Cask)

```bash
brew tap saivarunk/knowhere
brew install --cask knowhere
```

### Linux

Download the `.deb` or `.AppImage` from the [releases page](https://github.com/saivarunk/knowhere/releases).

```bash
# Debian/Ubuntu
sudo dpkg -i knowhere_*.deb

# AppImage
chmod +x Knowhere_*.AppImage
./Knowhere_*.AppImage
```

---

## Command Line (TUI)

### Homebrew (macOS/Linux)

```bash
brew tap saivarunk/knowhere
brew install knowhere
```

### Install Script

```bash
curl -fsSL https://raw.githubusercontent.com/saivarunk/knowhere/main/install.sh | bash
```

### From Source

```bash
git clone https://github.com/saivarunk/knowhere.git
cd knowhere
cargo build --release
```

The binary will be at `./target/release/knowhere`.

## Verify Installation

```bash
knowhere --version
```
