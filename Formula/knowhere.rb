class Knowhere < Formula
  desc "A lightweight SQL engine for querying CSV and Parquet files via TUI"
  homepage "https://github.com/saivarunk/knowhere"
  version "0.1.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/saivarunk/knowhere/releases/download/v#{version}/knowhere-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_SHA256_ARM64"
    end

    on_intel do
      url "https://github.com/saivarunk/knowhere/releases/download/v#{version}/knowhere-v#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_SHA256_X86_64"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/saivarunk/knowhere/releases/download/v#{version}/knowhere-v#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_SHA256_LINUX_ARM64"
    end

    on_intel do
      url "https://github.com/saivarunk/knowhere/releases/download/v#{version}/knowhere-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_SHA256_LINUX_X86_64"
    end
  end

  def install
    bin.install "knowhere"
  end

  test do
    # Create a test CSV file
    (testpath/"test.csv").write <<~CSV
      id,name,value
      1,foo,100
      2,bar,200
    CSV

    # Run a simple query
    output = shell_output("#{bin}/knowhere --query 'SELECT * FROM test' #{testpath}/test.csv")
    assert_match "foo", output
    assert_match "bar", output
  end
end
