cask "knowhere" do
  version "0.1.0"
  sha256 "PLACEHOLDER_SHA256"
  
  url "https://github.com/saivarunk/knowhere/releases/download/v#{version}/Knowhere_#{version}_universal.dmg"
  name "Knowhere"
  desc "SQL engine for querying CSV, Parquet, Delta Lake files"
  homepage "https://github.com/saivarunk/knowhere"
  
  livecheck do
    url :url
    strategy :github_latest
  end

  app "Knowhere.app"
  
  zap trash: [
    "~/Library/Application Support/com.knowhere.app",
    "~/Library/Preferences/com.knowhere.app.plist",
    "~/Library/Caches/com.knowhere.app",
  ]
end
