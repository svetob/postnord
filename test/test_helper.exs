require Logger

# Clear data
Logger.info "--- CLEARING TEST DATA ---"
File.rm_rf!("test/data")

# Start application
Logger.info "--- STARTING POSTNORD ---"
Postnord.main([])

ExUnit.start(capture_log: true, timeout: 10_000_000)
