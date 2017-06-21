# Clear test data from previous runs
File.rm_rf("test/data/")

# Start application
Postnord.main([])

ExUnit.start()
