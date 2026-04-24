package diff

// Test-only knobs for package-internal state. These are not part of the public API.

func LargeTableThresholdForTest() int64         { return largeTableThreshold }
func SetLargeTableThresholdForTest(n int64)     { largeTableThreshold = n }
