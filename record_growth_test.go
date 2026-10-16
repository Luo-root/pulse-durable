package durable

// CloneRecordForGrowthBenchmark exposes the real clone only to external tests,
// which can also import filestore without introducing a package import cycle.
func CloneRecordForGrowthBenchmark(record RunRecord) RunRecord {
	return cloneRecord(record)
}
