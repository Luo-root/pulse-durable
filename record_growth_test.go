package durable

// CloneRecordForGrowthBenchmark 仅向外部测试暴露真实克隆，
// 使其也能导入 filestore 而不引入包导入环。
func CloneRecordForGrowthBenchmark(record RunRecord) RunRecord {
	return cloneRecord(record)
}
