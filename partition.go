package eventqueue

import "hash/fnv"

// partitionOf 按 key 的 FNV-1a 哈希计算分区，保证同一 key 永远落在同一分区，
// 从而分区内可以严格按写入顺序消费。
func partitionOf(key string, partitions uint32) uint32 {
	if partitions == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % partitions
}
