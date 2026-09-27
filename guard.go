package eventqueue

import (
	"sync"
	"time"
)

// GuardResult 幂等判定结果。
type GuardResult int

const (
	// GuardFirstTime 该幂等键首次出现，可以执行业务副作用。
	GuardFirstTime GuardResult = iota
	// GuardDuplicate 该幂等键此前已确认生效，必须跳过副作用。
	GuardDuplicate
	// GuardLeased 该幂等键正被另一次进行中的尝试持有（租约内）。
	GuardLeased
)

func (r GuardResult) String() string {
	switch r {
	case GuardFirstTime:
		return "first-time"
	case GuardDuplicate:
		return "duplicate"
	case GuardLeased:
		return "leased"
	default:
		return "unknown"
	}
}

// dedupStatus 幂等键生命周期状态。
type dedupStatus uint8

const (
	statusLeased    dedupStatus = 1 // 进行中：崩溃/超时后允许重新 claim
	statusConfirmed dedupStatus = 2 // 业务副作用已生效：永久判重
)

// dedupEntry 单个幂等键的状态。
type dedupEntry struct {
	status        dedupStatus
	offset        int64
	leasedAt      time.Time
	leaseDeadline time.Time
	// committing 表示 claim 已成功、正处于“落盘 dedup+commit”过程中。
	// 此期间后台租约扫描不得回收该键（它即将成为 confirmed）。
	committing bool
}

// expiredLease 表示一个租约超时、需要被重新投递的幂等键。
type expiredLease struct {
	group    string
	dedupKey string
	offset   int64
}

// idempotencyGuard 在每个消费组内维护业务幂等键状态。
// 该结构只负责内存状态；confirmed 状态的持久化由 store 通过
// metaWAL(recDedup) + 快照完成。
type idempotencyGuard struct {
	mu     sync.Mutex
	groups map[string]map[string]*dedupEntry
}

func newIdempotencyGuard() *idempotencyGuard {
	return &idempotencyGuard{groups: make(map[string]map[string]*dedupEntry)}
}

func (g *idempotencyGuard) groupMap(group string) map[string]*dedupEntry {
	m, ok := g.groups[group]
	if !ok {
		m = make(map[string]*dedupEntry)
		g.groups[group] = m
	}
	return m
}

// loadConfirmed 用磁盘上已确认的幂等键初始化（仅启动重放时使用）。
func (g *idempotencyGuard) loadConfirmed(group, dedupKey string, offset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.groupMap(group)
	// 重放按顺序进行，后者覆盖前者。
	m[dedupKey] = &dedupEntry{status: statusConfirmed, offset: offset}
}

// claim 尝试占用幂等键：
//   - 首次 / 租约已过期：置 leased 并返回 GuardFirstTime；
//   - 已确认：返回 GuardDuplicate（绝不覆盖 confirmed 状态）；
//   - 租约仍被另一次尝试持有：返回 GuardLeased。
func (g *idempotencyGuard) claim(group, dedupKey string, now time.Time, lease time.Duration, offset int64) (GuardResult, error) {
	if dedupKey == "" {
		return GuardFirstTime, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.groupMap(group)
	e, ok := m[dedupKey]
	if !ok {
		m[dedupKey] = &dedupEntry{
			status:        statusLeased,
			offset:        offset,
			leasedAt:      now,
			leaseDeadline: now.Add(lease),
			committing:    true,
		}
		return GuardFirstTime, nil
	}
	switch e.status {
	case statusConfirmed:
		return GuardDuplicate, nil
	case statusLeased:
		if !e.committing && now.Before(e.leaseDeadline) {
			return GuardLeased, nil
		}
		e.offset = offset
		e.leasedAt = now
		e.leaseDeadline = now.Add(lease)
		e.committing = true
		return GuardFirstTime, nil
	}
	return GuardLeased, nil
}

// confirm 将幂等键置为永久生效。已 confirmed 的重复确认返回 false。
func (g *idempotencyGuard) confirm(group, dedupKey string, offset int64) bool {
	if dedupKey == "" {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.groupMap(group)
	e, ok := m[dedupKey]
	if ok && e.status == statusConfirmed {
		return false
	}
	if !ok {
		e = &dedupEntry{}
		m[dedupKey] = e
	}
	e.status = statusConfirmed
	e.offset = offset
	e.committing = false
	e.leaseDeadline = time.Time{}
	return true
}

// abortCommit 在落盘失败时撤销 claim 的提交中标记，使键可被重新 claim 或回收。
func (g *idempotencyGuard) abortCommit(group, dedupKey string) {
	if dedupKey == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.groups[group]; ok {
		if e, ok := m[dedupKey]; ok && e.status == statusLeased && e.committing {
			e.committing = false
		}
	}
}

// release 放弃占用（Nack 或重试），使幂等键回到可重新 claim 的状态。
// confirmed 的键不受影响。
func (g *idempotencyGuard) release(group, dedupKey string) {
	if dedupKey == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.groups[group]; ok {
		if e, ok := m[dedupKey]; ok && e.status == statusLeased {
			delete(m, dedupKey)
		}
	}
}

// statusOf 查询状态，不存在时返回 GuardFirstTime（调用方据此执行）。
func (g *idempotencyGuard) statusOf(group, dedupKey string) GuardResult {
	if dedupKey == "" {
		return GuardFirstTime
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.groups[group]
	if !ok {
		return GuardFirstTime
	}
	e, ok := m[dedupKey]
	if !ok {
		return GuardFirstTime
	}
	switch e.status {
	case statusConfirmed:
		return GuardDuplicate
	default:
		return GuardLeased
	}
}

// expireLeases 找出所有已超时的 leased 键并释放，返回需要重新投递的列表。
func (g *idempotencyGuard) expireLeases(now time.Time) []expiredLease {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []expiredLease
	for group, m := range g.groups {
		for k, e := range m {
			// 提交落盘中的键即将成为 confirmed，不能按租约超时回收。
			if e.status == statusLeased && !e.committing && !now.Before(e.leaseDeadline) {
				out = append(out, expiredLease{group: group, dedupKey: k, offset: e.offset})
				delete(m, k)
			}
		}
	}
	return out
}

// confirmedSnapshot 返回每个组当前已确认的幂等键（用于写快照/GC 判断）。
func (g *idempotencyGuard) confirmedSnapshot() map[string]map[string]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]map[string]int64, len(g.groups))
	for group, m := range g.groups {
		var cm map[string]int64
		for k, e := range m {
			if e.status != statusConfirmed {
				continue
			}
			if cm == nil {
				cm = make(map[string]int64)
			}
			cm[k] = e.offset
		}
		if len(cm) > 0 {
			out[group] = cm
		}
	}
	return out
}

// gcConfirmed 在“消息物理分段已被删除”之后回收对应的 confirmed 键：
// 只有当键所记录 offset 的消息已不在内存索引中时才删除——这意味着该消息
// 的物理数据已被分段 GC，不可能再被重新投递或重复提交，此时判重表才可安全回收。
// 绝不能仅按位点越过回收：位点越过之后，重投/重复确认旧 dedupKey 的请求
// 仍可能到达，必须继续判定 GuardDuplicate。
func (g *idempotencyGuard) gcConfirmed(offsetExists func(offset int64) bool) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	removed := 0
	for _, m := range g.groups {
		for k, e := range m {
			if e.status != statusConfirmed {
				continue
			}
			if !offsetExists(e.offset) {
				delete(m, k)
				removed++
			}
		}
	}
	return removed
}
