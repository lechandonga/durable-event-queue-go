package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// groupStateVersion 是状态文件格式版本。
const groupStateVersion = 1

// inflightState 记录一条已拉取未确认消息的状态。
type inflightState struct {
	Offset        int64 `json:"offset"`
	Attempts      int   `json:"attempts"`
	FirstPullUnix int64 `json:"first_pull_unix"`
	DeadlineUnix  int64 `json:"deadline_unix"` // 可见性超时时刻
}

// deadLetter 是一条进入死信队列的消息快照。
type deadLetter struct {
	Offset       int64  `json:"offset"`
	Key          string `json:"key"`
	Payload      []byte `json:"payload"`
	Attempts     int    `json:"attempts"`
	LastError    string `json:"last_error"`
	DeadUnixNano int64  `json:"dead_unix_nano"`
}

// groupState 是消费组的完整持久化状态。
type groupState struct {
	Version         int                      `json:"version"`
	Name            string                   `json:"name"`
	CommittedOffset int64                    `json:"committed_offset"` // 已提交位点（之前的消息不再投递）
	Inflight        map[int64]*inflightState `json:"inflight"`         // offset -> 拉取中
	Acked           map[int64]bool           `json:"acked"`            // 已 ack 待提交
	Retry           map[int64]int64          `json:"retry_not_before"` // offset -> 下次可投递时间
	Attempts        map[int64]int            `json:"attempts"`         // offset -> 已投递次数
	DeadLetters     []*deadLetter            `json:"dead_letters"`
	MaxDelivered    int64                    `json:"max_delivered"` // 曾拉取过的最大 offset
}

func newGroupState(name string, startOffset int64) *groupState {
	return &groupState{
		Version:         groupStateVersion,
		Name:            name,
		CommittedOffset: startOffset,
		Inflight:        make(map[int64]*inflightState),
		Acked:           make(map[int64]bool),
		Retry:           make(map[int64]int64),
		Attempts:        make(map[int64]int),
		MaxDelivered:    startOffset - 1,
	}
}

// groupStore 管理一个消费组的状态文件（原子 rename + fsync）。
type groupStore struct {
	path    string
	dir     string
	tmpPath string
}

func newGroupStore(dataDir, name string) *groupStore {
	dir := filepath.Join(dataDir, "groups")
	return &groupStore{
		path:    filepath.Join(dir, name+".json"),
		tmpPath: filepath.Join(dir, name+".json.tmp"),
		dir:     dir,
	}
}

// load 读取状态文件；文件不存在时返回 (nil, nil) 表示该组尚未创建。
func (s *groupStore) load() (*groupState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var st groupState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("group %s: corrupt state file: %w", s.path, err)
	}
	if st.Version != groupStateVersion {
		return nil, fmt.Errorf("group %s: unsupported state version %d", s.path, st.Version)
	}
	if st.Inflight == nil {
		st.Inflight = make(map[int64]*inflightState)
	}
	if st.Acked == nil {
		st.Acked = make(map[int64]bool)
	}
	if st.Retry == nil {
		st.Retry = make(map[int64]int64)
	}
	if st.Attempts == nil {
		st.Attempts = make(map[int64]int)
	}
	return &st, nil
}

// persist 以“临时文件 + fsync + rename + 目录 fsync”原子替换状态文件。
func (s *groupStore) persist(st *groupState) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // 数据落盘
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.tmpPath, s.path); err != nil {
		return err
	}
	return fsyncDir(s.dir) // rename 落盘
}

func (s *groupStore) exists() (bool, error) {
	if _, err := os.Stat(s.path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *groupStore) close() error { return nil }

// ConsumerGroup 是引擎对外暴露的消费组句柄（轻量值，实际逻辑在 Queue 上）。
type ConsumerGroup struct {
	name string
	q    *Queue
}

func (g *ConsumerGroup) Name() string { return g.name }

// Pull 阻塞拉取最多 max 条消息。
func (g *ConsumerGroup) Pull(max int, timeout time.Duration) ([]*DeliveredMessage, error) {
	return g.q.Pull(g.name, max, timeout)
}

// Ack 确认单条消息。
func (g *ConsumerGroup) Ack(offset int64) error { return g.q.Ack(g.name, offset) }

// Nack 否定确认，按退避策略重试；超限进死信。
func (g *ConsumerGroup) Nack(offset int64, cause error) error {
	return g.q.Nack(g.name, offset, cause)
}

// Commit 显式提交位点；回退请求会被拒绝。
func (g *ConsumerGroup) Commit(offset int64) error { return g.q.Commit(g.name, offset) }

// CommittedOffset 查询已提交位点。
func (g *ConsumerGroup) CommittedOffset() (int64, error) {
	return g.q.CommittedOffset(g.name)
}

// DeadLetters 返回死信快照。
func (g *ConsumerGroup) DeadLetters() ([]*deadLetter, error) {
	return g.q.DeadLetters(g.name)
}

// PullChan 持续向返回的通道投递消息，直到队列关闭；
// 每条消息的处理结果必须调用 done（Ack 或 Nack）。
func (g *ConsumerGroup) PullChan(buffer int) (<-chan *DeliveredMessage, func(int64, bool, error)) {
	if buffer <= 0 {
		buffer = 16
	}
	ch := make(chan *DeliveredMessage, buffer)
	go func() {
		defer close(ch)
		for {
			msgs, err := g.q.Pull(g.name, 1, 500*time.Millisecond)
			if err != nil {
				if errors.Is(err, ErrQueueClosed) {
					return
				}
				if errors.Is(err, ErrTimeout) {
					continue
				}
				return
			}
			for _, m := range msgs {
				ch <- m
			}
		}
	}()
	done := func(offset int64, ok bool, cause error) {
		if ok {
			_ = g.q.Ack(g.name, offset)
		} else {
			_ = g.q.Nack(g.name, offset, cause)
		}
	}
	return ch, done
}

// fsyncDir fsync 一个目录，保证该目录下的 create/rename 元数据在崩溃后仍然存在。
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
