package xdag

// State 表示一个任务在本次执行中的终态。
type State uint8

const (
	// StatePending 任务尚未被调度。
	StatePending State = iota
	// StateSuccess 任务执行成功，输出已写入结果集。
	StateSuccess
	// StateSkipped 任务的所有入边都失活，没有依赖构成执行它的理由，任务体未执行。
	StateSkipped
	// StateUpstreamSkipped 任务存在生效的入边，但该边的上游未成功（跳过或失败）。
	StateUpstreamSkipped
	// StateFailed 任务执行失败，或条件表达式求值出错。
	StateFailed
)

// String 实现 fmt.Stringer。
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateSuccess:
		return "success"
	case StateSkipped:
		return "skipped"
	case StateUpstreamSkipped:
		return "upstream_skipped"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Done 报告任务是否已进入终态。
func (s State) Done() bool { return s != StatePending }
