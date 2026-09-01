// progress.go —— Progress：一次执行的进度快照。

package xdag

import "fmt"

// Progress 是一次执行的进度快照：每种状态各有多少个任务。
//
// 这里刻意只给**原始计数**，不给「完成度」之类的判断。计数是无观点的事实，
// 调用方对「什么算完成」有不同口径时（跳过算不算数？取消算不算失败？）都能
// 自己从计数重新推导；反过来，库若只暴露一个百分比，就等于替调用方把口径
// 定死了。Done/Ratio 只是最常用那一种口径的便利方法。
//
// 各字段之和恒等于 Total。
type Progress struct {
	// Total 是图中的任务总数，执行全程不变。
	Total int
	// Pending 是尚未进入终态的任务数：在等依赖、被挂起、或正在执行中。
	Pending int
	// Success 是执行成功的任务数。
	Success int
	// Skipped 是因为依赖没跑成而被跳过的任务数。
	Skipped int
	// Canceled 是被取消的任务数（含整场取消、单任务取消、以及优雅停机）。
	Canceled int
	// Failed 是执行失败的任务数。
	Failed int
}

// Done 返回已经进入终态的任务数，即 Total - Pending。
func (p Progress) Done() int { return p.Total - p.Pending }

// Ratio 返回已进入终态的任务占比，取值 [0, 1]。
// 空图返回 1——没有任务要做，也就没有没做完的，与 Phase 对空图报
// PhaseSuccess 保持一致。
func (p Progress) Ratio() float64 {
	if p.Total == 0 {
		return 1
	}
	return float64(p.Done()) / float64(p.Total)
}

// String 实现 fmt.Stringer，给日志用的紧凑形式。
func (p Progress) String() string {
	return fmt.Sprintf("%d/%d done (success %d, skipped %d, canceled %d, failed %d)",
		p.Done(), p.Total, p.Success, p.Skipped, p.Canceled, p.Failed)
}

// Progress 返回当前的进度快照，执行期间与结束后都可以并发调用。
//
// 它只取一次锁、遍历一遍状态表，不像 States() 那样复制整张 map——进度条
// 这类高频轮询的场景应当用它。需要逐个任务的状态才用 States()。
//
// 快照在取锁的那一瞬间是自洽的（各字段之和恒等于 Total），但执行期间调用
// 时，返回之后状态就可能继续变化。
func (d *Scheduler) Progress() Progress {
	d.mu.Lock()
	defer d.mu.Unlock()

	p := Progress{Total: len(d.states)}
	for _, s := range d.states {
		switch s {
		case StateSuccess:
			p.Success++
		case StateSkipped:
			p.Skipped++
		case StateCanceled:
			p.Canceled++
		case StateFailed:
			p.Failed++
		default:
			p.Pending++
		}
	}
	return p
}
