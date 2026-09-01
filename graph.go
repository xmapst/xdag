// graph.go —— 依赖图的可视化输出：把依赖关系打印成人能读的形状。
//
// 纯只读，不参与调度，任何时候调用都安全。

package xdag

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

// ExecutionOrder 返回全部**成功**任务按完成先后排列的编号列表（文本形式）。
// 未执行、被取消与失败的任务不在其中，需要完整视图请用 States。
//
// 这是完成顺序，不是静态拓扑序：并发任务之间谁先谁后并不确定，
// 多次执行同一张图，顺序完全可能不同。
func (d *Scheduler) ExecutionOrder() string {
	d.mu.Lock()
	order := slices.Clone(d.executionOrder)
	d.mu.Unlock()

	var sb = strings.Builder{}
	sb.WriteString("\n")
	for i, step := range order {
		_, _ = fmt.Fprintf(&sb, "%d. %s\n", i+1, step)
	}
	return sb.String()
}

// PrintGraph 把依赖图打印到标准输出，等价于 WriteGraph(os.Stdout)。
// 它只是个调试用的可视化工具，不应在执行过程中依赖它的输出做判断。
func (d *Scheduler) PrintGraph() {
	_ = d.WriteGraph(os.Stdout)
}

// WriteGraph 把从每个根任务出发的依赖链写入 w，便于直观检查图结构。
//
// 输出是确定的：根任务与每层下游都按名字排序，同一张图多次输出完全一致。
// 汇聚点（被多个上游指向的任务）只在第一次出现时展开整棵子树，之后再出现
// 只画那条边并以 "..." 收尾——否则菱形结构会被重复展开，输出规模随菱形
// 层数指数膨胀。
//
// 任意时刻调用都安全：它只读 New 阶段就冻结下来的依赖快照，不读执行过程中
// 会被消费掉的入度表，也不会再去调用 ITask.Dependencies()。
func (d *Scheduler) WriteGraph(w io.Writer) error {
	// 根节点取自冻结的 depOrder 而不是 task.Dependencies()：后者是调用方
	// 实现的，完全可能在 New 之后改口，那样同一个任务会既出现在某条链的
	// 下游、又被当成根打印一遍
	roots := make([]string, 0, len(d.depOrder))
	for name, deps := range d.depOrder {
		if len(deps) == 0 {
			roots = append(roots, name)
		}
	}
	slices.Sort(roots)

	expanded := make(map[string]bool, len(d.depOrder))
	for _, root := range roots {
		if _, err := fmt.Fprintln(w, root); err != nil {
			return err
		}
		if err := d.writeChain(w, root, "  ", expanded); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}

// writeChain 递归写出 name 的下游依赖链，prefix 是当前缩进前缀，
// expanded 记录哪些任务的子树已经展开过。
func (d *Scheduler) writeChain(w io.Writer, name, prefix string, expanded map[string]bool) error {
	// 收集 + 排序一步到位；slices.Sorted 收进的是一份新切片，
	// 不会就地改动 d.dependents[name]。
	children := slices.Sorted(slices.Values(d.dependents[name]))

	for _, child := range children {
		if expanded[child] {
			_, err := fmt.Fprintf(w, "%s└─> %s ...\n", prefix, child)
			if err != nil {
				return err
			}
			continue
		}
		expanded[child] = true
		if _, err := fmt.Fprintf(w, "%s└─> %s\n", prefix, child); err != nil {
			return err
		}
		if err := d.writeChain(w, child, prefix+"    ", expanded); err != nil {
			return err
		}
	}
	return nil
}
