// check.go —— 构图期的校验：依赖是否存在、有没有环、名字对不对得上。

package xdag

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

var (
	// ErrCircularDependency 任务图中存在环：从某个任务出发，沿依赖边最终能绕回自身。
	ErrCircularDependency = errors.New("circular dependency detected")
	// ErrUnknownDependency 任务声明了一个不存在于任务表中的依赖名。
	ErrUnknownDependency = errors.New("unknown dependency")
)

// snapshotDeps 把每个任务的依赖列表拷贝一份冻结下来，返回按任务名索引的副本。
//
// ITask.Dependencies() 是调用方实现的，返回值既可能是共享切片，也可能每次
// 调用返回不同结果。校验、入度计算、依赖建边、调度判定如果各自独立调用
// Dependencies()，用到的就可能不是同一张图——计数与实际递减次数一旦对不上，
// 任务会永久停在 StatePending，或在依赖尚未完成时被错误地判定。
// 因此整个 Scheduler 生命周期只在 New 里调用这一次。
func snapshotDeps(tasks map[string]ITask) map[string][]string {
	out := make(map[string][]string, len(tasks))
	for name, task := range tasks {
		if task == nil {
			continue
		}
		out[name] = slices.Clone(task.Dependencies())
	}
	return out
}

// Validate 校验任务图：每个任务都必须非 nil、依赖必须都存在于任务表中，
// 且依赖关系不能成环。校验失败时返回的错误会指出具体是哪个任务、哪条边出的问题。
func Validate(tasks map[string]ITask) error {
	return validateGraph(tasks, snapshotDeps(tasks))
}

// validateGraph 在冻结的依赖快照上做校验，保证看到的与调度器实际使用的是同一张图。
func validateGraph(tasks map[string]ITask, deps map[string][]string) error {
	// 1. 悬空依赖：某个任务依赖了任务表里不存在的名字
	var dangling []string
	for name, task := range tasks {
		if task == nil {
			return fmt.Errorf("task %q is nil", name)
		}
		for _, dep := range deps[name] {
			if _, ok := tasks[dep]; !ok {
				dangling = append(dangling, fmt.Sprintf("%s -> %s", name, dep))
			}
		}
	}
	if len(dangling) > 0 {
		slices.Sort(dangling)
		return fmt.Errorf("%w: %s", ErrUnknownDependency, strings.Join(dangling, ", "))
	}

	// 2. 环检测，DFS 三色法；path 用于还原环上的具体路径，便于报错定位
	const (
		white = 0 // 未访问
		gray  = 1 // 在当前递归栈上
		black = 2 // 已完成，不可能再成环
	)
	color := make(map[string]int, len(tasks))
	var path []string
	var cycle []string

	var dfs func(string) bool
	dfs = func(name string) bool {
		switch color[name] {
		case gray:
			// 撞上了递归栈里的祖先，从 path 中把环截出来
			for i, n := range path {
				if n == name {
					cycle = append(slices.Clone(path[i:]), name)
					break
				}
			}
			return true
		case black:
			return false
		}
		color[name] = gray
		path = append(path, name)
		if slices.ContainsFunc(deps[name], dfs) {
			return true
		}
		path = path[:len(path)-1]
		color[name] = black
		return false
	}

	// 按名字排序后再遍历，保证多个环同时存在时报出的错误稳定可复现
	for _, name := range slices.Sorted(maps.Keys(tasks)) {
		if color[name] == white && dfs(name) {
			return fmt.Errorf("%w: %s", ErrCircularDependency, strings.Join(cycle, " -> "))
		}
	}
	return nil
}
