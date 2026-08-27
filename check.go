package xdag

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	// ErrCircularDependency 任务图中存在环。
	ErrCircularDependency = errors.New("circular dependency detected")
	// ErrUnknownDependency 任务声明了一个不存在的依赖。
	ErrUnknownDependency = errors.New("unknown dependency")
)

// dependenciesOf 安全地取出任务的依赖列表。
// map 中不存在的 key 会返回 nil 接口，直接调用其方法会 panic，因此这里显式判空。
func dependenciesOf(tasks map[string]Task, name string) []string {
	task, ok := tasks[name]
	if !ok || task == nil {
		return nil
	}
	return task.Dependencies()
}

// Validate 校验任务图：依赖必须存在，且不能成环。
// 相比 HasCycle，它会给出具体的任务名，便于定位问题。
func Validate(tasks map[string]Task) error {
	// 1. 悬空依赖
	var dangling []string
	for name, task := range tasks {
		if task == nil {
			return fmt.Errorf("task %q is nil", name)
		}
		for _, dep := range task.Dependencies() {
			if _, ok := tasks[dep]; !ok {
				dangling = append(dangling, fmt.Sprintf("%s -> %s", name, dep))
			}
		}
	}
	if len(dangling) > 0 {
		sort.Strings(dangling)
		return fmt.Errorf("%w: %s", ErrUnknownDependency, strings.Join(dangling, ", "))
	}

	// 2. 环检测，DFS 三色法；path 用于还原环上的具体路径
	const (
		white = 0 // 未访问
		gray  = 1 // 在当前递归栈上
		black = 2 // 已完成
	)
	color := make(map[string]int, len(tasks))
	var path []string
	var cycle []string

	var dfs func(string) bool
	dfs = func(name string) bool {
		switch color[name] {
		case gray:
			// 从 path 中截出环
			for i, n := range path {
				if n == name {
					cycle = append(append([]string{}, path[i:]...), name)
					break
				}
			}
			return true
		case black:
			return false
		}
		color[name] = gray
		path = append(path, name)
		for _, dep := range dependenciesOf(tasks, name) {
			if dfs(dep) {
				return true
			}
		}
		path = path[:len(path)-1]
		color[name] = black
		return false
	}

	names := make([]string, 0, len(tasks))
	for name := range tasks {
		names = append(names, name)
	}
	sort.Strings(names) // 保证错误信息稳定可复现
	for _, name := range names {
		if color[name] == white && dfs(name) {
			return fmt.Errorf("%w: %s", ErrCircularDependency, strings.Join(cycle, " -> "))
		}
	}
	return nil
}

// HasCycle 报告任务图中是否存在循环依赖。不存在的依赖会被当作叶子节点忽略。
//
// Deprecated: 使用 Validate 获取包含任务名的详细错误。
func HasCycle(tasks map[string]Task) bool {
	return errors.Is(cycleOnly(tasks), ErrCircularDependency)
}

func cycleOnly(tasks map[string]Task) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(tasks))

	var dfs func(string) bool
	dfs = func(name string) bool {
		if _, ok := tasks[name]; !ok {
			return false // 悬空依赖当作叶子
		}
		switch color[name] {
		case gray:
			return true
		case black:
			return false
		}
		color[name] = gray
		for _, dep := range dependenciesOf(tasks, name) {
			if dfs(dep) {
				return true
			}
		}
		color[name] = black
		return false
	}

	for name := range tasks {
		if color[name] == white && dfs(name) {
			return ErrCircularDependency
		}
	}
	return nil
}

// computeAncestors 为每个任务计算传递闭包祖先集合。
// 调用前必须保证任务图无环且无悬空依赖。
func computeAncestors(tasks map[string]Task) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{}, len(tasks))

	var visit func(string) map[string]struct{}
	visit = func(name string) map[string]struct{} {
		if set, ok := result[name]; ok {
			return set
		}
		set := make(map[string]struct{})
		result[name] = set // 无环，先占位即可，不会被提前读到不完整的值
		for _, dep := range dependenciesOf(tasks, name) {
			set[dep] = struct{}{}
			for a := range visit(dep) {
				set[a] = struct{}{}
			}
		}
		return set
	}

	for name := range tasks {
		visit(name)
	}
	return result
}
