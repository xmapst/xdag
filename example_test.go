// example_test.go —— godoc 里可运行的示例。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/xmapst/xdag"
)

// exampleTask 是一个最小的 ITask 实现，run 为空时返回固定的 output。
type exampleTask struct {
	name   string
	deps   []string
	run    func(input map[string]any) (map[string]any, error)
	output map[string]any
}

func (t *exampleTask) Name() string           { return t.name }
func (t *exampleTask) Dependencies() []string { return t.deps }
func (t *exampleTask) RetryPolicy() *xdag.RetryPolicy {
	return &xdag.RetryPolicy{MaxAttempts: 1}
}
func (t *exampleTask) PreExecution(context.Context, int64, map[string]any)         {}
func (t *exampleTask) PostExecution(context.Context, int64, map[string]any, error) {}
func (t *exampleTask) Execute(_ context.Context, _ int64, input map[string]any) (map[string]any, error) {
	if t.run != nil {
		return t.run(input)
	}
	return t.output, nil
}

// Example 演示最小可运行的用法：定义几个任务、声明依赖、执行、读取结果。
//
//	fetch-user ─┐
//	            ├─> summary
//	fetch-order ┘
func Example() {
	tasks := map[string]xdag.ITask{
		"fetch-user":  &exampleTask{name: "fetch-user", output: map[string]any{"id": 1001, "name": "Tom"}},
		"fetch-order": &exampleTask{name: "fetch-order", output: map[string]any{"amount": 199}},
		"summary": &exampleTask{
			name: "summary", deps: []string{"fetch-user", "fetch-order"},
			run: func(input map[string]any) (map[string]any, error) {
				return map[string]any{"user": input["fetch-user"], "order": input["fetch-order"]}, nil
			},
		},
	}

	dag, err := xdag.New(tasks)
	if err != nil {
		panic(err)
	}
	results, err := dag.Execute(context.Background())
	if err != nil {
		panic(err)
	}

	summary := results["summary"]
	fmt.Println(summary["user"])
	fmt.Println(summary["order"])

	// Output:
	// map[id:1001 name:Tom]
	// map[amount:199]
}

// Example_states 演示失败如何沿依赖链传播，以及如何用 States() 拿到完整视图——
// 结果集只包含成功的任务，States() 才能看到「跳过」与「失败」。
//
//	check(失败) ──> notify ──> report
func Example_states() {
	tasks := map[string]xdag.ITask{
		"check":  &exampleTask{name: "check", run: func(map[string]any) (map[string]any, error) { return nil, errors.New("service down") }},
		"notify": &exampleTask{name: "notify", deps: []string{"check"}},
		"report": &exampleTask{name: "report", deps: []string{"notify"}},
	}

	dag, err := xdag.New(tasks)
	if err != nil {
		panic(err)
	}
	_, _ = dag.Execute(context.Background()) // check 必然失败，忽略聚合错误

	states := dag.States()
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("%-8s %s\n", name, states[name])
	}

	// Output:
	// check    failed
	// notify   skipped
	// report   skipped
}

// Example_businessCondition 演示 xdag 不提供的能力：条件分支。
//
// xdag 的调度语义只有一条规则——全部依赖成功，任务才执行；它不会替任务
// 判断“要不要做”。需要按业务状态决定是否真正执行某个步骤，请在任务自身
// 的 Execute() 里判断，并把决定写进输出，供下游按需读取：不想让下游知道
// 这一步“被跳过”，就返回一个下游能识别的标记，而不是报错。
//
//	check ──> notify（按 check 的输出自行决定要不要真的发通知）
func Example_businessCondition() {
	tasks := map[string]xdag.ITask{
		"check": &exampleTask{name: "check", output: map[string]any{"code": 500}},
		"notify": &exampleTask{
			name: "notify", deps: []string{"check"},
			run: func(input map[string]any) (map[string]any, error) {
				check, _ := input["check"].(map[string]any)
				if check["code"] != 200 {
					// 业务层的分支判断：这一步“不做”，但对调度器而言仍然是成功的
					return map[string]any{"sent": false, "reason": "check failed"}, nil
				}
				return map[string]any{"sent": true}, nil
			},
		},
	}

	dag, err := xdag.New(tasks)
	if err != nil {
		panic(err)
	}
	results, err := dag.Execute(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(results["notify"]["sent"], results["notify"]["reason"])

	// Output:
	// false check failed
}

// Example_subgraph 演示把一整张子图当作一个任务：任务在自己的 Execute 里
// 构造并运行一个新的 Scheduler，把内层结果汇总成自己的 output。
//
// xdag 没有、也不需要「嵌套 DAG」这个概念——ITask 只是一个接口，里面跑什么
// 都行，包括另一个 Scheduler。需要注意的只有一点：如果外层用了
// WithMaxConcurrency，外层任务占着一个名额的同时内层还有自己独立的上限，
// 实际并发是两者相乘。
//
//	prepare ──> batch（内部是一张三个任务的子图）
func Example_subgraph() {
	tasks := map[string]xdag.ITask{
		"prepare": &exampleTask{name: "prepare", output: map[string]any{"n": 3}},
		"batch": &exampleTask{
			name: "batch", deps: []string{"prepare"},
			run: func(input map[string]any) (map[string]any, error) {
				prepare, _ := input["prepare"].(map[string]any)
				n, _ := prepare["n"].(int)

				// 按上游给的数量动态构造一张子图
				sub := make(map[string]xdag.ITask, n)
				for i := range n {
					name := fmt.Sprintf("item-%d", i)
					sub[name] = &exampleTask{name: name, output: map[string]any{"i": i}}
				}

				inner, err := xdag.New(sub)
				if err != nil {
					return nil, err
				}
				results, err := inner.Execute(context.Background())
				if err != nil {
					return nil, err
				}
				return map[string]any{"processed": len(results)}, nil
			},
		},
	}

	dag, err := xdag.New(tasks)
	if err != nil {
		panic(err)
	}
	results, err := dag.Execute(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(results["batch"]["processed"])

	// Output:
	// 3
}
