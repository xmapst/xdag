package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/xmapst/xdag"
	"github.com/xmapst/xdag/xexpr"
)

// branchTask 是一个带条件的最小任务实现。
type branchTask struct {
	name      string
	deps      []string
	edgeConds map[string]string
	output    map[string]any
	err       error
}

func (t *branchTask) Name() string                                                { return t.name }
func (t *branchTask) Dependencies() []string                                      { return t.deps }
func (t *branchTask) RetryPolicy() *xdag.RetryPolicy                              { return nil }
func (t *branchTask) PreExecution(context.Context, int64, map[string]any)         {}
func (t *branchTask) PostExecution(context.Context, int64, map[string]any, error) {}
func (t *branchTask) Condition(dep string) string                                 { return t.edgeConds[dep] }
func (t *branchTask) Execute(context.Context, int64, map[string]any) (map[string]any, error) {
	return t.output, t.err
}

// Example_conditionalBranch 演示条件分支与分支汇聚。
// 分支与汇聚都挂在依赖边上——边条件是 xdag 唯一的分支判断入口。
//
//	         ┌─ notify-ok   (code == 200) ─┐
//	check ───┤                             ├──> report (任一分支成功即可)
//	         └─ rollback    (code != 200) ─┘
func Example_conditionalBranch() {
	tasks := map[string]xdag.Task{
		"check": &branchTask{
			name:   "check",
			output: map[string]any{"code": 500},
		},
		"notify-ok": &branchTask{
			name: "notify-ok", deps: []string{"check"},
			edgeConds: map[string]string{"check": `Output("check").code == 200`},
		},
		"rollback": &branchTask{
			name: "rollback", deps: []string{"check"},
			edgeConds: map[string]string{"check": `Output("check").code != 200`},
		},
		"report": &branchTask{
			name: "report", deps: []string{"notify-ok", "rollback"},
			// 分支汇聚：默认要求所有依赖成功，这里改为任一成功即可
			edgeConds: map[string]string{
				"notify-ok": `Succeeded(Dep)`,
				"rollback":  `Succeeded(Dep)`,
			},
		},
	}

	dag, err := xdag.New(tasks, xdag.WithEvaluator(xexpr.New()))
	if err != nil {
		panic(err)
	}

	if _, err = dag.Execute(context.Background()); err != nil {
		panic(err)
	}

	states := dag.States()
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("%-10s %s\n", name, states[name])
	}

	// Output:
	// check      success
	// notify-ok  skipped
	// report     success
	// rollback   success
}

// Example_edgeCondition 演示用边条件表达「可选依赖」：
// notify 这条边由全局变量决定是否参与门禁，失活时 notify 失败也不会拦住 deploy。
func Example_edgeCondition() {
	tasks := map[string]xdag.Task{
		"build":  &branchTask{name: "build", output: map[string]any{"artifact": "app.tar"}},
		"notify": &branchTask{name: "notify", err: errors.New("notify service down")},
		"deploy": &branchTask{
			name: "deploy", deps: []string{"build", "notify"},
			// build 边无条件生效；notify 边只在严格模式下参与门禁
			edgeConds: map[string]string{"notify": `Vars["strictNotify"] == true`},
			output:    map[string]any{"ok": true},
		},
	}

	for _, strict := range []bool{false, true} {
		dag, err := xdag.New(tasks,
			xdag.WithEvaluator(xexpr.New()),
			xdag.WithVars(map[string]any{"strictNotify": strict}),
		)
		if err != nil {
			panic(err)
		}
		_, _ = dag.Execute(context.Background()) // notify 必然失败

		fmt.Printf("strictNotify=%-5v deploy=%s\n", strict, dag.State("deploy"))
	}

	// Output:
	// strictNotify=false deploy=success
	// strictNotify=true  deploy=upstream_skipped
}
