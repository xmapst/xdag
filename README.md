# xdag

[`xdag`](README.md) 是一个基于 Go 实现的轻量级 DAG（Directed Acyclic Graph，有向无环图）任务编排库，用于组织具有依赖关系的任务，并按依赖顺序自动并发执行。

实现的核心特点是：

- 基于 [`Task`](task.go:5) 抽象任务节点
- 基于 [`Validate()`](check.go) 在初始化时校验依赖存在性并检测环
- 基于 [`Dagcuter`](dag.go) 统一管理任务图、执行顺序和结果汇总
- 基于 [`Execute()`](dag.go) 启动 DAG 执行
- 基于 [`RetryPolicy`](retry.go:10) 与 [`ExecuteWithRetry()`](retry.go:41) 提供失败重试能力
- 基于 [`Conditional`](evaluator.go) 与 [`Evaluator`](evaluator.go) 提供表达式驱动的条件分支
- 基于 [`State`](state.go:4) 区分成功、跳过、上游跳过与失败四种终态

该项目适合在应用内实现轻量的任务流、步骤依赖执行器、批量异步编排器以及带重试能力的流程调度。

---

## 目录

- [项目概览](#项目概览)
- [功能特性](#功能特性)
- [适用场景](#适用场景)
- [安装方式](#安装方式)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [执行机制说明](#执行机制说明)
- [条件分支](#条件分支)
- [重试机制说明](#重试机制说明)
- [结果与执行顺序](#结果与执行顺序)
- [公开 API](#公开-api)
- [设计约束与注意事项](#设计约束与注意事项)
- [项目结构](#项目结构)
- [许可证](#许可证)

---

## 项目概览

项目模块定义见 [`go.mod`](go.mod:1)：

```go
module github.com/xmapst/xdag
```

主调度器实现位于 [`Dagcuter`](dag.go)，其职责包括：

- 保存任务集合 `Tasks`
- 维护任务执行结果 `results`
- 维护节点入度 `inDegrees`
- 维护反向依赖表 `dependents`
- 记录实际完成顺序 `executionOrder`
- 借助互斥锁和等待组控制并发执行过程

相较于传统串行步骤编排，该项目能够在依赖满足时自动并发执行多个任务，从而提升整体吞吐量。

---

## 功能特性

### 1. DAG 依赖编排

每个任务可以通过 [`Task.Dependencies()`](task.go:7) 声明依赖项。调度器会根据依赖关系自动构建图，并确保任务只在其所有前置任务成功完成后才开始执行。

### 2. 初始化环检测

在调用 [`New()`](dag.go) 创建执行器时，内部会调用 [`Validate()`](check.go) 校验任务图：依赖必须真实存在，且不能成环。任一条件不满足，初始化立即失败并给出具体的任务名。

### 3. 自动并发执行

所有入度为 `0` 的根任务会首先启动。后续任务在依赖全部完成后，会由 [`runTask()`](dag.go) 递归触发新的 goroutine 执行。

### 4. 任务级重试

任务执行失败后，可通过 [`Task.RetryPolicy()`](task.go:9) 返回的 [`RetryPolicy`](retry.go:10) 指定最大尝试次数、退避间隔与倍率。

### 5. 生命周期钩子

任务在执行前后分别会调用：

- [`Task.PreExecution()`](task.go:11)
- [`Task.PostExecution()`](task.go:15)

这使得你可以很方便地注入日志、监控、埋点、审计和失败记录逻辑。

### 6. 统一结果汇总

所有成功执行的任务输出都会被汇总到 [`Execute()`](dag.go) 的返回值中，便于上层调用方统一消费。

### 7. 可追踪执行顺序

调用 [`ExecutionOrder()`](dag.go) 可获取任务的**实际完成顺序**，适用于调试和观测执行过程。

### 8. 依赖图输出

调用 [`PrintGraph()`](dag.go) 可打印从根节点开始的依赖链路，便于理解整个 DAG 结构。

### 9. 条件分支

分支判断挂在**依赖边**上：任务实现可选接口 [`Conditional`](evaluator.go) 为每条依赖边声明守卫，边失活时该依赖不再参与门禁。所有入边都失活的任务被跳过，但下游调度照常推进。表达式引擎通过 [`WithEvaluator()`](option.go) 注入，官方实现见子包 `xexpr`。详见[条件分支](#条件分支)。

### 10. 重试条件

重试可以通过 [`RetryPolicy.RetryIf`](retry.go) 区分可重试与永久性错误，与边条件共用同一套表达式引擎。

### 11. 任务状态

调用 [`States()`](dag.go) / [`State()`](dag.go) 可获取每个任务的终态，区分「跳过」与「失败」，弥补返回结果集只包含成功任务的不足。

---

## 适用场景

[`xdag`](README.md) 适合如下场景：

- 多步骤数据处理流水线
- 业务任务依赖编排
- 批量异步作业调度
- 接口聚合与后处理流程
- 需要失败重试的远程调用任务
- 一个大任务拆分成多个可并发子任务的场景
- 应用内部轻量级流程引擎

如果你的业务中存在“前一步结果作为后一步输入”的模式，并且希望在保证依赖正确的前提下尽量并发执行，当前库是比较合适的基础组件。

---

## 安装方式

使用 [`go get`](README.md) 安装：

```bash
go get github.com/xmapst/xdag
```

---

## 快速开始

### 最小示例

三个任务，其中 `summary` 依赖前两个。这一段只用到根包，不需要任何第三方依赖：

```go
package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/xmapst/xdag"
)

// DemoTask 是一个最小的 Task 实现。
type DemoTask struct {
	name string
	deps []string
	run  func(input map[string]any) map[string]any
}

func (t *DemoTask) Name() string           { return t.name }
func (t *DemoTask) Dependencies() []string { return t.deps }

// 返回 nil 表示只执行一次、不重试
func (t *DemoTask) RetryPolicy() *xdag.RetryPolicy { return nil }

func (t *DemoTask) PreExecution(context.Context, int64, map[string]any)         {}
func (t *DemoTask) PostExecution(context.Context, int64, map[string]any, error) {}

func (t *DemoTask) Execute(_ context.Context, _ int64, input map[string]any) (map[string]any, error) {
	return t.run(input), nil
}

func main() {
	tasks := map[string]xdag.Task{
		"fetch-user": &DemoTask{name: "fetch-user",
			run: func(map[string]any) map[string]any {
				return map[string]any{"id": 1001, "name": "Tom"}
			}},
		"fetch-order": &DemoTask{name: "fetch-order",
			run: func(map[string]any) map[string]any {
				return map[string]any{"orderNo": "ORD-001", "amount": 199}
			}},
		"summary": &DemoTask{name: "summary", deps: []string{"fetch-user", "fetch-order"},
			run: func(input map[string]any) map[string]any {
				return map[string]any{"user": input["fetch-user"], "order": input["fetch-order"]}
			}},
	}

	// 任务图本身有问题（成环、悬空依赖、表达式编译失败）才会在这里报错
	dag, err := xdag.New(tasks)
	if err != nil {
		panic(err)
	}

	// 某个任务失败不会中断整个 DAG，results 里仍有已成功任务的输出，
	// 所以不要在 err != nil 时直接丢弃 results
	results, err := dag.Execute(context.Background())
	if err != nil {
		fmt.Println("部分任务失败:", err)
	}

	names := make([]string, 0, len(tasks))
	for name := range tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("%-12s %-8s %v\n", name, dag.State(name), results[name])
	}
}
```

运行输出：

```text
fetch-order  success  map[amount:199 orderNo:ORD-001]
fetch-user   success  map[id:1001 name:Tom]
summary      success  map[order:map[amount:199 orderNo:ORD-001] user:map[id:1001 name:Tom]]
```

几个要点：

- `fetch-user` 与 `fetch-order` 无依赖，会并发执行；`summary` 在两者都成功后才启动
- [`RetryPolicy()`](task.go:9) 返回 `nil` 表示只执行一次，配置重试见[重试机制说明](#重试机制说明)
- **任务失败不会中断整个 DAG**。[`Execute()`](dag.go) 返回的 `results` 仍然包含已成功任务的输出，`err` 是所有失败任务的聚合，所以不要在 `err != nil` 时直接丢弃 `results`
- `results` 只收录成功的任务，要区分「跳过」和「失败」得用 [`States()`](dag.go) / [`State()`](dag.go)

### 加上分支判断

在上面的基础上加一个 `notify-vip`，只在订单金额达标时才发通知。相对最小示例只有三处改动：

**一、任务实现可选接口 [`Conditional`](evaluator.go)**，为依赖边声明守卫：

```go
// NotifyTask 在 DemoTask 之上只多实现一个方法，就获得了分支能力。
type NotifyTask struct {
    DemoTask
}

// Condition 为依赖边 dep -> notify-vip 声明守卫。
// fetch-order 是 summary 的上游，因而也是本任务的祖先，可以直接引用它的输出。
func (t *NotifyTask) Condition(dep string) string {
    return `Output("fetch-order").amount >= 100`
}
```

**二、把它加进任务表**：

```go
"notify-vip": &NotifyTask{DemoTask{name: "notify-vip", deps: []string{"summary"},
    run: func(map[string]any) map[string]any {
        return map[string]any{"sent": true}
    }}},
```

**三、构建时注入表达式引擎**（`import "github.com/xmapst/xdag/xexpr"`）：

```go
dag, err := xdag.New(tasks, xdag.WithEvaluator(xexpr.New()))
```

金额为 `199` 时通知照发；改成 `49`，`notify-vip` 唯一的入边失活，任务被跳过，而 `summary` 完全不受影响：

```text
--- amount=199 ---
fetch-order  success  map[amount:199 orderNo:ORD-001]
fetch-user   success  map[id:1001 name:Tom]
notify-vip   success  map[sent:true]
summary      success  map[order:map[amount:199 orderNo:ORD-001] user:map[id:1001 name:Tom]]

--- amount=49 ---
fetch-order  success  map[amount:49 orderNo:ORD-001]
fetch-user   success  map[id:1001 name:Tom]
notify-vip   skipped  map[]
summary      success  map[order:map[amount:49 orderNo:ORD-001] user:map[id:1001 name:Tom]]
```

分支、汇聚、可选依赖的完整模式见[条件分支](#条件分支)。

---

## 核心概念

### 1. 任务接口

所有任务都必须实现 [`Task`](task.go:5)：

```go
type Task interface {
    Name() string
    Dependencies() []string
    RetryPolicy() *RetryPolicy
    PreExecution(ctx context.Context, attempt int64, input map[string]any)
    Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error)
    PostExecution(ctx context.Context, attempt int64, output map[string]any, err error)
}
```

各方法职责如下：

- [`Name()`](task.go:6)：返回任务名称
- [`Dependencies()`](task.go:7)：返回依赖任务名列表
- [`RetryPolicy()`](task.go:9)：返回任务级重试策略
- [`PreExecution()`](task.go:11)：执行前回调
- [`Execute()`](task.go:13)：任务主体逻辑
- [`PostExecution()`](task.go:15)：执行后回调

### 2. 可选接口 Conditional

任务可以额外实现 [`Conditional`](evaluator.go) 来声明分支条件，未实现时行为与之前完全一致：

```go
type Conditional interface {
    // 依赖边 dep -> 本任务 的守卫；空串表示该边无条件生效
    Condition(dep string) string
}
```

注意 `Condition` 带 `dep` 参数：它是**逐条依赖边**求值的，不是整个任务一个条件。xdag 没有不带参数的节点级条件，分支判断只归属于边，理由见[为什么条件挂在边上](#为什么条件挂在边上)。

### 3. 任务状态

每个任务最终落在 [`State`](state.go:4) 的一种终态上：

| 状态 | 含义 |
| --- | --- |
| `StateSuccess` | 执行成功，输出进入结果集 |
| `StateSkipped` | 条件表达式为 `false`，任务体未执行 |
| `StateUpstreamSkipped` | 上游未成功（跳过或失败）导致未执行 |
| `StateFailed` | 执行失败，或条件表达式求值出错 |

### 4. 输入参数约定

在 [`snapshot()`](dag.go) 中，调度器会将所有依赖任务的输出收集到下游任务的输入参数中。

例如：

- 若任务 `C` 依赖 `A` 和 `B`
- 则 `C` 的 [`Execute()`](task.go:13) 输入中会包含：
  - `input["A"]`
  - `input["B"]`

此外，在 [`executeTask()`](dag.go) 中，当前尝试次数会以显式参数形式传递给 [`PreExecution()`](task.go:11)、[`Execute()`](task.go:13) 和 [`PostExecution()`](task.go:15)。

其中 [`attempt`](task.go:11) 从 `1` 开始，表示当前是第几次尝试执行该任务。

### 5. 结果缓存

成功执行后的结果会在 [`commit()`](dag.go) 中写入结果集，后续依赖任务即可通过输入参数访问上游结果。

### 6. 入度与依赖关系

在 [`New()`](dag.go) 初始化期间，调度器会建立：

- `inDegrees`：每个任务尚未满足的依赖数量
- `dependents`：每个任务被哪些下游任务依赖

这两个结构共同决定任务何时可以被调度执行。

---

## 执行机制说明

### 初始化阶段

创建 [`Dagcuter`](dag.go) 的入口是 [`New()`](dag.go)。初始化时主要会做以下事情：

1. 检查任务数量是否超过 [`MaxTasks`](dag.go)（可用 [`WithMaxTasks()`](option.go) 覆盖）
2. 校验依赖图，调用 [`Validate()`](check.go)：悬空依赖与环都会在此暴露
3. 初始化结果集、入度表、反向依赖表、状态表
4. 遍历所有任务，构建 `inDegrees` 和 `dependents`
5. 计算每个任务的**传递闭包祖先集合**，用于限定条件表达式的可见范围
6. 预编译所有边条件与 `RetryIf` 表达式——语法错误、类型错误、越界的任务名引用都在这一步一次性抛出，而不是执行到一半才失败

其中：

```go
var MaxTasks = 150
```

这里的 [`MaxTasks`](dag.go) 默认最大任务数量限制

### 启动执行

[`Execute()`](dag.go) 的核心过程：

1. 创建错误通道 `errCh`
2. **先收集**所有入度为 `0` 的根任务，再统一派生 goroutine（边遍历边派生会与子任务对 `inDegrees` 的写入冲突）
3. 为这些根任务启动 goroutine 执行 [`runTask()`](dag.go)
4. 等待所有任务完成
5. 聚合所有错误并返回结果

`Dagcuter` 的入度表在执行过程中会被消费掉，因此不可重复执行，第二次调用返回 [`ErrAlreadyExecuted`](dag.go)。

### 单任务执行流程

[`runTask()`](dag.go) 的逻辑可概括为：

1. 读取任务对象
2. 调用 [`snapshot()`](dag.go) 在锁内准备输入参数与条件求值环境
3. 过入边门禁：
   - 声明了边条件 → 逐条求值，失活的边不参与门禁，生效的边要求上游为 `StateSuccess`
   - 未声明边条件 → 默认要求所有依赖为 `StateSuccess`
   - 全部入边失活 → `StateSkipped`；生效的边上游未成功 → `StateUpstreamSkipped`
4. 需要执行时调用 [`executeTask()`](dag.go) 运行任务
5. 调用 [`commit()`](dag.go) 写入终态：成功才记录执行顺序与输出
6. **无论成功、跳过还是失败**，都遍历子节点将入度减一
7. 若某个子节点入度减为 `0`，则启动新的 goroutine 执行它

第 6 步是分支能力成立的前提：跳过与失败同样要推进下游调度，否则整棵子树会被静默丢弃。

### 实际并发方式

直接通过：

```go
go d.runTask(ctx, child, errCh)
```

递归派生新的 goroutine。也就是说，当前并发度主要受任务图结构、就绪任务数量以及外部运行环境影响。

---

## 条件分支

### 启用方式

条件判断由表达式引擎驱动。根包 `xdag` 不直接依赖 expr，引擎放在子包 `xexpr` 中，通过 [`WithEvaluator()`](option.go) 注入：

```go
import (
    "github.com/xmapst/xdag"
    "github.com/xmapst/xdag/xexpr"
)

dag, err := xdag.New(tasks,
    xdag.WithEvaluator(xexpr.New()),
    xdag.WithVars(map[string]any{"env": "prod"}),
)
```

任务侧实现 [`Conditional`](evaluator.go)，为依赖边声明守卫：

```go
func (t *NotifyTask) Condition(dep string) string {
    if dep == "check" {
        return `Output("check").code == 200`
    }
    return "" // 空串表示该边无条件生效
}
```

### 为什么条件挂在边上

xdag 只有边条件一个分支判断入口——[`Conditional.Condition(dep)`](evaluator.go) 的签名带 `dep`，每条入边单独求值，没有「整个任务一个条件」的写法。

节点条件回答「这个任务该不该做」，边条件回答「这条依赖算不算数」。后者是更基础的原语：DAG 的调度语义本来就由边定义，把判断放回边上，门禁只需要一条规则，不存在「节点条件是否覆盖上游检查」这类需要额外约定的组合。分支、汇聚、可选依赖都由同一个机制表达。

代价是明确的，请留意：

- **没有依赖的根任务无法附加条件**，因为它没有入边。需要按开关控制入口时，在任务自身的 `Execute()` 中处理，或引入一个显式的前置任务。
- **节点级的业务谓词需要写在每条入边上**。例如「只在自动部署模式下执行 deploy」，若 deploy 依赖 `build` 和 `approve`，两条边都要带上这个条件；只写一条会导致另一条边仍然生效，任务照常执行。

### 门禁语义

任务被调度时，先过**入边门禁**，通过后才执行任务体：

- 声明了边条件 → 逐条求值。**失活**（求值为 `false`）的边不参与门禁；**生效**（求值为 `true` 或未声明）的边要求上游必须是 `StateSuccess`
- 未声明任何边条件 → 要求所有依赖成功（默认行为，与旧版本一致）
- 没有依赖的根任务没有入边，不受门禁约束

门禁的三种结果对应三种终态：

| 结果 | 终态 | 含义 |
| --- | --- | --- |
| 通过 | 继续执行 | 至少一条生效的边，且其上游都成功 |
| 所有入边失活 | `StateSkipped` | 没有任何依赖构成执行本任务的理由 |
| 生效的边上游未成功 | `StateUpstreamSkipped` | 依赖没跑成 |

被跳过时：

- 任务体不执行，[`Task.Execute()`](task.go:13) 一次都不会被调用
- 输出不会进入结果集
- **下游调度照常推进**，子节点入度正常递减

### 常用模式

求值时 `Env.Dep` 是该边的上游任务名，因此一条表达式可以复用到所有边上。

**条件分支**——同一个上游分流到两个互斥的下游：

```go
// notify-ok
func (t *NotifyOK) Condition(string) string { return `Output("check").code == 200` }
// rollback
func (t *Rollback) Condition(string) string { return `Output("check").code != 200` }
```

`check` 返回 500 时，`notify-ok` 唯一的入边失活，任务被标记 `StateSkipped`。

**OR 汇聚**——两条分支必有一条被跳过，汇聚节点不能要求全部成功：

```go
func (t *Report) Condition(string) string { return `Succeeded(Dep)` }
```

任一上游成功即可执行；全都没成功时所有边失活，`report` 被跳过。

**可选依赖**——某条依赖失败不应拦住本任务：

```go
func (t *Deploy) Condition(dep string) string {
    if dep == "notify" {
        return `Vars["strictNotify"] == true`
    }
    return "" // build 边无条件生效
}
```

`strictNotify` 为 `false` 时，即使 `notify` 失败，`deploy` 照常执行。

### 表达式环境

表达式在 [`Env`](evaluator.go) 上求值，可用的字段与函数如下：

| 名称 | 类型 | 说明 |
| --- | --- | --- |
| `Task` | `string` | 当前任务名 |
| `Dep` | `string` | 当前依赖边的上游任务名；边条件中恒非空，`RetryIf` 中为空 |
| `Attempt` | `int64` | 当前尝试次数，仅在 `RetryIf` 中非零 |
| `Error` | `string` | 最近一次失败的错误信息，仅在 `RetryIf` 中非空 |
| `Vars` | `map[string]any` | [`WithVars()`](option.go) 注入的全局变量 |
| `Inputs` | `map[string]map[string]any` | 直接依赖的输出，未成功的依赖不在其中 |
| `Output(name)` | `map[string]any` | 祖先任务的输出，未成功时为 `nil` |
| `State(name)` | `string` | 祖先任务状态：`success` / `skipped` / `upstream_skipped` / `failed` |
| `Succeeded(name)` | `bool` | 祖先任务是否成功 |
| `Skipped(name)` | `bool` | 祖先任务是否被跳过（含上游跳过） |
| `Failed(name)` | `bool` | 祖先任务是否失败 |

示例：

```text
Output("check").code == 200
Inputs["fetch-user"]["vip"] == true and Vars["env"] == "prod"
Succeeded(Dep)
```

### 可见范围限制在祖先集合

`Output()` / `State()` 等函数只能访问**当前任务的传递闭包祖先**，访问其他任务会报错。

这是刻意的约束：非祖先任务在本任务被调度时不保证已经完成，读取它的输出会让求值结果随调度时序漂移，同一张图每次跑出来的结论可能不同。

### 求值出错的处理

默认策略是 [`FailOnConditionError`](option.go)：求值出错视为任务失败。表达式写错和条件不成立是两回事，静默跳过会让问题极难定位。

需要宽松行为时可以切换：

```go
xdag.New(tasks,
    xdag.WithEvaluator(xexpr.New()),
    xdag.WithConditionErrorPolicy(xdag.SkipOnConditionError),
)
```

### 编译期校验

所有表达式在 [`New()`](dag.go) 阶段一次性编译完成。[`xexpr.New()`](xexpr/xexpr.go) 默认开启：

- `expr.Env(xdag.Env{})`：以 `Env` 结构体做编译期类型检查，拼错的字段名会被拒绝
- `expr.AsBool()`：强制表达式结果为 `bool`
- `expr.MaxNodes(1000)`：限制 AST 规模

因此语法错误、类型错误、未知标识符都会在构建 DAG 时暴露，不会跑到一半才失败。需要调整时可以把额外的 `expr.Option`（来自 `github.com/expr-lang/expr`）传给 [`xexpr.New()`](xexpr/xexpr.go)，它们追加在默认选项之后，可覆盖默认行为：

```go
import (
    "github.com/expr-lang/expr"        // 上游库，包名 expr
    "github.com/xmapst/xdag/xexpr"     // 本项目子包，包名 xexpr
)

// 收紧 AST 规模上限，并禁用不需要的内置函数
engine := xexpr.New(
    expr.MaxNodes(200),
    expr.DisableBuiltin("now"),
)
```

子包取名 `xexpr` 而非 `expr`，就是为了让它和上游库在同一个文件里无需别名即可共存。

> 注意：`Output()` 返回 `map[string]any`，取字段后是动态类型，`AsBool()` 在编译期无法判定，此类错误在求值时抛出。

### 静态引用校验

编译之后还会做一次**引用范围校验**，把越界的任务名引用从运行期错误提前到构建期：

| 写法 | 要求 |
| --- | --- |
| `Output("x")` / `State("x")` / `Succeeded("x")` / `Skipped("x")` / `Failed("x")` | `x` 必须是当前任务的祖先 |
| `Inputs["x"]` | `x` 必须是当前任务的**直接依赖**，否则运行期恒为 `nil` |

```text
task c: edge condition a -> c "Output(\"b\").x == 2" references "b", which is not an ancestor of c
task c: edge condition b -> c "Inputs[\"a\"][\"x\"] == 1" uses Inputs["a"], but "a" is not a direct dependency of c
```

该校验对边条件与 `RetryIf` 一视同仁。

实现方式是遍历编译后的 AST 收集字面量引用，能力通过可选接口 [`Referencer`](evaluator.go) 暴露：自定义的 `Evaluator` 不实现它也能正常工作，只是拿不到这层检查。

> 只有**字面量**参数能被静态判定。`Output(Dep)`、`Succeeded(Task)` 这类动态引用会跳过静态检查，由 `Env` 在运行期兜底。

### 完整示例

```go
tasks := map[string]xdag.Task{
    "check": &BranchTask{name: "check"},
    "notify-ok": &BranchTask{name: "notify-ok", deps: []string{"check"},
        edgeConds: map[string]string{"check": `Output("check").code == 200`}},
    "rollback": &BranchTask{name: "rollback", deps: []string{"check"},
        edgeConds: map[string]string{"check": `Output("check").code != 200`}},
    "report": &BranchTask{name: "report", deps: []string{"notify-ok", "rollback"},
        edgeConds: map[string]string{
            "notify-ok": `Succeeded(Dep)`,
            "rollback":  `Succeeded(Dep)`,
        }},
}

dag, _ := xdag.New(tasks, xdag.WithEvaluator(xexpr.New()))
_, _ = dag.Execute(context.Background())

for name, state := range dag.States() {
    fmt.Printf("%-10s %s\n", name, state)
}
// check      success
// notify-ok  skipped
// report     success
// rollback   success
```

可运行版本见 [`example_test.go`](example_test.go) 中的 `Example_conditionalBranch` 与 `Example_edgeCondition`。

---

## 重试机制说明

### 重试策略结构

[`RetryPolicy`](retry.go:10) 定义如下：

```go
type RetryPolicy struct {
    Interval    time.Duration
    MaxInterval time.Duration
    MaxAttempts int64
    Multiplier  float64
    RetryIf     string // 可选的重试条件表达式
}
```

### 默认策略

在 [`newRetryExecutor()`](retry.go:21) 中：

- 若策略为空，则默认只执行一次
- 默认重试间隔为 `1s`
- 默认最大间隔为 `30s`
- 默认倍率为 `2.0`
- [`MaxAttempts`](retry.go:13) 的语义为“总执行次数”而非“重试次数”
- 若 [`MaxAttempts`](retry.go:13) `<= 0`，则在 [`ExecuteWithRetry()`](retry.go:41) 中无限重试，直到成功或 [`context.Context`](task.go:11) 被取消
- 若 [`MaxAttempts`](retry.go:13) `= 1`，则只执行一次
- 若 [`MaxAttempts`](retry.go:13) `= 2`，则最多执行两次，以此类推

### 重试条件 RetryIf

默认行为是「只要失败就重试到次数用尽」，这会让永久性错误白白等满退避时间。[`RetryPolicy.RetryIf`](retry.go) 可以区分「可重试」与「不可重试」：

```go
func (t *DemoTask) RetryPolicy() *xdag.RetryPolicy {
    return &xdag.RetryPolicy{
        MaxAttempts: 5,
        Interval:    time.Second,
        RetryIf:     `Error matches "timeout|connection reset|5\\d\\d"`,
    }
}
```

求值时机与语义：

- 在每次失败后、**真正发起下一次尝试之前**求值
- 求值为 `false` → 立即放弃剩余尝试，返回最后一次业务错误
- 空串 → 无条件重试，与旧行为一致
- 已经是最后一次尝试时**不求值**，避免无谓开销和误导性的错误信息

表达式环境在分支条件的基础上多了两个字段：

- `Error`：最近一次失败的错误信息（任务返回的原始错误，未经调度器包装）
- `Attempt`：当前是第几次尝试，从 `1` 开始

```text
Error matches "timeout|connection reset"
not (Error contains "invalid argument")
Attempt < 3 or Vars["aggressive"] == true
```

> 正则里的转义要注意两层：expr 的双引号字符串会处理转义序列，`"5\d\d"` 中的 `\d` 必须写成 `\\d`。
> 上面示例的 Go 侧用的是反引号原始字符串，所以源码里就是 `5\\d\\d`。
> 从 YAML/JSON 读配置时可以改用 expr 自己的反引号原始字符串，写作 `` Error matches `5\d\d` ``，无需二次转义。

`RetryIf` 与分支条件共用同一套引擎，因此同样享受构建期编译与静态引用校验；同样需要通过 [`WithEvaluator()`](option.go) 配置引擎。

> 该表达式在 [`New()`](dag.go) 阶段读取并编译，运行期再修改 `RetryPolicy` 不会生效。

求值出错时遵循 [`WithConditionErrorPolicy()`](option.go)：默认返回条件错误与业务错误的聚合；`SkipOnConditionError` 则放弃重试并只返回业务错误，避免坏掉的条件掩盖真正的问题。

### 指数退避

退避时间由 [`calculateBackoff()`](retry.go:87) 计算，公式为：

```text
backoff = Interval * Multiplier^(attempt-1)
```

并且最终等待时间不会超过 `MaxInterval`。

### 执行过程

[`executeTask()`](dag.go) 内部通过 [`ExecuteWithRetry()`](retry.go:41) 包裹真实任务执行。每次尝试都会：

1. 生成当前 [`attempt`](task.go:11)
2. 调用 [`PreExecution()`](task.go:11)
3. 调用 [`Execute()`](task.go:13)
4. 调用 [`PostExecution()`](task.go:15)
5. 失败且仍有剩余次数时，先求值 `RetryIf`（若已配置），通过后再按退避策略等待并重试

---

## 结果与执行顺序

### 返回结果

[`Execute()`](dag.go) 返回：

```go
(map[string]map[string]any, error)
```

返回结果示例：

```go
map[string]map[string]any{
    "fetch-user": {
        "id":   1001,
        "name": "Tom",
    },
    "summary": {
        "ok": true,
    },
}
```

说明：

- 第一层 key 是任务名
- 第二层 map 是任务输出
- 只有成功执行的任务结果会进入最终结果集

结果集只包含成功的任务，无法区分「被跳过」与「失败」。需要完整视图时用 [`States()`](dag.go)：

```go
for name, state := range dag.States() {
    fmt.Printf("%-10s %s\n", name, state) // success / skipped / upstream_skipped / failed
}
```

单个任务用 [`State()`](dag.go)：

```go
if dag.State("rollback") == xdag.StateSkipped { /* ... */ }
```

### 执行顺序

[`ExecutionOrder()`](dag.go) 返回一个格式化字符串，用于展示**成功执行**的任务的完成顺序，例如：

```text
1. fetch-user
2. fetch-order
3. summary
```

需要注意：

- 这是**完成顺序**，不是静态拓扑排序
- 并发任务之间的完成顺序可能不稳定
- 多次执行时，只要存在并发，就可能出现不同顺序
- 被跳过与失败的任务不在其中，需要用 [`States()`](dag.go) 查看

### 错误聚合

在 [`Execute()`](dag.go) 中，多个任务错误会通过 [`errors.Join()`](dag.go) 聚合返回。

这意味着：

- 调用方可以一次性拿到多个失败任务的信息
- 当前实现不会因为某个任务失败就主动中止整个 DAG
- 失败任务的下游会被标记为 `StateUpstreamSkipped`，不会执行，但也不会让调度停摆
- 如果需要失败即停，可以由业务层配合 [`context.Context`](task.go:11) 实现取消逻辑

---

## 公开 API

### 构造与初始化

- [`New(tasks, opts...)`](dag.go)
  - 创建一个新的 [`Dagcuter`](dag.go)
  - 校验任务数量、依赖图，并预编译所有条件表达式

### 构建选项

- [`WithEvaluator()`](option.go)：注入表达式引擎
- [`WithVars()`](option.go)：注入全局变量
- [`WithConditionErrorPolicy()`](option.go)：条件求值出错时的处理策略
- [`WithMaxTasks()`](option.go)：覆盖任务数量上限，传 `0` 表示不限制

### 调度相关方法

- [`Execute()`](dag.go)：执行整个任务图；重复调用返回 `ErrAlreadyExecuted`
- [`States()`](dag.go)：返回所有任务的状态快照
- [`State(name)`](dag.go)：返回单个任务的状态
- [`ExecutionOrder()`](dag.go)：返回成功任务的完成顺序
- [`PrintGraph()`](dag.go)：输出依赖图

### 接口

- [`Task`](task.go:5)：任务抽象
- [`Conditional`](evaluator.go)：可选，声明依赖边条件（唯一的分支判断入口）
- [`Evaluator`](evaluator.go)：表达式引擎，把表达式编译成 [`Program`](evaluator.go)
- [`Program`](evaluator.go)：编译后的表达式，必须并发安全
- [`Referencer`](evaluator.go)：`Program` 的可选扩展，报告静态引用的任务名，供构建期范围校验

### 静态分析辅助

供自定义 `Evaluator` 实现 [`Referencer`](evaluator.go) 时使用：

- [`Reference`](evaluator.go) / [`RefKind`](evaluator.go)：一处任务名引用及其可见范围
- [`IsAncestorFunc()`](evaluator.go)：判断标识符是否为以任务名为参数的 `Env` 函数
- [`InputsField`](evaluator.go)：`Env` 中按依赖名索引的字段名

### 辅助函数与错误

- [`Validate()`](check.go)：校验依赖存在性与无环，返回带任务名的错误
- [`HasCycle()`](check.go)：仅检查环，已废弃，建议改用 `Validate()`
- `ErrCircularDependency` / `ErrUnknownDependency` / `ErrNoEvaluator` / `ErrAlreadyExecuted`

### 表达式引擎子包

- [`xexpr.New(opts...)`](xexpr/xexpr.go)：基于 `github.com/expr-lang/expr` 的 `Evaluator` 实现

---

## 项目结构

```text
.
├── README.md            # 项目说明文档
├── LICENSE              # 许可证
├── go.mod               # 模块定义
├── go.sum               # 依赖校验文件
├── dag.go               # DAG 调度器核心实现
├── task.go              # Task 接口定义
├── state.go             # 任务状态定义
├── evaluator.go         # 边条件的接口与求值环境
├── option.go            # 构建选项
├── retry.go             # 重试策略与执行器
├── check.go             # 图校验、环检测、祖先集合
└── xexpr/               # 基于 expr-lang/expr 的 Evaluator 实现
    └── xexpr.go
```

关键文件说明：

- [`dag.go`](dag.go)：定义 [`Dagcuter`](dag.go)、[`New()`](dag.go)、[`Execute()`](dag.go) 等核心逻辑
- [`task.go`](task.go)：定义任务抽象 [`Task`](task.go:5)
- [`state.go`](state.go)：定义任务终态 [`State`](state.go:4)
- [`evaluator.go`](evaluator.go)：定义 [`Conditional`](evaluator.go)、[`Evaluator`](evaluator.go)、[`Program`](evaluator.go) 与求值环境 [`Env`](evaluator.go)
- [`option.go`](option.go)：定义 [`Option`](option.go) 及各项构建选项
- [`retry.go`](retry.go)：定义 [`RetryPolicy`](retry.go:10) 与重试执行逻辑
- [`check.go`](check.go)：定义 [`Validate()`](check.go)、[`HasCycle()`](check.go) 与祖先集合计算
- [`xexpr/`](xexpr)：把 expr 依赖隔离在子包，根包保持零第三方依赖
- [`go.mod`](go.mod)：定义模块路径与依赖

---

## 许可证

许可证内容见 [`LICENSE`](LICENSE)。