# xdag

[`xdag`](README.md) 是一个基于 Go 实现的轻量级 DAG（Directed Acyclic Graph，有向无环图）任务编排库，用于组织具有依赖关系的任务，并按依赖顺序自动并发执行。

实现的核心特点是：

- 基于 [`Task`](task.go:5) 抽象任务节点
- 基于 [`HasCycle()`](check.go:5) 在初始化时进行环检测
- 基于 [`Dagcuter`](dag.go:13) 统一管理任务图、执行顺序和结果汇总
- 基于 [`Execute()`](dag.go:49) 启动 DAG 执行
- 基于 [`RetryPolicy`](retry.go:10) 与 [`ExecuteWithRetry()`](retry.go:41) 提供失败重试能力

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

主调度器实现位于 [`Dagcuter`](dag.go:13)，其职责包括：

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

在调用 [`New()`](dag.go:23) 创建执行器时，内部会调用 [`HasCycle()`](check.go:5) 检查是否存在循环依赖。如果存在环，初始化立即失败。

### 3. 自动并发执行

所有入度为 `0` 的根任务会首先启动。后续任务在依赖全部完成后，会由 [`runTask()`](dag.go:76) 递归触发新的 goroutine 执行。

### 4. 任务级重试

任务执行失败后，可通过 [`Task.RetryPolicy()`](task.go:9) 返回的 [`RetryPolicy`](retry.go:10) 指定最大尝试次数、退避间隔与倍率。

### 5. 生命周期钩子

任务在执行前后分别会调用：

- [`Task.PreExecution()`](task.go:11)
- [`Task.PostExecution()`](task.go:15)

这使得你可以很方便地注入日志、监控、埋点、审计和失败记录逻辑。

### 6. 统一结果汇总

所有成功执行的任务输出都会被汇总到 [`Execute()`](dag.go:49) 的返回值中，便于上层调用方统一消费。

### 7. 可追踪执行顺序

调用 [`ExecutionOrder()`](dag.go:149) 可获取任务的**实际完成顺序**，适用于调试和观测执行过程。

### 8. 依赖图输出

调用 [`PrintGraph()`](dag.go:159) 可打印从根节点开始的依赖链路，便于理解整个 DAG 结构。

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

下面示例展示如何定义三个任务：

- `fetch-user`
- `fetch-order`
- `summary`

其中 `summary` 依赖前两个任务。

```go
package main

import (
    "context"
    "fmt"
    "time"

    xdag "github.com/xmapst/xdag"
)

type DemoTask struct {
    name string
    deps []string
}

func (t *DemoTask) Name() string {
    return t.name
}

func (t *DemoTask) Dependencies() []string {
    return t.deps
}

func (t *DemoTask) RetryPolicy() *xdag.RetryPolicy {
    return &xdag.RetryPolicy{
        Interval:    time.Second,
        MaxInterval: 5 * time.Second,
        MaxAttempts: 3,
        Multiplier:  2,
    }
}

func (t *DemoTask) PreExecution(ctx context.Context, input map[string]any) {
    fmt.Printf("[pre] %s input=%v\n", t.name, input)
}

func (t *DemoTask) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
    switch t.name {
    case "fetch-user":
        return map[string]any{
            "id":   1001,
            "name": "Tom",
        }, nil
    case "fetch-order":
        return map[string]any{
            "order_no": "ORD-001",
            "amount":   199,
        }, nil
    case "summary":
        return map[string]any{
            "user":  input["fetch-user"],
            "order": input["fetch-order"],
            "ok":    true,
        }, nil
    default:
        return map[string]any{"task": t.name}, nil
    }
}

func (t *DemoTask) PostExecution(ctx context.Context, output map[string]any, err error) {
    fmt.Printf("[post] %s output=%v err=%v\n", t.name, output, err)
}

func main() {
    tasks := map[string]xdag.Task{
        "fetch-user":  &DemoTask{name: "fetch-user"},
        "fetch-order": &DemoTask{name: "fetch-order"},
        "summary":     &DemoTask{name: "summary", deps: []string{"fetch-user", "fetch-order"}},
    }

    dag, err := xdag.New(tasks)
    if err != nil {
        panic(err)
    }

    results, err := dag.Execute(context.Background())
    if err != nil {
        panic(err)
    }

    fmt.Printf("results=%#v\n", results)
    fmt.Println(dag.ExecutionOrder())
}
```

### 执行预期

- `fetch-user` 与 `fetch-order` 无依赖，可并发执行
- `summary` 依赖前两者，因此会在它们完成后启动
- 最终结果由 [`Execute()`](dag.go:49) 返回，类型为 `map[string]map[string]any`

---

## 核心概念

### 1. 任务接口

所有任务都必须实现 [`Task`](task.go:5)：

```go
type Task interface {
    Name() string
    Dependencies() []string
    RetryPolicy() *RetryPolicy
    PreExecution(ctx context.Context, input map[string]any)
    Execute(ctx context.Context, input map[string]any) (map[string]any, error)
    PostExecution(ctx context.Context, output map[string]any, err error)
}
```

各方法职责如下：

- [`Name()`](task.go:6)：返回任务名称
- [`Dependencies()`](task.go:7)：返回依赖任务名列表
- [`RetryPolicy()`](task.go:9)：返回任务级重试策略
- [`PreExecution()`](task.go:11)：执行前回调
- [`Execute()`](task.go:13)：任务主体逻辑
- [`PostExecution()`](task.go:15)：执行后回调

### 2. 输入参数约定

在 [`prepareInputs()`](dag.go:139) 中，调度器会将所有依赖任务的输出收集到下游任务的输入参数中。

例如：

- 若任务 `C` 依赖 `A` 和 `B`
- 则 `C` 的 [`Execute()`](task.go:13) 输入中会包含：
  - `input["A"]`
  - `input["B"]`

此外，在 [`executeTask()`](dag.go:109) 中，还会注入：

```go
inputs["attempt"] = n
```

这表示当前是第几次尝试执行该任务。

### 3. 结果缓存

成功执行后的结果会被写入 [`d.results.Store()`](dag.go:98)，后续依赖任务即可通过输入参数访问上游结果。

### 4. 入度与依赖关系

在 [`New()`](dag.go:23) 初始化期间，调度器会建立：

- `inDegrees`：每个任务尚未满足的依赖数量
- `dependents`：每个任务被哪些下游任务依赖

这两个结构共同决定任务何时可以被调度执行。

---

## 执行机制说明

### 初始化阶段

创建 [`Dagcuter`](dag.go:13) 的入口是 [`New()`](dag.go:23)。初始化时主要会做以下事情：

1. 检查任务数量是否超过 [`MaxWorkers`](dag.go:11)
2. 检查依赖图是否有环，调用 [`HasCycle()`](check.go:5)
3. 初始化结果集、入度表、反向依赖表
4. 遍历所有任务，构建 `inDegrees` 和 `dependents`

其中：

```go
var MaxWorkers = 150
```

这里的 [`MaxWorkers`](dag.go:11) 默认最大任务数量限制

### 启动执行

[`Execute()`](dag.go:49) 的核心过程：

1. 创建错误通道 `errCh`
2. 遍历所有任务，找到入度为 `0` 的任务
3. 为这些根任务启动 goroutine 执行 [`runTask()`](dag.go:76)
4. 等待所有任务完成
5. 聚合所有错误并返回结果

### 单任务执行流程

[`runTask()`](dag.go:76) 的逻辑可概括为：

1. 读取任务对象
2. 调用 [`prepareInputs()`](dag.go:139) 准备输入参数
3. 调用 [`executeTask()`](dag.go:109) 运行任务
4. 若成功，则记录执行顺序并保存输出
5. 遍历其所有子节点，将子节点入度减一
6. 若某个子节点入度减为 `0`，则启动新的 goroutine 执行它

### 实际并发方式

直接通过：

```go
go d.runTask(ctx, child, errCh)
```

递归派生新的 goroutine。也就是说，当前并发度主要受任务图结构、就绪任务数量以及外部运行环境影响。

---

## 重试机制说明

### 重试策略结构

[`RetryPolicy`](retry.go:10) 定义如下：

```go
type RetryPolicy struct {
    Interval    time.Duration
    MaxInterval time.Duration
    MaxAttempts int
    Multiplier  float64
}
```

### 默认策略

在 [`newRetryExecutor()`](retry.go:21) 中：

- 若策略为空，则默认不重试
- 默认重试间隔为 `1s`
- 默认最大间隔为 `30s`
- 默认倍率为 `2.0`
- 若 `MaxAttempts <= 0`，在 [`ExecuteWithRetry()`](retry.go:42) 中会直接执行一次

### 指数退避

退避时间由 [`calculateBackoff()`](retry.go:87) 计算，公式为：

```text
backoff = Interval * Multiplier^(attempt-1)
```

并且最终等待时间不会超过 `MaxInterval`。

### 执行过程

[`executeTask()`](dag.go:109) 内部通过 [`ExecuteWithRetry()`](retry.go:41) 包裹真实任务执行。每次尝试都会：

1. 写入 `inputs["attempt"]`
2. 调用 [`PreExecution()`](task.go:11)
3. 调用 [`Execute()`](task.go:13)
4. 调用 [`PostExecution()`](task.go:15)
5. 失败则根据策略等待后重试

---

## 结果与执行顺序

### 返回结果

[`Execute()`](dag.go:49) 返回：

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

### 执行顺序

[`ExecutionOrder()`](dag.go:149) 返回一个格式化字符串，用于展示任务完成顺序，例如：

```text
1. fetch-user
2. fetch-order
3. summary
```

需要注意：

- 这是**完成顺序**，不是静态拓扑排序
- 并发任务之间的完成顺序可能不稳定
- 多次执行时，只要存在并发，就可能出现不同顺序

### 错误聚合

在 [`Execute()`](dag.go:65) 中，多个任务错误会通过 [`errors.Join()`](dag.go:65) 聚合返回。

这意味着：

- 调用方可以一次性拿到多个失败任务的信息
- 当前实现不会因为某个任务失败就主动中止整个 DAG
- 如果需要失败即停，可以由业务层配合 [`context.Context`](task.go:11) 实现取消逻辑

---

## 公开 API

### 构造与初始化

- [`New()`](dag.go:23)
  - 创建一个新的 [`Dagcuter`](dag.go:13)
  - 同时进行任务数量校验与环检测

### 调度相关方法

- [`Execute()`](dag.go:49)
  - 执行整个任务图
- [`ExecutionOrder()`](dag.go:149)
  - 返回任务完成顺序
- [`PrintGraph()`](dag.go:159)
  - 输出依赖图

### 内部关键方法

虽然以下方法通常由内部调度使用，但理解它们有助于阅读源码：

- [`runTask()`](dag.go:76)
- [`executeTask()`](dag.go:109)
- [`prepareInputs()`](dag.go:139)
- [`printChain()`](dag.go:178)
- [`newRetryExecutor()`](retry.go:21)
- [`calculateBackoff()`](retry.go:87)

### 辅助函数

- [`HasCycle()`](check.go:5)
  - 检查任务图是否包含循环依赖

---

## 项目结构

```text
.
├── README.md   # 项目说明文档
├── LICENSE     # 许可证
├── go.mod      # 模块定义
├── go.sum      # 依赖校验文件
├── dag.go      # DAG 调度器核心实现
├── task.go     # Task 接口定义
├── retry.go    # 重试策略与执行器
└── check.go    # 环检测实现
```

关键文件说明：

- [`dag.go`](dag.go) ：定义 [`Dagcuter`](dag.go:13)、[`New()`](dag.go:23)、[`Execute()`](dag.go:49) 等核心逻辑
- [`task.go`](task.go) ：定义任务抽象 [`Task`](task.go:5)
- [`retry.go`](retry.go) ：定义 [`RetryPolicy`](retry.go:10) 与重试执行逻辑
- [`check.go`](check.go) ：定义环检测函数 [`HasCycle()`](check.go:5)
- [`go.mod`](go.mod) ：定义模块路径与依赖

---

## 许可证

许可证内容见 [`LICENSE`](LICENSE)。