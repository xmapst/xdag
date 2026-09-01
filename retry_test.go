// retry_test.go —— 重试策略：次数、退避、抖动、不可重试错误。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// 重试
// ---------------------------------------------------------------------------

func TestRetryEventuallySucceeds(t *testing.T) {
	a := newTask("a").retries(5)
	a.fn = func(_ context.Context, attempt int64, _ map[string]any) (map[string]any, error) {
		if attempt < 3 {
			return nil, errors.New("not yet")
		}
		return map[string]any{"attempt": attempt}, nil
	}

	dag := mustNew(t, tasksOf(a))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if a.runs() != 3 {
		t.Errorf("a executed %d times, want 3", a.runs())
	}
	if results["a"]["attempt"] != int64(3) {
		t.Errorf("attempt = %v, want 3", results["a"]["attempt"])
	}
}

func TestRetryExhaustsAttempts(t *testing.T) {
	boom := errors.New("boom")
	a := newTask("a").retries(3).fails(boom)

	dag := mustNew(t, tasksOf(a))
	_, err := dag.Execute(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if !strings.Contains(err.Error(), "exhausted retries") {
		t.Errorf("unexpected error: %v", err)
	}
	// 文案里不该出现次数——那会和 TaskResult.Attempts 构成两个真相来源
	if strings.ContainsAny(err.Error(), "0123456789") {
		t.Errorf("错误文案里不该带次数，请读 TaskResult.Attempts: %v", err)
	}
	// 也不该重复拼接任务名
	if strings.Count(err.Error(), "task a") != 1 {
		t.Errorf("任务名被重复拼接: %v", err)
	}
	if got := dag.Results()["a"].Attempts; got != 3 {
		t.Errorf("TaskResult.Attempts = %d, want 3", got)
	}
	if a.runs() != 3 {
		t.Errorf("a executed %d times, want 3", a.runs())
	}
	assertState(t, dag, "a", xdag.StateFailed)
}

// MaxAttempts 的零值必须是「只执行一次」，而不是无限重试。
// &RetryPolicy{Interval: ...} 这种只想设间隔的写法不该永不放弃。
func TestZeroMaxAttemptsRunsOnce(t *testing.T) {
	a := newTask("a").fails(errors.New("boom"))
	a.policy = &xdag.RetryPolicy{Interval: time.Millisecond} // MaxAttempts 未填

	dag := mustNew(t, tasksOf(a))
	done := make(chan struct{})
	go func() {
		_, _ = dag.Execute(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("零值 MaxAttempts 仍在无限重试")
	}

	if a.runs() != 1 {
		t.Errorf("a executed %d times, want 1", a.runs())
	}
	assertState(t, dag, "a", xdag.StateFailed)
}

// 无限重试必须显式写出来，且能被 ctx 取消正常中止。
func TestInfiniteAttemptsIsExplicitAndCancelable(t *testing.T) {
	a := newTask("a").fails(errors.New("boom"))
	a.policy = &xdag.RetryPolicy{
		Interval:    time.Millisecond,
		MaxInterval: time.Millisecond,
		Multiplier:  1,
		MaxAttempts: xdag.InfiniteAttempts,
	}

	ctx, cancel := context.WithCancel(context.Background())
	dag := mustNew(t, tasksOf(a))

	done := make(chan struct{})
	go func() {
		_, _ = dag.Execute(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for a.runs() < 5 {
		select {
		case <-deadline:
			t.Fatalf("InfiniteAttempts 未持续重试，runs=%d", a.runs())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("无限重试未能被取消中止")
	}
	if a.runs() < 5 {
		t.Errorf("a executed %d times, want >= 5", a.runs())
	}
}

// sharedPolicyTask 的 RetryPolicy() 返回同一个共享指针，模拟用户的包级默认策略。
type sharedPolicyTask struct {
	*testTask
	shared *xdag.RetryPolicy
}

func (t *sharedPolicyTask) RetryPolicy() *xdag.RetryPolicy { return t.shared }

// 调度器不得改写调用方的 RetryPolicy：多个任务共享同一个策略对象时，
// 就地补默认值既是数据竞争，也会静默篡改用户配置。需要 -race 才能稳定暴露。
func TestSharedRetryPolicyIsNotMutated(t *testing.T) {
	shared := &xdag.RetryPolicy{MaxAttempts: 1}
	before := *shared

	m := make(map[string]xdag.ITask, 64)
	for i := range 64 {
		name := fmt.Sprintf("t%02d", i)
		m[name] = &sharedPolicyTask{testTask: newTask(name), shared: shared}
	}

	dag := mustNew(t, m)
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if *shared != before {
		t.Errorf("调用方的 RetryPolicy 被改写了: %+v, want %+v", *shared, before)
	}
}

// 退避间隔不得因浮点溢出变成负值而绕过 MaxInterval。
// Multiplier 极大时，第二次尝试的 backoff 就会超出 int64。
func TestBackoffDoesNotOverflowPastMaxInterval(t *testing.T) {
	a := newTask("a").fails(errors.New("boom"))
	a.policy = &xdag.RetryPolicy{
		Interval:    time.Millisecond,
		MaxInterval: 60 * time.Millisecond,
		MaxAttempts: 4,
		Multiplier:  1e13, // 第 2 次尝试起 backoff 就溢出 int64
	}

	dag := mustNew(t, tasksOf(a))
	start := time.Now()
	_, _ = dag.Execute(context.Background())
	elapsed := time.Since(start)

	// 第 1 次等待 1ms（尚未溢出），第 2、3 次 backoff 超出 int64 上限，
	// 必须各自被钳制到 MaxInterval，总耗时约 1+60+60=121ms。
	// 溢出未修复时退避绕回负数、time.After 立即触发，总耗时只有毫秒级。
	if elapsed < 100*time.Millisecond {
		t.Errorf("退避被绕过：4 次尝试仅耗时 %v，期望约 121ms", elapsed)
	}
	if a.runs() != 4 {
		t.Errorf("a executed %d times, want 4", a.runs())
	}
}

// Multiplier < 1 时 backoff 应随尝试次数单调收敛到 0，而不是在浮点下溢后
// 突然跳回 MaxInterval——那样会让一次快速收敛的重试序列变成几十秒的空等。
func TestBackoffUnderflowDoesNotJumpToMaxInterval(t *testing.T) {
	a := newTask("a").fails(errors.New("boom"))
	a.policy = &xdag.RetryPolicy{
		Interval:    5 * time.Millisecond,
		MaxInterval: 500 * time.Millisecond,
		MaxAttempts: 30,
		Multiplier:  0.001, // 几次尝试内就会在浮点下溢出到 0
	}

	dag := mustNew(t, tasksOf(a))
	start := time.Now()
	_, _ = dag.Execute(context.Background())
	elapsed := time.Since(start)

	// 修复前：一旦 backoff 下溢为 0，会被误判成溢出，退避跳回 500ms；
	// 30 次尝试里有大半会各自空等 500ms，总耗时轻松超过数秒。
	// 修复后：backoff 平滑收敛到 0，总耗时应停留在毫秒级。
	if elapsed > 300*time.Millisecond {
		t.Errorf("退避下溢后被错误地钳制到 MaxInterval：30 次尝试耗时 %v，期望远小于 300ms", elapsed)
	}
	if a.runs() != 30 {
		t.Errorf("a executed %d times, want 30", a.runs())
	}
}

// ---------------------------------------------------------------------------
// 退避与抖动（直接断言取值，不靠计时）
// ---------------------------------------------------------------------------

// backoffSamples 采样同一次尝试的退避时间，用于观察抖动分布。
func backoffSamples(t *testing.T, p xdag.RetryPolicy, attempt int64, n int) []time.Duration {
	t.Helper()
	r := xdag.NewRetryExecutorForTest(p)
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = r.CalculateBackoff(attempt, p.MaxInterval)
	}
	return out
}

// Jitter=0 时退避完全确定，等于指数退避公式的值。
func TestBackoffWithoutJitterIsDeterministic(t *testing.T) {
	p := xdag.RetryPolicy{Interval: 100 * time.Millisecond, Multiplier: 2, MaxInterval: time.Hour}
	want := []time.Duration{100, 200, 400, 800}
	for i, ms := range want {
		got := backoffSamples(t, p, int64(i+1), 3)
		for _, g := range got {
			if g != ms*time.Millisecond {
				t.Errorf("attempt %d 退避 = %v, want %v", i+1, g, ms*time.Millisecond)
			}
		}
	}
}

// 抖动**只向下**：任何一次取值都不得超过基准退避，否则 MaxInterval 与硬上限
// 就不再是真正的上限。这条断言是对称抖动改法的直接杀手。
func TestJitterOnlyShortensNeverLengthens(t *testing.T) {
	const base = 200 * time.Millisecond
	p := xdag.RetryPolicy{Interval: base, Multiplier: 1, MaxInterval: base, Jitter: 0.5}

	samples := backoffSamples(t, p, 1, 2000)
	lower := time.Duration(float64(base) * 0.5)
	var min, max time.Duration = base, 0
	for _, s := range samples {
		if s > base {
			t.Fatalf("退避 %v 超过了基准 %v —— 抖动必须只向下", s, base)
		}
		if s < lower {
			t.Fatalf("退避 %v 低于 Jitter=0.5 的下界 %v", s, lower)
		}
		if s < min {
			min = s
		}
		if s > max {
			max = s
		}
	}
	// 确认分布真的铺开了，而不是恒定值（抖动被整个短路掉也算 bug）
	if max-min < time.Duration(float64(base)*0.3) {
		t.Errorf("抖动分布过窄：min=%v max=%v，看起来没有真正随机", min, max)
	}
}

// 越界的 Jitter 必须被钳制到 [0,1]，包括 NaN 与 ±Inf。
// NaN 尤其危险：它与 0、1 的任何比较都是 false，不显式挡就会一路穿到
// float64→Duration 的转换，塌成负值，让退避被完全跳过。
func TestJitterOutOfRangeIsClamped(t *testing.T) {
	const base = 100 * time.Millisecond

	cases := []struct {
		name       string
		jitter     float64
		wantEffect float64
		wantExact  bool // true 表示应退化为「不抖动」，取值恒等于 base
	}{
		{"NaN", math.NaN(), 0, true},
		{"负无穷", math.Inf(-1), 0, true},
		{"负数", -1, 0, true},
		{"正无穷", math.Inf(1), 1, false},
		{"大于1", 5, 1, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := xdag.RetryPolicy{Interval: base, Multiplier: 1, MaxInterval: base, Jitter: c.jitter}
			r := xdag.NewRetryExecutorForTest(p)
			if got := r.EffectiveJitter(); got != c.wantEffect {
				t.Fatalf("生效的 Jitter = %v, want %v", got, c.wantEffect)
			}
			for _, s := range backoffSamples(t, p, 1, 500) {
				if s < 0 {
					t.Fatalf("退避为负：%v —— 越界的 Jitter 没有被钳制", s)
				}
				if s > base {
					t.Fatalf("退避 %v 超过基准 %v", s, base)
				}
				if c.wantExact && s != base {
					t.Fatalf("Jitter 应被钳成 0（不抖动），却得到 %v（基准 %v）", s, base)
				}
			}
		})
	}
}

// 抖动同样覆盖被钳到 MaxInterval 的情形——一批同时失败的任务最容易一起
// 顶在上限上，那里正是最需要打散的地方。
func TestJitterAppliesAtMaxInterval(t *testing.T) {
	const cap = 50 * time.Millisecond
	// Multiplier=10 保证第 5 次尝试早已远超 MaxInterval，必然被钳到 cap
	p := xdag.RetryPolicy{Interval: 10 * time.Millisecond, Multiplier: 10, MaxInterval: cap, Jitter: 0.8}

	samples := backoffSamples(t, p, 5, 500)
	var distinct int
	seen := map[time.Duration]bool{}
	for _, s := range samples {
		if s > cap {
			t.Fatalf("退避 %v 超过 MaxInterval %v", s, cap)
		}
		if !seen[s] {
			seen[s] = true
			distinct++
		}
	}
	if distinct < 10 {
		t.Errorf("顶在 MaxInterval 时抖动失效了：500 次采样只有 %d 个不同取值", distinct)
	}
}
