// iface_test.go —— 守住 IScheduler「恒等于 *Scheduler 导出方法全集」那条承诺。

package xdag_test

import (
	"reflect"
	"slices"
	"testing"

	"github.com/xmapst/xdag"
)

// TestISchedulerMirrorsScheduler 补上编译期断言抓不到的那一半。
//
// dag.go 的 `var _ IScheduler = (*Scheduler)(nil)` 只保证「接口里有的，
// Scheduler 都有」：删方法、改签名会当场编译失败。反方向它一声不吭——给
// Scheduler 加了导出方法却忘了写进接口，照样编译通过。而 IScheduler 的文档
// 承诺了两边恒等，没有这个测试，那条承诺就只是一句愿望：Phase/State/Progress
// 正是这么漏掉的，与接口同一个 commit 落地，漏了很久才被手工补上。
//
// 只比方法名：签名那一侧已经由编译期断言覆盖，签名不符根本编译不过。
// 两个 NumMethod 数的都只是导出方法——非接口类型的 reflect.Type.NumMethod
// 本就不计未导出方法，所以这里拿到的就是公开 API 的全集。
func TestISchedulerMirrorsScheduler(t *testing.T) {
	iface := reflect.TypeOf((*xdag.IScheduler)(nil)).Elem()
	declared := make([]string, 0, iface.NumMethod())
	for i := range iface.NumMethod() {
		declared = append(declared, iface.Method(i).Name)
	}

	impl := reflect.TypeOf((*xdag.Scheduler)(nil))
	exported := make([]string, 0, impl.NumMethod())
	for i := range impl.NumMethod() {
		exported = append(exported, impl.Method(i).Name)
	}

	slices.Sort(declared)
	slices.Sort(exported)

	for _, name := range exported {
		if !slices.Contains(declared, name) {
			t.Errorf("*Scheduler.%s 不在 IScheduler 里：新增导出方法要同步写进 "+
				"dag.go 的接口，否则那条「等于全集」的承诺就是假的", name)
		}
	}
	for _, name := range declared {
		if !slices.Contains(exported, name) {
			t.Errorf("IScheduler.%s 在 *Scheduler 上不存在（编译期断言本该先拦住它）", name)
		}
	}
}
