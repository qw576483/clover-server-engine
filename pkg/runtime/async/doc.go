// Package async 提供通用的「异步任务执行器 / 工作池」原语。
//
// 支持命名、超时、按名查杀（Kill）、停止——用 Go 的 goroutine + context 取消 + 工作池表达，
// 并发安全、零外部依赖、可纯内存单测。
//
// 与 timer 的分工：timer 负责「一次性/周期/定时触发」（After/Every/Cron），本包负责
// 「提交一段要跑的工作并拿到结果 / 取消 / 重试」——二者互补，本包适合把阻塞或耗时工作
// （数据库、HTTP、离线计算）从请求主链路 offload 出去。
//
// 典型用法：
//
//	p := async.New(async.WithWorkers(8), async.WithQueueSize(1024))
//	p.Start()
//	defer p.Stop()
//
//	t, err := p.Submit("sync-1", func(ctx context.Context) error {
//	    return dumpToDB(ctx)
//	}, async.WithTimeout(5*time.Second))
//	if err != nil { return err }
//	if err := t.Wait(); err != nil { return err }
//
// 取消是协作式的：任务函数必须观察传入的 ctx 并在 ctx.Done() 时尽快返回，
// 否则超时 / Kill 只能等其自行结束。
package async
