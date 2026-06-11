# 实现 `PregelLoop::execute`

## 涉及文件

- `src/runtime/mod.rs`
- `src/graph/node.rs`
- `src/graph/branch.rs`
- `src/channel/channel_writer.rs`
- `src/pregel/node.rs`
- `src/pregel/task.rs`
- `src/pregel/loops.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`
- `docs/plan/implement_pregel_loop_execute.md`

## 目标

实现 `PregelLoop::execute(&mut self) -> Result<(), GraphError>`，对齐源项目 `PregelRunner.tick(...)` 的核心执行语义：同一 superstep 中已准备好的 tasks 可以并发执行，每个 task 只产生 pending writes，channel 更新仍延迟到后续 Update 阶段。

运行依赖按源项目约定视为只读：`RuntimeContext<ContextT>` 通过共享引用传给节点、branch 和 writer。图状态变化必须通过节点输出和 channel writes 表达。

## 实施步骤

1. 将节点、branch 和 writer 签名从 `&mut RuntimeContext<ContextT>` 调整为 `&RuntimeContext<ContextT>`。
   - 验证：相关单元测试只读取 context，不再依赖 runtime 隐式可变共享。
2. 在 `PregelTaskManager` 增加 `execute_pending_tasks`。
   - 验证：空任务返回空 writes；多任务按稳定顺序返回 `PregelTaskWrites`。
3. 在 `PregelLoop` 持有只读运行上下文并实现 `execute`。
   - 验证：`execute` 执行已准备任务并填充 `pending_writes`，不修改 channel。
4. 同步运行时文档和 Rust 化取舍文档。
   - 验证：文档说明 `execute` 已完成的范围，以及 retry、timeout、cache、error handler 等仍暂缓。

## 不实现范围

- 不实现 `tick` 的真实任务准备闭环。
- 不实现 `after_tick` 的 writes 应用和 stream 输出。
- 不迁移源项目的 retry、timeout、cache、error handler、waiter、PUSH/Send task、checkpoint 或 interrupt 语义。
- 不为 `ContextT` 提供隐式锁；需要可变共享资源时由调用方显式放入线程安全类型。
