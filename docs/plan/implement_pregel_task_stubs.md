# 实现 Pregel task 结构与方法桩计划

## 涉及文件

- `docs/plan/implement_pregel_task_stubs.md`
- `src/pregel/task.rs`
- `docs/doc/implementation_scope_runtime.md`

## Summary

本次只在 `task.rs` 建立同步 `stream()` 后续会依赖的最小 task 中间层：任务、任务写入、任务执行结果、step 批次。参照源项目的职责边界，但不照搬 `PregelExecutableTask` 的复杂字段，不实现 checkpoint、retry、cache、subgraph、debug、interrupt。

由于 Rust 的普通 `impl` 方法不能只有签名，本次方法统一采用真实签名加 `todo!()` 函数体，后续实现 `stream()` 时逐个填充。

## Key Changes

- 新增 `TaskId`、`TaskPath`、`TaskWrite` 类型别名。
- 新增 `PregelTask<StateT>`，表示已计划执行的节点任务。
- 新增 `PregelTaskWrites`，表示可统一交给 Update 阶段应用的写入批次。
- 新增 `PregelTaskResult<UpdateT>`，表示节点执行后的输出和 writer 组装后的 writes。
- 新增 `PregelStep<StateT>`，表示一个 superstep 中计划出的任务集合。
- 为上述类型添加最小方法桩，方法体暂为 `todo!()`。

## Test Plan

- `cargo check`
- `cargo test`
- `cargo clippy --all-targets --all-features`

## Result

已完成 `task.rs` 的最小 task 结构和方法桩：`PregelTask`、`PregelTaskWrites`、`PregelTaskResult`、`PregelStep` 及相关类型别名均已添加，方法体暂为 `todo!()`。已同步更新运行时范围文档，明确这些结构尚未接入 `Pregel` 主循环。

验证结果：`cargo check`、`cargo test`、`cargo clippy --all-targets --all-features` 均可通过；当前仓库仍保留既有骨架阶段 warning，新增 task 桩也会在未接入前产生 dead code warning。

## Assumptions

- 新增类型保持 `pub(crate)`，不作为公开 API。
- task 层只负责承载 stream/invoke 的中间数据，不负责真正读取 channel、执行节点或应用 writes。
- `Command`、`Commands`、checkpoint、interrupt、retry、cache、subgraph 和 debug stream 暂不进入 `task.rs` v1。
