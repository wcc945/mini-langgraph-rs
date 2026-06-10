# 实现带核心字段的同步 PregelLoop 骨架计划

## 涉及文件

- `docs/plan/implement_sync_pregel_loop_stub.md`
- `src/pregel/loops.rs`
- `src/pregel/mod.rs`
- `src/pregel/task.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

参考源项目 `PregelLoop` / `SyncPregelLoop`，新增 Rust 版同步 `PregelLoop` 骨架。当前只保留项目已具备承载能力的核心运行时字段，并将源项目的 `tasks: dict[str, PregelExecutableTask]` 映射为现有 `PregelTaskManager`。

本轮只提供 `tick`、`execute`、`after_tick` 三个空方法，不实现 superstep 调度、任务执行、channel 写入应用、checkpoint、interrupt、cache、retry、stream 输出或 async 行为。

## Key Changes

- 在 `src/pregel/loops.rs` 新增 `PregelLoop<StateT, UpdateT, ContextT>`。
- 新增 `PregelLoopStatus`，保留源项目 loop status 的同步运行时状态集合。
- `PregelLoop` 运行期间按字段借用 `Pregel` 容器中的 nodes、channels、managed、input/output/stream channels、stream mode、trigger 索引和 name，并持有输入、步数、停止步数、状态、`PregelTaskManager`、更新 channel 集合和输出。
- 将 `src/pregel/mod.rs` 中的 `loops` 暴露为 crate 内部模块。
- 将 `PregelTaskManager::new()` 从方法桩补为最小空 map 初始化，保证 `PregelLoop::new()` 可用。
- 同步更新运行时范围文档和 Rust 化改进文档，说明字段映射与暂不迁移范围。

## Test Plan

- `cargo fmt`
- `cargo check`
- `cargo test`
- `cargo clippy --all-targets --all-features`

## Assumptions

- 使用标准拼写 `execute`，不使用 `excute`。
- `PregelLoop::new` 接收 `&mut Pregel`，但结构体内部只保存拆分后的字段借用，贴近源项目向 `SyncPregelLoop` 传入 `nodes=self.nodes`、`specs=self.channels` 等字段的方式。
- `task_manager` 是源项目 `tasks` 字段在 Rust 版中的承载边界。
- 三个方法保持无逻辑；后续接入真实运行时语义时再调整方法签名或填充行为。
