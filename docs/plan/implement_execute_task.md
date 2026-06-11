# 实现 Pregel `execute_task` 计划

## 涉及文件

- `docs/plan/implement_execute_task.md`
- `src/pregel/task.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

实现 Rust mini 版 `PregelTaskManager::execute_task` 的单任务执行原语：先调用 task 保存的 `bound` 得到节点输出，再把节点输出按顺序交给 `writers` 的 `assemble`，把产生的 `(channel, StateValue)` 追加到 task 自身的 `writes` 中。

本轮不实现真正的批量并发调度，也不改 `PregelLoop::execute` 主循环。当前 `RuntimeContext<ContextT>` 以 `&mut` 传入，直接并发执行需要先定义 context 共享或复制语义，并扩大泛型约束。

## Key Changes

- `execute_task` 调用 `bound(&task.input, context)` 执行节点主逻辑。
- `NodeOutput::Update(update)` 转成 `StateValue` 后传给 writers；`NodeOutput::None` 转成 `StateValue::Null`，允许控制流 writer 继续执行。
- `NodeOutput::Command` / `Commands` 返回 `GraphError::UnsupportedPregelCommand`。
- 每个 writer 按原顺序调用 `assemble(&output, true, &task.input, context)`，组装结果追加到 `task.writes`，不直接更新 channel。
- bound 错误包装为 `GraphError::PregelTaskFailed`，writer 错误保持原样返回。

## Test Plan

- `cargo fmt`
- `cargo test`
- `cargo clippy --all-targets --all-features`

## Result

已实现 `execute_task` 单任务执行逻辑，并新增测试覆盖 writer 顺序、context/input 传递、保留既有 writes、`NodeOutput::None`、Command 暂不支持、bound 错误包装和 writer 错误传播。

## Assumptions

- 本轮采用单任务执行边界，不实现真正并发调度。
- `allow_passthrough` 使用 `true`，对齐源项目 `ChannelWrite.do_write` 默认允许 passthrough 的路径。
- writer 的 `state` 参数暂传入 `&task.input`；后续实现 `local_read(fresh=true)` 或更完整 runtime state reader 时再扩展。
