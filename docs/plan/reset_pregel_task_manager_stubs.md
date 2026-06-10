# 清理 Pregel task 并重建任务管理器桩计划

## 涉及文件

- `docs/plan/reset_pregel_task_manager_stubs.md`
- `src/pregel/task.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

清空 `src/pregel/task.rs` 中旧的 task 中间层结构，改为一个参照 Python `PregelExecutableTask` 核心字段的 Rust 任务结构，以及一个仅保留方法签名的任务管理器。

当前只建立后续运行时调度会用到的结构边界，不实现任务提交、准备、执行、写入聚合、retry、cache、timeout、checkpoint 或 subgraph 行为。

## Key Changes

- 不再定义 `TaskId`、`TaskPath`、`TaskWrite` 类型别名，字段直接使用 `String`、`Vec<String>` 和 `Vec<(String, StateValue)>`。
- 新增 `PregelExecutableTask<StateT, UpdateT, ContextT>`，包含 `name`、`input`、`bound`、`writes`、`writers`、`triggers`、`id` 和 `path`。
- 新增 `PregelTaskManager<StateT, UpdateT, ContextT>`，内部用 `HashMap<String, PregelExecutableTask<StateT, UpdateT, ContextT>>` 按任务 id 暂存任务。
- 为任务管理器添加 `new`、`submit_task`、`prepare_tasks`、`prepare_task` 和 `execute_task` 方法桩，方法体暂为 `todo!()`。
- 暂不迁移 Python `PregelExecutableTask` 中的 `config`、`retry_policy`、`cache_key`、`subgraphs` 和 `timeout` 字段；任务结构直接保存可执行 `bound` 和节点 writers，不再通过 `proc: PregelNode` 间接承载。

## Test Plan

- `cargo fmt`
- `cargo check`
- `cargo test`
- `cargo clippy --all-targets --all-features`

## Result

已完成 `task.rs` 结构清理和任务管理器桩重建，并同步更新运行时范围文档与 Rust 化改进记录。

## Assumptions

- 新增类型保持 `pub(crate)`，不作为公开 API。
- `execute_task` 只接受任务结构体，符合后续运行时执行边界。
- 当前方法体统一使用 `todo!()`，后续实现 `stream()` 或调度循环时再填充行为。
