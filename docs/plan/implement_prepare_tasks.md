# 实现 Pregel `prepare_task(s)` 计划

## 涉及文件

- `docs/plan/implement_prepare_tasks.md`
- `src/pregel/task.rs`
- `src/error.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

参照源项目 `prepare_single_task` / `prepare_next_tasks` 的 PULL 任务主线，实现 Rust mini 版 `PregelTaskManager::prepare_task` 和 `prepare_tasks`。

本轮只实现“根据可用 trigger channel 准备节点执行任务”的最小语义，不迁移 Python 源项目的 PUSH/Send、checkpoint 版本表、scratchpad、cache、retry、timeout、subgraph、interrupt/resume 和 Runnable config。

## Key Changes

- `PregelTaskManager::submit_task` 按 task id 写入内部任务表。
- `prepare_task` 只实现 PULL 节点任务：trigger channel 可用时读取普通 channel 输入，应用可选 mapper，并构造 `PregelExecutableTask`。
- `prepare_tasks` 根据 `updated_channels + trigger_to_nodes` 选择候选节点；没有增量信息时按节点名扫描全部节点。
- 任务输入统一以 `StateValue::Object` 组装；managed value 当前只识别为合法读取项，暂不注入输入。
- 任务 id 使用确定性字符串 `pull:{step}:{name}:{triggers}`，path 使用 `["pull", node_name]` 表达源项目 `(PULL, node_name)`。
- 本轮不把 `PregelLoop::tick` 接入真实调度，避免在 writes 应用与 channel consume 完成前改变 stream 行为。

## Test Plan

- `cargo fmt`
- `cargo test`
- `cargo clippy --all-targets --all-features`

## Result

已实现 `submit_task`、PULL 任务准备和按更新 channel 筛选候选节点的 `prepare_tasks`，并为 trigger、输入组装、mapper、managed 读取边界和未知读取 channel 增加单元测试。

## Assumptions

- `prepare_task(s)` 保持 `pub(crate)`，不作为公开 API。
- Rust mini 版暂不引入任务 id hash 依赖；确定性字符串 id 足够支撑当前调度骨架。
- managed value 输入读取需要先扩展 `ManagedValueSpec::get`，不纳入本轮。
- 本轮只迁移源项目 PULL 准备任务主线，不实现 PUSH/Send、checkpoint、scratchpad、cache/retry/timeout 或 interrupt/resume。
