# 实现 `PregelLoop::apply_writes`

本计划涉及改动文件：
- `src/pregel/loops.rs`
- `docs/plan/implement_pregel_loop_apply_writes.md`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

在 `src/pregel/loops.rs` 中新增 mini Rust 版 `PregelLoop::apply_writes`，参照源项目 `langgraph/pregel/_algo.py::apply_writes`，但只迁移当前项目已有运行时模型能表达的语义：任务排序、触发 channel 消费、按 channel 聚合同轮写入、空更新推进 step、finish 通知，以及返回可触发下一轮任务的 `updated_channels`。

不引入 checkpoint、channel version、pending writes 持久化、managed value 写入、reserved control channel 或完整 `after_tick` 主循环集成。

## Key Changes

- 在 `PregelLoop` 上新增 `apply_writes`。
- 按源项目规则将任务按 `task.path` 前 3 段稳定排序；同一 channel 内的 values 保持排序后 task 顺序和 task 内 writes 顺序。
- 若任一 task 有 triggers，则视为真实 superstep：先 `consume()` 已读 trigger channel，再聚合写入并调用 `update(values)`，随后对未更新但可用的 channel 调 `update(vec![])`，最后在没有后续可触发节点时调用 `finish()`。
- 未知写入 channel 参照源项目 warning/ignore 行为，在 mini 版中静默忽略。
- 新增 `PregelTaskWrites` 作为对齐源项目的轻量写入批次；`first(input_channels)` 显式接收本次输入 channel 集合，把 fresh input 映射成 input writes，构造无 triggers 的 `PregelTaskWrites` 并直接调用 `apply_writes`；`enter()` 传入 `self.input_channels`。仍暂不实现源项目的 discard tasks、checkpoint pending writes、resume/Command 分支。

## Tests

- 覆盖按 task path 排序后的 reducer 聚合顺序。
- 覆盖同 step 多写 `LastValue` 时传播 `MultipleUpdatesWithoutReducer`。
- 覆盖未知写入 channel 被忽略。
- 覆盖已读 trigger channel 的 `consume()` 行为。
- 覆盖真实 superstep 中未更新的可用 `EphemeralValue` 会被 `update(vec![])` 清理。
- 覆盖无 trigger 的 null/input-only 写入不执行 consume、空更新或 finish。
- 覆盖 `first()` 的 fresh input 写入走 `apply_writes` 聚合路径，而不是逐个 channel 直接更新。
- 覆盖 `first(input_channels)` 使用调用方传入的 input channel 集合，而不是隐式固定读取 `self.input_channels`。
- 覆盖无后续触发节点时会调用 `finish()`，且 finish 后可用 channel 会进入 `updated_channels`。

## Docs And Assumptions

- 更新 `docs/doc/implementation_scope_runtime.md`，说明 `PregelLoop::apply_writes` 已具备 Update 阶段核心原语，但 `after_tick`/完整主循环仍未接入。
- 更新 `docs/doc/rust_improvements_over_original.md`，记录 Rust 版暂不迁移 checkpoint/channel version/reserved control writes，并说明未知 channel 写入沿用源项目忽略策略。
- 假设本轮目标是实现 `apply_writes` 方法本身，不实现完整 `tick -> execute -> after_tick` 调度闭环。




