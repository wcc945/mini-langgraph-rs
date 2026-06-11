# 新增 PregelLoop enter/first 初始化阶段

## 涉及文件

- `docs/plan/implement_pregel_loop_enter_first.md`
- `src/pregel/loops.rs`
- `src/pregel/pregel.rs`
- `src/error.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

对齐源项目 `with SyncPregelLoop(...) as loop:` 的进入阶段：Rust 版保持 `PregelLoop::new` 只负责构造和复制本次运行态，新增 `enter` 在 `new` 之后显式调用，并由 `enter` 调用 `first` 完成 fresh input 的最小初始化。

本轮只迁移源项目 `_first` 的“初始输入写入 input channel 并记录 updated channels”主线，不实现 checkpoint、resume、Command、interrupt、cache、debug stream 或真实 task 调度。

## Key Changes

- 在 `PregelLoop` 增加 `enter(&mut self) -> Result<(), GraphError>`，调用 `first`，把返回值写入 `self.updated_channels`，并将状态切到 `PregelLoopStatus::Pending`。
- 在 `PregelLoop` 增加 `first(&mut self) -> Result<Option<HashSet<String>>, GraphError>`，把 `self.input` 映射为 input channel 更新并应用到本次 loop 的 `channels`。
- 输入映射规则对齐源项目 `map_input` 的最小语义：`None` 输入返回 `GraphError::EmptyPregelInput`；单 input channel 接收整个 `StateValue`；多 input channel 要求 `StateValue::Object`，只写入 key 属于 `input_channels` 的字段；没有任何映射写入时返回 `GraphError::EmptyPregelInput`。
- `first` 通过对应 channel 的 `update(vec![value])` 应用输入；`update` 返回 `true` 的 channel 加入 `updated_channels`。
- `Pregel::stream` 中构造 loop 后立即调用 `loop_state.enter()`；`new` 或 `enter` 失败都通过 stream sender 发送 `Err(GraphError)` 后结束后台任务。
- 保持 `tick`、`execute`、`after_tick` 和 `is_stream_closed` 的现有桩语义；本轮不准备任务、不执行节点、不发送 stream item。
- 同步更新运行时范围文档和 Rust 化差异文档，记录 `enter/first` 与源项目 `__enter__/_first` 的对应关系和暂不迁移范围。

## Test Plan

- 更新 `PregelLoop` 单元测试：`new` 后仍保持构造态；`enter` 会把单 input channel 写入 channel，`updated_channels` 包含该 channel，状态为 `Pending`；多 input channel 接收 `StateValue::Object` 时只写入匹配字段；`None` 输入和多 input channel 非 object 输入返回明确错误。
- 更新 `Pregel::stream` 测试：验证后台 task 会调用 `enter`，并在初始化失败时把错误发送给 receiver。
- 验证命令：`cargo fmt`、`cargo test`、`cargo clippy --all-targets --all-features`。

## Assumptions

- `enter` 使用 `&mut self` 而不是消费 `self`，便于 `Pregel::stream` 在 `new` 后显式调用并继续主循环。
- `first` 暂不写入 `pending_writes`，因为 Rust 版还没有 checkpoint pending writes 和 task replay 语义；初始输入直接应用到本次运行的 channel map。
- `first` 完成后状态设为 `Pending`，后续真实 `tick` 会基于 `updated_channels` 准备第一轮任务。
