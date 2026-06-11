# 实现 Pregel `values` / `updates` stream item 计划

## 涉及文件

- `src/pregel/loops.rs`
- `src/pregel/pregel.rs`
- `docs/plan/implement_pregel_stream_items.md`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## 摘要

补齐 mini Rust Pregel 运行时的 stream item 发送语义：`execute()` 在节点任务产生 writes 后发送 `updates`，`after_tick()` 在本轮 writes 应用到 channel 后发送 `values`。

本轮只覆盖当前已有的 `StreamMode::Values` 和 `StreamMode::Updates`，不迁移源项目的 debug、interrupt、messages、custom、checkpoint 或多 mode 列表。

## 实现要点

- 在 `PregelLoop` 内新增 helper，用于解析 stream/output channels、读取可用 channel 快照、把 task writes 映射为 updates payload。
- `StreamMode::Updates` 下，`execute()` 在 task writes 命中 stream/output channels 时发送 `PregelStreamItem { step, mode: Updates, data }`。
- `StreamMode::Values` 下，`after_tick()` 先应用 pending writes，再在 stream/output channels 有更新时发送当前 channel 快照。
- `PregelStreamItem.data` 继续使用 `StateValue`；节点更新分组和多 channel 快照通过 `StateValue::Object` / `StateValue::List` 表达。
- receiver 关闭视为正常停止条件，由 `mpsc::Sender::is_closed()` 反映，不包装成运行时错误。

## 测试计划

- 覆盖 `execute()` 发送和跳过 `updates` 的场景。
- 覆盖 `after_tick()` 发送和跳过 `values` 的场景。
- 覆盖 receiver drop 后 `is_stream_closed()` 的行为。
- 更新 `Pregel::stream` 集成测试，验证默认 `values` 模式和显式 `updates` 模式都会输出 item。
- 执行 `cargo fmt`、`cargo test`、`cargo clippy --all-targets --all-features`。

## 假设

- `updates` 只包含写入 stream/output channels 的节点更新，不暴露控制流 trigger channel。
- 不可用 channel 不进入 `values` 快照，避免尚未写入输出时把正常执行变成错误。
- `CompiledStateGraph` 保持当前 `stream_channels = output_channels` 的默认行为。
