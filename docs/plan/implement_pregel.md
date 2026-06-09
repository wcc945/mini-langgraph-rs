# 实现 `Pregel` v1 计划

## Summary

本次只实现同步 Pregel 运行时的最小容器骨架：保存 nodes/channels、输入输出 channel 配置、stream 模式、步数限制，并提供基础 `validate()` 和触发索引构建。

不实现源项目中的 async、checkpoint、store、cache、retry、timeout、interrupt、debug event、schema/jsonschema、subgraph、stream transformer、bulk update 等能力。

## Key Changes

- 在 `src/pregel/pregel.rs` 定义 `StreamMode` 和 `Pregel<StateT, UpdateT, ContextT>`。
- 保留字段：`nodes`、`channels`、`managed`、`input_channels`、`output_channels`、`stream_channels`、`stream_mode`、`recursion_limit`、`trigger_to_nodes`、`name`。
- 构造函数 `new(...) -> Result<Self, GraphError>` 使用默认值：`stream_mode = Values`、`stream_channels = None`、`recursion_limit = 25`、`name = "LangGraph"`，并立即调用 `validate()`。
- 不提供配置 builder/helper 方法；后续由 `CompiledStateGraph` 接入时再决定配置入口。
- `validate()` 校验节点读取 channel、trigger channel、input/output/stream channel 是否存在，要求至少一个 input channel 被节点订阅，并重建 `trigger_to_nodes`。
- 暂不实现 `invoke`、`stream`、schema、checkpoint、state update、cache 等执行能力。

## Test Plan

- `new_validates_and_builds_trigger_index`
- `validate_rejects_unknown_read_channel`
- `validate_rejects_unknown_trigger_channel`
- `validate_rejects_input_channel_without_subscriber`
- `validate_rejects_unknown_output_or_stream_channel`
- `validate_rejects_zero_recursion_limit`

验证命令：`cargo fmt`、`cargo test`、`cargo check`、`cargo clippy --all-targets --all-features`。

## Assumptions

- `Pregel` 保持 `pub(crate)`，不作为公开 API。
- 所有 channel key 参数统一使用 `Vec<String>`，不复制 Python 的 `str | Sequence[str]` 双形态。
- `managed` 先只参与 validate，不实现 scratchpad 读取。
- 本次不接入 `CompiledStateGraph`，不执行节点。
