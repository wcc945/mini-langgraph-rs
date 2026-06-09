# 状态、channel 与 reducer

## 目标

实现节点局部更新如何合并回共享状态，保证并发写入时行为明确、可测试。

## 源项目参考

- `study/chapter_3/README.md`：channel、reducer、运行时调度 channel 的核心说明。
- `libs/langgraph/langgraph/channels/base.py`
- `libs/langgraph/langgraph/channels/last_value.py`
- `libs/langgraph/langgraph/channels/binop.py`
- `libs/langgraph/langgraph/channels/ephemeral_value.py`
- `libs/langgraph/langgraph/channels/named_barrier_value.py`

## 应实现

- 共享状态以字段为单位更新，节点返回局部更新。
- 默认字段合并策略等价于 `LastValue`：同一轮没有写入则保持不变，单个写入覆盖，多写入应报错。
- 可配置 reducer 字段，语义参考 `BinaryOperatorAggregate`，用于合并同一轮多个写入。
- 内部调度信号，语义参考 `EphemeralValue`，避免旧触发信号重复触发节点。
- 多起点 join 的最小 barrier 语义：多个前置节点都完成后再触发目标节点。

## 当前代码状态

- `src/channel/mod.rs` 已开始迁移源项目 `BaseChannel[Value, Update, Checkpoint]`，当前用 Rust trait 和 associated types 表达 `Value`、`Update`、`Checkpoint`。
- `BaseChannel` 当前包含 `value_type`、`update_type`、`copy`、`checkpoint`、`from_checkpoint`、`get`、`is_available`、`update`、`consume`、`finish` 等基础接口；`consume` 和 `finish` 默认返回 `Ok(false)`，对应源项目默认 no-op。`from_checkpoint(&self, checkpoint)` 会接收原 channel 实例，用于保留 reducer、barrier 名称等配置后恢复 checkpoint。
- 已为 `BaseChannel` 的默认行为补充测试：使用测试专用 channel 覆盖 `copy` 从 checkpoint 重建、`is_available` 基于 `get` 结果判断、`update` 写入最新值，以及默认 `consume` / `finish` no-op 语义。
- 已实现 `LastValue`、`BinaryOperatorAggregate`、`EphemeralValue` 和 `NamedBarrierValue` 的 MVP 版，并补充单元测试覆盖空读、更新、checkpoint/copy、consume、reducer 错误和 barrier 非法值。
- 为了让不同字段的 channel 能放入同一个 `HashMap`，当前引入 crate 内部动态值 `StateValue`，并定义 `DynChannel = dyn BaseChannel<Value = StateValue, Update = StateValue, Checkpoint = StateValue>`。
- 已实现 `ChannelWriter` MVP（`src/channel/channel_writer.rs`），用于把节点输出 `StateValue` 组装成待追加到 task writes 的 `(channel, StateValue)` 项，不直接更新 channel 表。
- `ChannelWriter::assemble` 支持单 channel 的 `ChannelWriteEntry`、多 channel 的 `ChannelWriteTupleEntry` 和可执行 `ChannelExecutable`：单 channel entry 可处理固定值、passthrough 输出、mapper 转换、`SkipWrite` 和 `skip_none`；tuple entry 直接把节点返回值交给 mapper，再展开为多条 channel writes；executable entry 可读取当前 state 和 `RuntimeContext` 后返回动态 `ChannelWriteEntry`，用于表达条件分支 route writer。
- `ChannelWriter::state_value` 提供最小 `Into<StateValue>` 转换入口，并支持 `bool`、数字、字符串、列表和 `HashMap<String, T>` 等常见 Rust 值转为 `StateValue`；暂不把 `Option<T>` 自动转为 `StateValue`，避免混淆 `None = 不写字段` 与显式 `StateValue::Null`。
- `StateGraph` 当前的 channel 表使用 `channels: HashMap<String, Box<DynChannel>>`，对应源项目 `channels: dict[str, BaseChannel]` 的动态类型路线。
- `src/managed/mod.rs` 已新增最小 `ManagedValueSpec` trait，`StateGraph` 已包含 `managed: HashMap<String, Box<dyn ManagedValueSpec>>`，对应源项目 `managed: dict[str, ManagedValueSpec]`；字段名保存在 `HashMap` key 中，spec 表示 managed value 的计算规格。
- 已为 `ManagedValueSpec` 补充 marker trait 边界测试，确保测试实现满足 `Send + Sync` 约束。
- `src/graph/schema.rs` 已新增 crate 内部手写 `StateSchema` trait，作为源项目 `_add_schema(self.state_schema)` 的 Rust 版最小入口；`StateGraph::with_schema()` 会调用 `StateT::channels()` 和 `StateT::managed()` 自动填充 builder 的 state channel 与 managed value 表，`StateGraph::new()` 仍保持空 builder 语义。
- `GraphError` 已补充 `EmptyChannel`、多值写入、非法 channel update 和非法 barrier value 等错误，用于 channel 读取空值和更新失败时返回结构化错误。
- 节点返回的局部更新暂由泛型 `UpdateT` 表达，框架还没有固定内置 `Update` 结构。
- `NodeOutput<UpdateT>` 把“状态更新”与“节点完整返回值”分开：`UpdateT` 只表示局部状态更新，`Command<UpdateT>` 可在后续携带 update 与控制流信息。
- `ChannelWriter` 已接入普通边控制流 writer；状态字段写入协议尚未实现，真正的 channel 合并仍应由后续 runtime Update 阶段统一完成。

## 当前未完成

- `BinaryOperatorAggregate` 当前不实现 Python 版 `Overwrite` 特性。
- `StateValue` 目前只是最小动态值枚举，尚未和用户 `StateT` / `UpdateT` 建立转换协议。
- `ManagedValueSpec` 当前只是空 trait 边界，尚未实现源项目 `ManagedValue.get(scratchpad)` 的 scratchpad 读取语义。
- `StateSchema` 目前是 crate 内部能力，需要手写 channel / managed 表；尚未提供 `derive` 宏，也不从 Rust 结构体字段自动推断默认 `LastValue` channel。若后续要作为外部公共 API，需要同步公开 channel 和 managed 类型。
- `BaseChannel::checkpoint` 目前要求具体 channel 实现，尚未提供等价源项目“默认调用 get，空 channel 返回 missing”的通用 sentinel 机制。
- `UpdateT` 与 `StateT` 的合并协议尚未确定；后续可在动态字段更新、手写强类型 update 或宏生成 update 之间选择。

## 暂缓

- 完整 channel trait 生态。
- checkpoint 序列化所需的 channel 快照细节。
- `topic`、`delta`、`untracked_value` 等高级 channel。
- `LastValueAfterFinish`、`NamedBarrierValueAfterFinish` 等 defer 变体。
