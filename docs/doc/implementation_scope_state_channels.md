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
- `BaseChannel` 当前包含 `value_type`、`update_type`、`copy`、`checkpoint`、`from_checkpoint`、`get`、`is_available`、`update`、`consume`、`finish` 等基础接口；`consume` 和 `finish` 默认返回 `Ok(false)`，对应源项目默认 no-op。
- 已为 `BaseChannel` 的默认行为补充测试：使用测试专用 channel 覆盖 `copy` 从 checkpoint 重建、`is_available` 基于 `get` 结果判断、`update` 写入最新值，以及默认 `consume` / `finish` no-op 语义。
- 为了让不同字段的 channel 能放入同一个 `HashMap`，当前引入动态值 `StateValue`，并定义 `DynChannel = dyn BaseChannel<Value = StateValue, Update = StateValue, Checkpoint = StateValue>`。
- `StateGraph` 当前的 channel 表使用 `channels: HashMap<String, Box<DynChannel>>`，对应源项目 `channels: dict[str, BaseChannel]` 的动态类型路线。
- `src/managed/mod.rs` 已新增最小 `ManagedValueSpec` trait，`StateGraph` 已包含 `managed: HashMap<String, Box<dyn ManagedValueSpec>>`，对应源项目 `managed: dict[str, ManagedValueSpec]`；字段名保存在 `HashMap` key 中，spec 表示 managed value 的计算规格。
- 已为 `ManagedValueSpec` 补充 marker trait 边界测试，确保测试实现满足 `Send + Sync` 约束。
- `GraphError` 已补充 `EmptyChannel`，用于后续 channel 读取空值时返回错误。
- 节点返回的局部更新暂由泛型 `UpdateT` 表达，框架还没有固定内置 `Update` 结构。
- `NodeOutput<UpdateT>` 把“状态更新”与“节点完整返回值”分开：`UpdateT` 只表示局部状态更新，`Command<UpdateT>` 可在后续携带 update 与控制流信息。
- 当前还没有实现将 `UpdateT` 拆分为字段级 writes 的 trait、动态 map、宏生成 partial update 或 channel 合并逻辑。

## 当前未完成

- `LastValue`、`BinaryOperatorAggregate`、`EphemeralValue` 和 barrier channel 尚未实现。
- 并发写入校验、reducer 聚合和非法 update 错误尚未实现。
- `StateValue` 目前只是最小动态值枚举，尚未和用户 `StateT` / `UpdateT` 建立转换协议。
- `ManagedValueSpec` 当前只是空 trait 边界，尚未实现源项目 `ManagedValue.get(scratchpad)` 的 scratchpad 读取语义。
- `BaseChannel::checkpoint` 目前要求具体 channel 实现，尚未提供等价源项目“默认调用 get，空 channel 返回 missing”的通用 sentinel 机制。
- `UpdateT` 与 `StateT` 的合并协议尚未确定；后续可在动态字段更新、手写强类型 update 或宏生成 update 之间选择。

## 暂缓

- 完整 channel trait 生态。
- checkpoint 序列化所需的 channel 快照细节。
- `topic`、`delta`、`untracked_value` 等高级 channel。
- `LastValueAfterFinish`、`NamedBarrierValueAfterFinish` 等 defer 变体。
