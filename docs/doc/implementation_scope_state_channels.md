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

- 节点返回的局部更新暂由泛型 `UpdateT` 表达，框架还没有固定内置 `Update` 结构。
- `NodeOutput<UpdateT>` 把“状态更新”与“节点完整返回值”分开：`UpdateT` 只表示局部状态更新，`Command<UpdateT>` 可在后续携带 update 与控制流信息。
- 当前还没有实现将 `UpdateT` 拆分为字段级 writes 的 trait、动态 map、宏生成 partial update 或 channel 合并逻辑。

## 当前未完成

- `LastValue`、`BinaryOperatorAggregate`、`EphemeralValue` 和 barrier channel 尚未实现。
- 并发写入校验、reducer 聚合和非法 update 错误尚未实现。
- `UpdateT` 与 `StateT` 的合并协议尚未确定；后续可在动态字段更新、手写强类型 update 或宏生成 update 之间选择。

## 暂缓

- 完整 channel trait 生态。
- checkpoint 序列化所需的 channel 快照细节。
- `topic`、`delta`、`untracked_value` 等高级 channel。
- `LastValueAfterFinish`、`NamedBarrierValueAfterFinish` 等 defer 变体。
