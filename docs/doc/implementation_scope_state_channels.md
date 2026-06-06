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

## 暂缓

- 完整 channel trait 生态。
- checkpoint 序列化所需的 channel 快照细节。
- `topic`、`delta`、`untracked_value` 等高级 channel。
- `LastValueAfterFinish`、`NamedBarrierValueAfterFinish` 等 defer 变体。