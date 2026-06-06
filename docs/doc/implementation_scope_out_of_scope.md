# 暂不实现范围

## 原则

本项目是 `langgraph` 的 mini Rust 实现，不追求复刻完整 Python 生态。只有当能力服务核心图执行语义时，才进入当前实现范围。

## 暂不实现

- LangGraph Platform、部署、CLI、远程服务和 SDK。
- LangSmith tracing、可观测性平台集成和云端调试能力。
- `prebuilt` agent、tool node、React agent、message transformer 等高级 agent 组件。
- LangChain 模型、prompt、tool、callback 的完整适配层。
- 多语言 SDK、HTTP/WebSocket streaming transport。
- sqlite/postgres checkpoint 后端。
- 长期 memory store、向量检索 store、加密 serde。
- 图可视化、UI、Studio、远程图管理。
- 完整 Python API 兼容层。

## 允许记录但不实现

如果源项目高级能力影响当前设计，可以在文档中记录其语义和取舍，但不要提前引入复杂抽象。实现前必须能说明它解决的核心问题、最小接口和测试方式。