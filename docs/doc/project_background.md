# 项目背景

本项目是 `langgraph` 的 mini 实现，目标是在 Rust 中实现一个精简、可读、可测试的图执行框架。

原始项目可参考本地路径：`E:\codes\Python\langgraph`。

## 参考依据

本轮实现范围基于源项目中的 README、`study/chapter_0`、`study/chapter_2`、`study/chapter_2_5`、`study/chapter_3`、`study/chapter_4`、`study/chapter_5` 文档，以及 `libs/langgraph/langgraph` 下的核心源码结构判断。

源项目核心主线可以概括为：

```text
StateGraph builder
  -> nodes / edges / branches
  -> compile()
  -> CompiledStateGraph / Pregel
  -> invoke() / stream()
```

## 实现范围

实现范围拆分为以下子文档，后续涉及相关能力时应先阅读对应文档：

- [核心构图 API](implementation_scope_core_graph.md)
- [状态、channel 与 reducer](implementation_scope_state_channels.md)
- [同步 Pregel 运行时](implementation_scope_runtime.md)
- [可恢复执行与持久化取舍](implementation_scope_persistence.md)
- [暂不实现范围](implementation_scope_out_of_scope.md)

## 参考原则

- 参考 `langgraph` 的核心行为和命名，但 Rust 实现应保持符合 Rust 习惯。
- 优先保持代码简单直接，避免为了兼容完整 `langgraph` API 而引入过早抽象。
- 当原始项目行为复杂时，应提取最小可验证规则，并通过测试固定行为。
- 涉及图执行语义的变更，需要同步更新相关文档和测试。