# 核心构图 API

## 目标

实现 `langgraph` 最小可用主线：用户先定义共享状态，再注册节点和边，最后编译成可执行图。

## 源项目参考

- `study/chapter_0/README.md`：最小心智模型与 `StateGraph -> compile -> invoke/stream` 链路。
- `study/chapter_2/README.md`：`StateGraph` 如何记录节点、普通边和条件边。
- `study/chapter_2_5/README.md`：`StateNodeSpec`、`BranchSpec` 与 `CompiledStateGraph`。
- `libs/langgraph/langgraph/graph/state.py`
- `libs/langgraph/langgraph/graph/_node.py`
- `libs/langgraph/langgraph/graph/_branch.py`

## 应实现

- `START` / `END` 虚拟节点常量。
- `StateGraph` builder，负责保存图定义，不直接执行节点。
- `add_node(name, func)`，先支持显式节点名和同步函数。
- `add_edge(from, to)`，支持固定控制流。
- `add_conditional_edges(source, route, path_map)`，支持路由函数返回 key 后映射到目标节点。
- `compile()`，完成基本校验并生成可执行图。
- 节点签名采用 Rust 习惯表达，但语义保持 `State -> Partial<State>`。

## 当前代码状态

- `src/graph/mod.rs` 已作为 graph 模块入口，声明 `state`、`node`、`branch`、`waiting_edge` 子模块，并导出 `StateGraph`、`WaitingEdgeSpec`、`START`、`END`。
- `src/graph/state.rs` 已开始定义 `StateGraph<StateT, UpdateT, ContextT, InputT, OutputT>` 骨架，当前保存 `nodes`、`edges`、`branches`、`waiting_edges`、`channels`、`managed`，并用 `PhantomData<(InputT, OutputT)>` 保留 input/output 类型占位。
- `StateGraph` 已实现 `new()` 和 `Default`，用于创建空 builder；暂不接收 Python 版 `state_schema/context_schema/input_schema/output_schema` 参数。
- `src/graph/node.rs` 已开始定义节点执行层骨架：`NodeFn<NodeInputT, UpdateT, ContextT>` 使用统一签名 `(&NodeInputT, &mut RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>`。
- `NodeOutput<UpdateT>` 当前设计为支持普通 `Update(UpdateT)`、`Command(Command<UpdateT>)`、多个 `Commands(Vec<Command<UpdateT>>)` 和 `None`。
- `StateNodeSpec<NodeInputT, UpdateT, ContextT>` 当前仅保存 `runnable`，暂不包含源项目中的 `metadata`、`input_schema`、`retry_policy`、`cache_policy`、`timeout` 等策略字段。
- `src/graph/waiting_edge.rs` 已新增 `WaitingEdgeSpec`，用于表示多起点 join 边；构造时会对 `starts` 排序并去重，使 `a,b -> c` 与 `b,a -> c` 归一为同一条 join 边。

## 当前未完成

- `StateGraph` 还没有 `add_node`、`add_edge`、`add_conditional_edges`、`compile` 等 builder 方法。
- `Command<UpdateT>` 当前只是返回值结构方向，`goto`、父图跳转、动态控制流等执行语义尚未实现。
- `BranchSpec` 当前已有 route 目标解析骨架，但还未接入 `add_conditional_edges`、Pregel 写入和运行时调度。
- `CompiledStateGraph` 和运行时调度结构尚未实现。
- `WaitingEdgeSpec` 目前只是结构定义，还未接入 `add_edge(list, end)`、barrier channel 或 Pregel 调度。

## 暂缓

- 自动从函数名推断节点名。
- 多种 Runnable 适配层。
- 节点级 `retry_policy`、`cache_policy`、`timeout`、`metadata`。
- `Command(goto=...)` 的完整执行语义与节点动态跳转目标展示。
