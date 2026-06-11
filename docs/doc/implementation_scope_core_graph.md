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
- `StateGraph` 已实现 `add_node`、`add_edge` 和 `add_conditional_edges` 的基础 builder API；节点名、边端点和条件分支名使用 `impl Into<String>` 风格的公共入参，内部统一收敛为 `String`。
- `add_edge` 当前通过 `IntoEdgeStarts` 接收单个起点或多个起点：单起点写入普通 `edges`，多起点写入 `waiting_edges`，并保留 `START` / `END` 的基本方向校验。
- `add_conditional_edges` 当前要求显式传入分支名，不尝试像 Python 版一样从函数对象推断名称。
- `StateGraph` 已实现 `add_sequence`、`set_entry_point`、`set_conditional_entry_point` 和 `set_finish_point` 的 MVP 版：它们仅作为已有 builder API 的薄封装，不引入自动节点名推断或 Python Runnable 适配。
- `StateGraph` 已实现 `validate` 的 MVP 版：校验所有边和条件分支的起点/终点是否存在，并要求图至少有一个从 `START` 出发的入口。
- `StateGraph` 已实现 `compile(self)` MVP：消费 builder，调用 `validate()`，并把用户节点、普通边 trigger 和条件分支 writer 编译成 `CompiledStateGraph` / `Pregel` 容器；当前只做结构转换，不执行节点。
- `CompiledStateGraph` 已作为编译后图的最小外壳，内部持有 `Pregel`；`compile()` 会先 `attach_node(START, None)` 创建订阅 `START` 的入口节点，`attach_node` 会为每个用户节点创建并订阅 `branch:to:{node}` trigger，`START -> node` 和普通 `node -> target` 都只给起点节点追加写入 `branch:to:{target}` 的 writer，`node -> END` 不生成 `END` 节点或 trigger。条件分支通过可执行 `ChannelWriter` 写入 `branch:to:{target}`，`END` 路由不会生成 trigger。
- 构图 API 和 `validate` 已统一返回 `GraphError`，避免使用散落的字符串错误；`GraphError` 当前包含重复节点、保留节点名、未知节点、非法边端点、重复分支、缺少入口和未知分支目标等构图错误。
- 已为核心构图模块补充单元测试，覆盖 `BranchSpec::resolve`、`WaitingEdgeSpec` 起点归一化、`StateNodeSpec` runnable 保存和执行、`StateGraph` builder 成功路径、错误路径与 `validate` 校验。
- `StateGraph` 已实现 `new()`，用于创建空 builder；暂不接收 Python 版 `state_schema/context_schema/input_schema/output_schema` 参数。
- `src/graph/node.rs` 已开始定义节点执行层骨架：`NodeFn<NodeInputT, UpdateT, ContextT>` 使用统一签名 `(&NodeInputT, &RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>`，其中 `RuntimeContext` 按运行依赖只读视图处理。
- `NodeOutput<UpdateT>` 当前设计为支持普通 `Update(UpdateT)`、`Command(Command<UpdateT>)`、多个 `Commands(Vec<Command<UpdateT>>)` 和 `None`。
- `StateNodeSpec<NodeInputT, UpdateT, ContextT>` 当前仅保存 `runnable`，暂不包含源项目中的 `metadata`、`input_schema`、`retry_policy`、`cache_policy`、`timeout` 等策略字段。
- `src/graph/waiting_edge.rs` 已新增 `WaitingEdgeSpec`，用于表示多起点 join 边；构造时会对 `starts` 排序并去重，使 `a,b -> c` 与 `b,a -> c` 归一为同一条 join 边。

## 当前未完成

- `Command<UpdateT>` 当前只是返回值结构方向，`goto`、父图跳转、动态控制流等执行语义尚未实现。
- `BranchSpec` 当前已有 route 目标解析骨架，并已接入 `add_conditional_edges` 的 builder 存储；`compile()` 已能把单目标条件分支接入 Pregel writer。运行时 superstep 调度尚未实现，因此条件边目前只完成编译装配和 writer 组装验证。
- `CompiledStateGraph` 当前还没有 `invoke`、`stream` 或运行时调度方法。
- `WaitingEdgeSpec` 已接入多起点 `add_edge` 的 builder 存储；`compile()` 当前会遍历 waiting edge，并通过 `CompiledStateGraph::attach_edge(starts, end)` 生成 `join:{starts}:{end}` barrier channel。

## 暂缓

- 自动从函数名推断节点名。
- 多种 Runnable 适配层。
- 节点级 `retry_policy`、`cache_policy`、`timeout`、`metadata`。
- `Command(goto=...)` 的完整执行语义与节点动态跳转目标展示。
