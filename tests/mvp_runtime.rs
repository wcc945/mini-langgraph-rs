use std::collections::HashMap;

use mini_langgraph_rs::{
    GraphError, NodeFn, NodeOutput, RuntimeContext, START, StateGraph, StateValue, StreamMode,
};

fn update_value(value: impl Into<StateValue>) -> StateValue {
    StateValue::Object(HashMap::from([("value".to_string(), value.into())]))
}

fn graph_with_value_channel() -> StateGraph<StateValue, StateValue> {
    StateGraph::with_channels(["value"])
}

fn object_field<'a>(value: &'a StateValue, key: &str) -> Option<&'a StateValue> {
    match value {
        StateValue::Object(values) => values.get(key),
        _ => None,
    }
}

fn update_chunk(step: f64, data: StateValue) -> StateValue {
    StateValue::Object(HashMap::from([
        ("step".to_string(), StateValue::Number(step)),
        (
            "mode".to_string(),
            StateValue::String("updates".to_string()),
        ),
        ("data".to_string(), data),
    ]))
}

#[test]
fn invoke_runs_compiled_state_graph_and_returns_final_output() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("done")))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(output, StateValue::String("done".to_string()));
}

#[test]
fn invoke_returns_multiple_output_channels_as_object() {
    let mut graph: StateGraph<StateValue, StateValue> =
        StateGraph::with_channels(["left", "right"]);
    graph
        .add_node(
            "write",
            Box::new(|_, _| {
                Ok(NodeOutput::Update(StateValue::Object(HashMap::from([
                    ("left".to_string(), StateValue::String("L".to_string())),
                    ("right".to_string(), StateValue::Number(2.0)),
                ]))))
            }),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(
        output,
        StateValue::Object(HashMap::from([
            ("left".to_string(), StateValue::String("L".to_string())),
            ("right".to_string(), StateValue::Number(2.0)),
        ]))
    );
}

#[test]
fn add_sequence_runs_nodes_in_order_and_passes_state_forward() {
    let mut graph = graph_with_value_channel();
    let nodes: Vec<(&str, NodeFn<StateValue, StateValue, ()>)> = vec![
        (
            "first",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("first")))),
        ),
        (
            "second",
            Box::new(|state: &StateValue, _| {
                let Some(StateValue::String(value)) = object_field(state, "value") else {
                    return Err(GraphError::InvalidPregelInput("missing value".to_string()));
                };

                Ok(NodeOutput::Update(update_value(format!("{value}->second"))))
            }),
        ),
    ];
    graph.add_sequence(nodes).unwrap();
    graph.set_entry_point("first").unwrap();
    graph.set_finish_point("second").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(output, StateValue::String("first->second".to_string()));
}

#[tokio::test]
async fn default_stream_emits_values_items() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("value")))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let mut receiver = compiled
        .stream(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();
    let item = receiver.recv().await.unwrap().unwrap();

    assert_eq!(item.mode, StreamMode::Values);
    assert_eq!(item.data, StateValue::String("value".to_string()));
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn stream_values_emits_each_state_change_in_order() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "first",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("one")))),
        )
        .unwrap();
    graph
        .add_node(
            "second",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("two")))),
        )
        .unwrap();
    graph.set_entry_point("first").unwrap();
    graph.add_edge("first", "second").unwrap();
    graph.set_finish_point("second").unwrap();
    let compiled = graph.compile().unwrap();

    let mut receiver = compiled
        .stream(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();
    let first = receiver.recv().await.unwrap().unwrap();
    let second = receiver.recv().await.unwrap().unwrap();

    assert_eq!(first.mode, StreamMode::Values);
    assert_eq!(first.data, StateValue::String("one".to_string()));
    assert_eq!(second.mode, StreamMode::Values);
    assert_eq!(second.data, StateValue::String("two".to_string()));
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn stream_with_updates_emits_node_updates() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value(1_i64)))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();
    let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);

    let mut receiver = compiled.stream(Some(StateValue::Null), context).unwrap();
    let item = receiver.recv().await.unwrap().unwrap();

    assert_eq!(item.mode, StreamMode::Updates);
    assert_eq!(
        item.data,
        StateValue::Object(HashMap::from([(
            "write".to_string(),
            StateValue::Number(1.0)
        )]))
    );
}

#[test]
fn invoke_with_updates_stream_mode_returns_stream_chunks() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value(1_i64)))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();
    let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);

    let output = compiled.invoke(Some(StateValue::Null), context).unwrap();

    assert_eq!(
        output,
        StateValue::List(vec![update_chunk(
            1.0,
            StateValue::Object(HashMap::from([(
                "write".to_string(),
                StateValue::Number(1.0)
            )]))
        )])
    );
}

#[test]
fn invoke_with_updates_stream_mode_collects_multiple_chunks() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "first",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("one")))),
        )
        .unwrap();
    graph
        .add_node(
            "second",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("two")))),
        )
        .unwrap();
    graph.set_entry_point("first").unwrap();
    graph.add_edge("first", "second").unwrap();
    graph.set_finish_point("second").unwrap();
    let compiled = graph.compile().unwrap();
    let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);

    let output = compiled.invoke(Some(StateValue::Null), context).unwrap();

    assert_eq!(
        output,
        StateValue::List(vec![
            update_chunk(
                1.0,
                StateValue::Object(HashMap::from([(
                    "first".to_string(),
                    StateValue::String("one".to_string())
                )]))
            ),
            update_chunk(
                2.0,
                StateValue::Object(HashMap::from([(
                    "second".to_string(),
                    StateValue::String("two".to_string())
                )]))
            ),
        ])
    );
}

#[tokio::test]
async fn stream_uses_stream_mode_from_runtime_context() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("context-mode")))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();
    let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);

    let mut receiver = compiled.stream(Some(StateValue::Null), context).unwrap();
    let item = receiver.recv().await.unwrap().unwrap();

    assert_eq!(item.mode, StreamMode::Updates);
    assert_eq!(
        item.data,
        StateValue::Object(HashMap::from([(
            "write".to_string(),
            StateValue::String("context-mode".to_string())
        )]))
    );
}

#[test]
fn invoke_passes_runtime_context_to_nodes() {
    let mut graph: StateGraph<StateValue, StateValue, i64> = StateGraph::with_channels(["value"]);
    graph
        .add_node(
            "write",
            Box::new(|_, runtime| Ok(NodeOutput::Update(update_value(runtime.context)))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::new(7_i64))
        .unwrap();

    assert_eq!(output, StateValue::Number(7.0));
}

#[test]
fn conditional_edge_routes_to_selected_node() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph
        .add_node(
            "next",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("routed")))),
        )
        .unwrap();
    graph.set_entry_point("route").unwrap();
    graph
        .add_conditional_edges(
            "route",
            "choose",
            Box::new(|_, _| Some("next".to_string())),
            HashMap::from([("next".to_string(), "next".to_string())]),
        )
        .unwrap();
    graph.set_finish_point("next").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(output, StateValue::String("routed".to_string()));
}

#[test]
fn conditional_entry_point_routes_to_selected_node() {
    let mut graph: StateGraph<StateValue, StateValue, &'static str> =
        StateGraph::with_channels(["value"]);
    graph
        .add_node(
            "left",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("left")))),
        )
        .unwrap();
    graph
        .add_node(
            "right",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("right")))),
        )
        .unwrap();
    graph
        .set_conditional_entry_point(
            "entry",
            Box::new(|_, runtime: &RuntimeContext<&'static str>| Some(runtime.context.to_string())),
            HashMap::from([
                ("left".to_string(), "left".to_string()),
                ("right".to_string(), "right".to_string()),
            ]),
        )
        .unwrap();
    graph.set_finish_point("left").unwrap();
    graph.set_finish_point("right").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::new("right"))
        .unwrap();

    assert_eq!(output, StateValue::String("right".to_string()));
}

#[test]
fn conditional_edge_can_skip_all_targets() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph
        .add_conditional_edges(
            "route",
            "choose",
            Box::new(|_, _| None),
            HashMap::from([("unused".to_string(), "route".to_string())]),
        )
        .unwrap();
    graph.set_entry_point("route").unwrap();
    graph.set_finish_point("route").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(output, StateValue::Null);
}

#[test]
fn conditional_edge_reports_invalid_runtime_branch_key() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph
        .add_node(
            "next",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("next")))),
        )
        .unwrap();
    graph.set_entry_point("route").unwrap();
    graph
        .add_conditional_edges(
            "route",
            "choose",
            Box::new(|_, _| Some("missing".to_string())),
            HashMap::from([("next".to_string(), "next".to_string())]),
        )
        .unwrap();
    graph.set_finish_point("next").unwrap();
    let compiled = graph.compile().unwrap();

    let error = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap_err();

    assert!(matches!(error, GraphError::InvalidBranchTarget(target) if target == "missing"));
}

#[test]
fn waiting_edge_runs_after_all_start_nodes_finish() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("a", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph
        .add_node("b", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph
        .add_node(
            "join",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("joined")))),
        )
        .unwrap();
    graph.add_edge(START, "a").unwrap();
    graph.add_edge(START, "b").unwrap();
    graph.add_edge(["a", "b"], "join").unwrap();
    graph.set_finish_point("join").unwrap();
    let compiled = graph.compile().unwrap();

    let output = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(output, StateValue::String("joined".to_string()));
}

#[test]
fn repeated_invokes_use_isolated_runtime_state() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("fresh")))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let first = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();
    let second = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(first, StateValue::String("fresh".to_string()));
    assert_eq!(second, StateValue::String("fresh".to_string()));
}

#[tokio::test]
async fn repeated_streams_use_isolated_runtime_state() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("fresh")))),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let mut first = compiled
        .stream(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();
    let mut second = compiled
        .stream(Some(StateValue::Null), RuntimeContext::default())
        .unwrap();

    assert_eq!(
        first.recv().await.unwrap().unwrap().data,
        StateValue::String("fresh".to_string())
    );
    assert_eq!(
        second.recv().await.unwrap().unwrap().data,
        StateValue::String("fresh".to_string())
    );
}

#[test]
fn compile_rejects_missing_entrypoint() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "write",
            Box::new(|_, _| Ok(NodeOutput::Update(update_value("done")))),
        )
        .unwrap();

    let error = match graph.compile() {
        Err(error) => error,
        Ok(_) => panic!("compile should reject missing entrypoint"),
    };

    assert!(matches!(error, GraphError::MissingEntrypoint));
}

#[test]
fn compile_rejects_unknown_conditional_branch_target() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph.set_entry_point("route").unwrap();
    graph
        .add_conditional_edges(
            "route",
            "choose",
            Box::new(|_, _| Some("missing".to_string())),
            HashMap::from([("missing".to_string(), "missing".to_string())]),
        )
        .unwrap();

    let error = match graph.compile() {
        Err(error) => error,
        Ok(_) => panic!("compile should reject unknown branch target"),
    };

    assert!(matches!(
        error,
        GraphError::UnknownBranchTarget { node, branch, target }
            if node == "route" && branch == "choose" && target == "missing"
    ));
}

#[test]
fn builder_rejects_duplicate_and_reserved_nodes() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("write", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();

    let duplicate = match graph.add_node("write", Box::new(|_, _| Ok(NodeOutput::None))) {
        Err(error) => error,
        Ok(_) => panic!("duplicate node should be rejected"),
    };
    let reserved = match graph.add_node(START, Box::new(|_, _| Ok(NodeOutput::None))) {
        Err(error) => error,
        Ok(_) => panic!("reserved node should be rejected"),
    };

    assert!(matches!(duplicate, GraphError::DuplicateNode(node) if node == "write"));
    assert!(matches!(reserved, GraphError::ReservedNodeName(node) if node == START));
}

#[test]
fn node_error_is_wrapped_with_node_name() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "fail",
            Box::new(|_, _| Err(GraphError::InvalidPregelInput("bad".to_string()))),
        )
        .unwrap();
    graph.set_entry_point("fail").unwrap();
    graph.set_finish_point("fail").unwrap();
    let compiled = graph.compile().unwrap();

    let error = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap_err();

    assert!(matches!(
        error,
        GraphError::PregelTaskFailed { node, message }
            if node == "fail" && message == "invalid Pregel input: bad"
    ));
}

#[test]
fn node_update_must_be_state_object() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "bad_update",
            Box::new(|_, _| Ok(NodeOutput::Update(StateValue::String("bad".to_string())))),
        )
        .unwrap();
    graph.set_entry_point("bad_update").unwrap();
    graph.set_finish_point("bad_update").unwrap();
    let compiled = graph.compile().unwrap();

    let error = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap_err();

    assert!(matches!(
        error,
        GraphError::InvalidChannelUpdate(message)
            if message.contains("expected object update")
    ));
}

#[tokio::test]
async fn none_input_is_reported_as_empty_input() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node("write", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    let compiled = graph.compile().unwrap();

    let mut receiver = compiled.stream(None, RuntimeContext::default()).unwrap();
    let error = receiver.recv().await.unwrap().unwrap_err();

    assert!(matches!(
        error,
        GraphError::EmptyPregelInput(channels) if channels == vec![START.to_string()]
    ));
}

#[test]
fn command_outputs_are_reported_as_unsupported() {
    let mut graph = graph_with_value_channel();
    graph
        .add_node(
            "command",
            Box::new(|_, _| Ok(NodeOutput::<StateValue>::Commands(Vec::new()))),
        )
        .unwrap();
    graph.set_entry_point("command").unwrap();
    graph.set_finish_point("command").unwrap();
    let compiled = graph.compile().unwrap();

    let error = compiled
        .invoke(Some(StateValue::Null), RuntimeContext::default())
        .unwrap_err();

    assert!(matches!(error, GraphError::UnsupportedPregelCommand));
}
