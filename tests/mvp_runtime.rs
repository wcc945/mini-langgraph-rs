use std::collections::HashMap;

use mini_langgraph_rs::{GraphError, NodeOutput, START, StateGraph, StateValue, StreamMode};

fn update_value(value: impl Into<StateValue>) -> StateValue {
    StateValue::Object(HashMap::from([("value".to_string(), value.into())]))
}

fn graph_with_value_channel() -> StateGraph<StateValue, StateValue> {
    StateGraph::with_channels(["value"])
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

    let output = compiled.invoke(Some(StateValue::Null)).unwrap();

    assert_eq!(output, StateValue::String("done".to_string()));
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

    let mut receiver = compiled.stream(Some(StateValue::Null)).unwrap();
    let item = receiver.recv().await.unwrap().unwrap();

    assert_eq!(item.mode, StreamMode::Values);
    assert_eq!(item.data, StateValue::String("value".to_string()));
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

    let mut receiver = compiled
        .stream_with_mode(Some(StateValue::Null), StreamMode::Updates)
        .unwrap();
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

    let output = compiled.invoke(Some(StateValue::Null)).unwrap();

    assert_eq!(output, StateValue::String("routed".to_string()));
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

    let output = compiled.invoke(Some(StateValue::Null)).unwrap();

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

    let first = compiled.invoke(Some(StateValue::Null)).unwrap();
    let second = compiled.invoke(Some(StateValue::Null)).unwrap();

    assert_eq!(first, StateValue::String("fresh".to_string()));
    assert_eq!(second, StateValue::String("fresh".to_string()));
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

    let mut receiver = compiled.stream(None).unwrap();
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

    let error = compiled.invoke(Some(StateValue::Null)).unwrap_err();

    assert!(matches!(error, GraphError::UnsupportedPregelCommand));
}
