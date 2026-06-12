use criterion::{BenchmarkId, Criterion, black_box};
use mini_langgraph_rs::checkpoint::MemorySaver;
use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Graph builders
// ---------------------------------------------------------------------------

fn build_single_node_graph() -> mini_langgraph_rs::graph::CompiledStateGraph<StateValue, StateValue>
{
    let mut graph: StateGraph<StateValue, StateValue> = StateGraph::with_channels(["value"]);
    graph
        .add_node(
            "write",
            Box::new(|_, _| {
                Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                    "value".to_string(),
                    StateValue::String("done".to_string()),
                )]))))
            }),
        )
        .unwrap();
    graph.set_entry_point("write").unwrap();
    graph.set_finish_point("write").unwrap();
    graph.compile().unwrap()
}

fn build_linear_chain(
    n: usize,
) -> mini_langgraph_rs::graph::CompiledStateGraph<StateValue, StateValue> {
    let mut graph: StateGraph<StateValue, StateValue> = StateGraph::with_channels(["value"]);
    let names: Vec<String> = (0..n).map(|i| format!("n{}", i)).collect();

    for (i, name) in names.iter().enumerate() {
        let idx = i;
        graph
            .add_node(
                name.clone(),
                Box::new(move |_, _| {
                    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                        "value".to_string(),
                        StateValue::Number(idx as f64),
                    )]))))
                }),
            )
            .unwrap();
    }

    graph.set_entry_point(names[0].as_str()).unwrap();
    for w in names.windows(2) {
        graph.add_edge(w[0].as_str(), w[1].as_str()).unwrap();
    }
    graph.set_finish_point(names[n - 1].as_str()).unwrap();
    graph.compile().unwrap()
}

fn build_conditional_graph() -> mini_langgraph_rs::graph::CompiledStateGraph<StateValue, StateValue>
{
    let mut graph: StateGraph<StateValue, StateValue> = StateGraph::with_channels(["value"]);

    graph
        .add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))
        .unwrap();

    for &name in &["a", "b", "c"] {
        let s = name.to_string();
        graph
            .add_node(
                name,
                Box::new(move |_, _| {
                    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                        "value".to_string(),
                        StateValue::String(s.clone()),
                    )]))))
                }),
            )
            .unwrap();
    }

    graph.set_entry_point("route").unwrap();
    graph
        .add_conditional_edges(
            "route",
            "pick",
            Box::new(|_, _| Some("b".to_string())),
            HashMap::from([
                ("a".to_string(), "a".to_string()),
                ("b".to_string(), "b".to_string()),
                ("c".to_string(), "c".to_string()),
            ]),
        )
        .unwrap();
    graph.set_finish_point("a").unwrap();
    graph.set_finish_point("b").unwrap();
    graph.set_finish_point("c").unwrap();
    graph.compile().unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_single_node(c: &mut Criterion) {
    let compiled = build_single_node_graph();
    c.bench_function("single_node", |b| {
        b.iter(|| {
            black_box(
                compiled
                    .invoke(Some(StateValue::Null), RuntimeContext::default())
                    .unwrap(),
            );
        });
    });
}

fn bench_linear_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("linear_chain");
    for n in [5usize, 10, 20] {
        let compiled = build_linear_chain(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    compiled
                        .invoke(Some(StateValue::Null), RuntimeContext::default())
                        .unwrap(),
                );
            });
        });
    }
    group.finish();
}

fn bench_conditional_edge(c: &mut Criterion) {
    let compiled = build_conditional_graph();
    c.bench_function("conditional_edge", |b| {
        b.iter(|| {
            black_box(
                compiled
                    .invoke(Some(StateValue::Null), RuntimeContext::default())
                    .unwrap(),
            );
        });
    });
}

fn bench_stream_values(c: &mut Criterion) {
    let compiled = build_linear_chain(10);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("stream_values", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut rx = compiled
                    .stream(Some(StateValue::Null), RuntimeContext::default())
                    .unwrap();
                while rx.recv().await.is_some() {}
            });
        });
    });
}

fn bench_checkpoint(c: &mut Criterion) {
    let compiled = build_linear_chain(10);
    c.bench_function("checkpoint", |b| {
        b.iter(|| {
            let ctx = RuntimeContext::new(()).with_checkpointer(MemorySaver::new());
            black_box(compiled.invoke(Some(StateValue::Null), ctx).unwrap());
        });
    });
}

// ---------------------------------------------------------------------------
// Python comparison helper
// ---------------------------------------------------------------------------

fn load_python_results(path: &str) -> Option<HashMap<String, f64>> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut map = HashMap::new();
    let mut rest = content.as_str();
    while let Some(pos) = rest.find("\"median_sec\":") {
        let start = pos + "\"median_sec\":".len();
        let val_str = rest[start..].trim_start();
        let end = val_str
            .find(|c: char| matches!(c, ',' | '}' | '\n'))
            .unwrap_or(val_str.len());
        let value: f64 = val_str[..end].trim().parse().ok()?;
        let before = &rest[..pos];
        if let Some(key_end) = before.rfind("\":") {
            let key_start = before[..key_end].rfind('"').map(|i| i + 1).unwrap_or(0);
            let key = before[key_start..key_end].trim_matches('"').to_string();
            map.insert(key, value);
        }
        rest = &rest[pos + 1..];
    }
    if map.is_empty() { None } else { Some(map) }
}

fn print_python_comparison() {
    let path = "benches/python_bench_results.json";
    let Some(py) = load_python_results(path) else {
        println!(
            "\n--- Python benchmark results not found at {path}. Run `python benches/python_bench.py` first. ---\n"
        );
        return;
    };

    println!(
        "\n{:=^72}",
        " Python Baseline (from python_bench_results.json) "
    );
    println!("{:<24} {:>14}", "benchmark", "Python median (ms)");
    println!("{:-<40}", "");
    let mut keys: Vec<_> = py.keys().collect();
    keys.sort();
    for key in &keys {
        println!("{:<24} {:>14.6}", key, py[*key] * 1000.0);
    }
    println!(
        "{:=^72}\n",
        " Compare Rust timings above by running `cargo bench` "
    );
}

fn main() {
    print_python_comparison();

    let mut criterion = Criterion::default()
        .significance_level(0.1)
        .sample_size(100);

    bench_single_node(&mut criterion);
    bench_linear_chain(&mut criterion);
    bench_conditional_edge(&mut criterion);
    bench_stream_values(&mut criterion);
    bench_checkpoint(&mut criterion);

    criterion.final_summary();
}
