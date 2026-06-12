"""同场景 Python 基准测试，生成 python_bench_results.json。

用法: python benches/python_bench.py
输出: benches/python_bench_results.json
"""

import json
import time
import os
from statistics import median
from typing import TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver


class State(TypedDict):
    value: str


def run_invoke(graph, input_val, iterations=500, config=None):
    times = []
    for _ in range(max(10, iterations // 20)):
        graph.invoke(input_val, config=config)
    for _ in range(iterations):
        t0 = time.perf_counter()
        graph.invoke(input_val, config=config)
        times.append(time.perf_counter() - t0)
    return median(times)


def run_stream(graph, input_val, iterations=200):
    times = []
    for _ in range(10):
        for _ in graph.stream(input_val, stream_mode="values"):
            pass
    for _ in range(iterations):
        t0 = time.perf_counter()
        for _ in graph.stream(input_val, stream_mode="values"):
            pass
        times.append(time.perf_counter() - t0)
    return median(times)


def main():
    results = {}
    input_val = {"value": ""}

    # --- single_node ---
    builder = StateGraph(State)
    builder.add_node("write", lambda state: {"value": "done"})
    builder.add_edge(START, "write")
    builder.add_edge("write", END)
    graph = builder.compile()
    t = run_invoke(graph, input_val)
    results["single_node"] = {"median_sec": t}
    print(f"single_node:          {t * 1000:.6f} ms")

    # --- linear_chain ---
    for n in (5, 10, 20):
        builder = StateGraph(State)
        names = [f"n{i}" for i in range(n)]
        for i, name in enumerate(names):
            idx = i
            builder.add_node(name, lambda state, _idx=idx: {"value": f"step{_idx}"})
        builder.add_edge(START, names[0])
        for i in range(len(names) - 1):
            builder.add_edge(names[i], names[i + 1])
        builder.add_edge(names[-1], END)
        graph = builder.compile()
        t = run_invoke(graph, input_val)
        key = f"linear_chain_{n}"
        results[key] = {"median_sec": t}
        print(f"{key:<20} {t * 1000:.6f} ms")

    # --- conditional_edge ---
    builder = StateGraph(State)
    builder.add_node("route", lambda state: state)
    for name in ("a", "b", "c"):
        _name = name
        builder.add_node(name, lambda state, n=_name: {"value": n})
    builder.add_edge(START, "route")
    builder.add_conditional_edges(
        "route",
        lambda state: "b",
        {"a": "a", "b": "b", "c": "c"},
    )
    for name in ("a", "b", "c"):
        builder.add_edge(name, END)
    graph = builder.compile()
    t = run_invoke(graph, input_val)
    results["conditional_edge"] = {"median_sec": t}
    print(f"conditional_edge:     {t * 1000:.6f} ms")

    # --- stream_values ---
    builder = StateGraph(State)
    names = [f"n{i}" for i in range(10)]
    for i, name in enumerate(names):
        idx = i
        builder.add_node(name, lambda state, _idx=idx: {"value": f"step{_idx}"})
    builder.add_edge(START, names[0])
    for i in range(len(names) - 1):
        builder.add_edge(names[i], names[i + 1])
    builder.add_edge(names[-1], END)
    graph = builder.compile()
    t = run_stream(graph, input_val)
    results["stream_values"] = {"median_sec": t}
    print(f"stream_values:        {t * 1000:.6f} ms")

    # --- checkpoint ---
    builder = StateGraph(State)
    names = [f"n{i}" for i in range(10)]
    for i, name in enumerate(names):
        idx = i
        builder.add_node(name, lambda state, _idx=idx: {"value": f"step{_idx}"})
    builder.add_edge(START, names[0])
    for i in range(len(names) - 1):
        builder.add_edge(names[i], names[i + 1])
    builder.add_edge(names[-1], END)
    graph = builder.compile(checkpointer=MemorySaver())
    cfg = {"configurable": {"thread_id": "bench"}}
    t = run_invoke(graph, input_val, iterations=200, config=cfg)
    results["checkpoint"] = {"median_sec": t}
    print(f"checkpoint:           {t * 1000:.6f} ms")

    # --- output ---
    out_path = os.path.join(os.path.dirname(__file__), "python_bench_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")


if __name__ == "__main__":
    main()
