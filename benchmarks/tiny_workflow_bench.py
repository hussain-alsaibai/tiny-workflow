"""
tiny-workflow benchmarks — measuring throughput on DAG workloads.

Run with: python benchmarks/tiny_workflow_bench.py
"""

import asyncio
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiny_workflow import LambdaStep, WorkflowEngine, Workflow


# ──────────────────────────────────────────────
# Benchmark 1: 50 steps — 5 levels × 10 parallel
# ──────────────────────────────────────────────

def make_step(step_id: str):
    """Create a trivially-fast step (no I/O, pure CPU)."""
    async def fn(ctx):
        # Simulate ~0.1ms of async work
        await asyncio.sleep(0.0001)
        return {"id": step_id, "done": True}
    return LambdaStep(step_id, fn, name=step_id)


def build_workflow_50_steps() -> WorkflowEngine:
    """Build a 50-step DAG: 5 levels × 10 parallel steps per level."""
    engine = WorkflowEngine()

    # Level 0: 10 parallel steps
    level0 = [make_step(f"s0_{i}") for i in range(10)]
    for s in level0:
        engine.register(s)

    # Level 1: 10 parallel steps, each depends on all level-0 steps
    level1_ids = [f"s0_{i}" for i in range(10)]
    level1 = [make_step(f"s1_{i}") for i in range(10)]
    for s in level1:
        s.depends_on = list(level1_ids)
        engine.register(s)

    # Level 2: 10 parallel steps, each depends on all level-1 steps
    level2_ids = [f"s1_{i}" for i in range(10)]
    level2 = [make_step(f"s2_{i}") for i in range(10)]
    for s in level2:
        s.depends_on = list(level2_ids)
        engine.register(s)

    # Level 3: 10 parallel steps, each depends on all level-2 steps
    level3_ids = [f"s2_{i}" for i in range(10)]
    level3 = [make_step(f"s3_{i}") for i in range(10)]
    for s in level3:
        s.depends_on = list(level3_ids)
        engine.register(s)

    # Level 4: 10 parallel steps, each depends on all level-3 steps
    level4_ids = [f"s3_{i}" for i in range(10)]
    level4 = [make_step(f"s4_{i}") for i in range(10)]
    for s in level4:
        s.depends_on = list(level4_ids)
        engine.register(s)

    return engine


def run_sync_benchmark():
    """Run 50-step DAG in synchronous (asyncio.run) mode."""
    engine = build_workflow_50_steps()

    start = time.perf_counter()
    ctx = engine.run()
    elapsed = time.perf_counter() - start

    total_steps = sum(1 for r in ctx.results.values() if r.status.value == "SUCCESS")
    throughput = total_steps / elapsed if elapsed > 0 else 0

    return {
        "mode": "sync (asyncio.run)",
        "total_steps": total_steps,
        "elapsed_s": round(elapsed, 4),
        "steps_per_sec": round(throughput, 1),
        "workflow_id": ctx.workflow_id,
    }


async def run_async_benchmark():
    """Run 50-step DAG in true async mode (called from within event loop)."""
    engine = build_workflow_50_steps()

    start = time.perf_counter()
    ctx = await engine._run_async()
    elapsed = time.perf_counter() - start

    total_steps = sum(1 for r in ctx.results.values() if r.status.value == "SUCCESS")
    throughput = total_steps / elapsed if elapsed > 0 else 0

    return {
        "mode": "async (native await)",
        "total_steps": total_steps,
        "elapsed_s": round(elapsed, 4),
        "steps_per_sec": round(throughput, 1),
        "workflow_id": ctx.workflow_id,
    }


# ──────────────────────────────────────────────
# Benchmark 2: Priority ordering — 20 steps at same level
# ──────────────────────────────────────────────

def make_priority_step(step_id: str, priority: int):
    async def fn(ctx):
        await asyncio.sleep(0.0001)
        return f"priority_{priority}"
    s = LambdaStep(step_id, fn, name=step_id, priority=priority)
    return s


def run_priority_benchmark():
    """Verify that higher-priority steps complete first within a level."""
    engine = WorkflowEngine()
    step_ids_in_order = []

    for i in range(20):
        # Register in random priority order
        p = 19 - i  # reverse order: lowest i = highest priority
        s = make_priority_step(f"p_{i}", priority=p)
        s.name = f"prio_{p}"
        engine.register(s)

    async def tracker(ctx):
        pass

    # All 20 steps at level 0 (no deps) — should run in priority order
    ctx = engine.run()

    # Collect order from results
    order = list(ctx.results.keys())
    return {
        "total_steps": len(ctx.results),
        "order": order,
    }


# ──────────────────────────────────────────────
# Benchmark 3: on_error_continue fire-and-forget
# ──────────────────────────────────────────────

def make_error_step(step_id: str, will_fail: bool):
    async def fn(ctx):
        await asyncio.sleep(0.001)
        if will_fail:
            raise RuntimeError(f"{step_id} failed intentionally")
        return {"ok": True, "id": step_id}
    return LambdaStep(step_id, fn, name=step_id)


def run_error_continue_benchmark():
    """Test that on_error_continue=True steps don't block downstream."""
    engine = WorkflowEngine()

    # Fire-and-forget step that will fail — downstream should still run
    fail_step = make_error_step("will_fail", will_fail=True)
    fail_step.on_error_continue = True
    fail_step.retry_count = 0
    engine.register(fail_step)

    # Downstream step
    downstream = make_error_step("downstream", will_fail=False)
    downstream.depends_on = ["will_fail"]
    engine.register(downstream)

    ctx = engine.run()

    fail_status = ctx.results["will_fail"].status.value
    down_status = ctx.results["downstream"].status.value

    return {
        "fire_and_forget_status": fail_status,
        "downstream_status": down_status,
        "downstream_ran": down_status == "SUCCESS",
    }


# ──────────────────────────────────────────────
# Benchmark 4: JSON DAG export
# ──────────────────────────────────────────────

def run_dag_export_benchmark():
    """Measure JSON DAG schema export performance."""
    engine = build_workflow_50_steps()

    times = []
    for _ in range(100):
        start = time.perf_counter()
        schema = engine.export_dag_json()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_ms = (sum(times) / len(times)) * 1000
    p99_ms = sorted(times)[int(len(times) * 0.99)] * 1000

    return {
        "nodes_in_dag": len(schema["nodes"]),
        "edges_in_dag": len(schema["edges"]),
        "levels_in_dag": len(schema["levels"]),
        "avg_export_ms": round(avg_ms, 3),
        "p99_export_ms": round(p99_ms, 3),
    }


# ──────────────────────────────────────────────
# Main — run all benchmarks
# ──────────────────────────────────────────────

def print_result(label: str, result: dict):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    for k, v in result.items():
        print(f"  {k:<25} {v}")


def main():
    print("\n🚀 tiny-workflow v0.3.0 benchmarks")
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  asyncio: {asyncio.__version__}")

    # Benchmark 1a: sync execution
    print("\n⏳ Running sync benchmark (50 steps, 5 levels × 10 parallel)...")
    r1 = run_sync_benchmark()
    print_result("Sync execution (asyncio.run)", r1)

    # Benchmark 1b: async execution
    print("\n⏳ Running async benchmark (50 steps, 5 levels × 10 parallel)...")
    r2 = asyncio.run(run_async_benchmark())
    print_result("Async execution (native await)", r2)

    # Priority benchmark
    print("\n⏳ Running priority-ordering benchmark (20 steps, same level)...")
    r3 = run_priority_benchmark()
    print_result("Priority ordering", r3)

    # on_error_continue benchmark
    print("\n⏳ Running on_error_continue benchmark...")
    r4 = run_error_continue_benchmark()
    print_result("Fire-and-forget (on_error_continue)", r4)

    # DAG export benchmark
    print("\n⏳ Running JSON DAG schema export benchmark (100 iterations)...")
    r5 = run_dag_export_benchmark()
    print_result("JSON DAG schema export", r5)

    # Summary
    print(f"\n{'='*60}")
    print("  Summary")
    print(f"{'='*60}")
    print(f"  Throughput (sync):   {r1['steps_per_sec']} steps/sec")
    print(f"  Throughput (async):  {r2['steps_per_sec']} steps/sec")
    print(f"  Fire-and-forget:    downstream ran = {r4['downstream_ran']}")
    print(f"  DAG export avg:      {r5['avg_export_ms']} ms")
    print(f"\n✅ All benchmarks complete.\n")


if __name__ == "__main__":
    main()
