# tiny-workflow

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)
[![Part of tiny-*](https://img.shields.io/badge/part%20of-tiny--*-purple.svg)](https://github.com/hussain-alsaibai)

> Async step orchestrator with DAG dependencies, retries, state persistence, and human approval gates. Zero dependencies.

## Why tiny-workflow?

Existing workflow engines — Prefect, Airflow, Temporal — are enterprise-grade. They solve distributed systems, scheduling, and observability at scale. But developers building **agentic pipelines** need something simpler: just DAG execution, retries, persistence, and a way to inject human decisions.

`tiny-workflow` is that something. ~700 lines of stdlib Python. No server, no SaaS, no YAML.

## Features

- **DAG-based execution** — steps declare dependencies; engine handles the rest
- **Async/sync steps** — native `async def` and regular `def` both supported
- **Conditional branching** — skip steps based on workflow state
- **Retry with backoff** — configurable count and exponential backoff
- **State persistence (JSON)** — checkpoint after every step; resume on crash
- **Human approval gates** — pause workflow mid-flight for human review
- **Event hooks** — plug into step lifecycle events
- **Step timeout** — fail fast on runaway steps
- **Parallel execution** — steps at the same DAG level run concurrently
- **Dead-letter queue** — steps that exhaust retries land in DLQ

## Quick Start

### Installation

```bash
pip install tiny-workflow
```

Or copy `tiny_workflow.py` into your project. It's one file, zero dependencies.

### Define Steps

```python
import asyncio
from tiny_workflow import Workflow, LambdaStep

# Sync or async — both work
async def fetch_data(ctx):
    await asyncio.sleep(0.1)
    return {"data": [1, 2, 3]}

def process(ctx):
    return {"processed": sum(ctx["state"]["data"])}

def save(ctx):
    print(f"Saved: {ctx['state']['processed']}")

# Build the DAG
wf = (
    Workflow()
    .step(LambdaStep("fetch", fetch_data, name="Fetch Data"))
    .step(LambdaStep("process", process, name="Process Data", depends_on=["fetch"]))
    .step(LambdaStep("save", save, name="Save Result", depends_on=["process"]))
    .run()
)

print(wf.state)
```

### Retry, Timeout & Conditions

```python
from tiny_workflow import Step, WorkflowEngine, StepStatus

class UnstableAPI(Step):
    id = "unstable_api"
    name = "Call Unstable API"
    depends_on = []
    retry_count = 3
    retry_delay = 2.0
    timeout = 30.0
    skip_on_condition = None  # or "state.get('skip_api', False)"

    async def run(self, ctx):
        # your logic
        pass

    def describe(self):
        return "Calls an unreliable external API with 3 retries"
```

### Human Approval Gate

```python
from tiny_workflow import LambdaStep, StepStatus

async def deploy(ctx):
    # Deploy to production
    pass

deploy_step = LambdaStep(
    "deploy",
    deploy,
    name="Deploy to Production",
    depends_on=["test"],
    approval_required=True,
)

engine = WorkflowEngine(event_hooks={"on_workflow_complete": [...]})
engine.register(deploy_step)
# engine.run()  # blocks at deploy step, waiting for approval

# Later, from another process or the approval UI:
engine.approve("deploy")
# or:
engine.reject("deploy", reason="QA failed on staging")
```

### State Persistence

```python
engine = WorkflowEngine(
    workflow_id="nightly-pipeline-001",
    persist_path="/tmp/workflow_state.json",
)
engine.register(step1)
engine.register(step2)
ctx = engine.run()

# On restart:
ctx = load_workflow("/tmp/workflow_state.json")
print(ctx.state)
```

### Event Hooks

```python
hooks = {
    "on_step_start":      [lambda step_id: print(f"Starting: {step_id}")],
    "on_step_success":    [lambda step_id, result: print(f"Done: {step_id}")],
    "on_step_failure":    [lambda step_id, error: print(f"Failed: {step_id} — {error}")],
    "on_step_retry":      [lambda step_id, attempt: print(f"Retry {attempt}: {step_id}")],
    "on_workflow_complete": [lambda ctx: notify(ctx)],
    "on_workflow_failure":  [lambda ctx, failed_step: alert(failed_step)],
}

engine = WorkflowEngine(event_hooks=hooks)
```

### Conditional Branching

```python
def check_flag(ctx):
    return ctx["state"].get("run_expensive", False)

def cheap(ctx):
    return "cheap result"

def expensive(ctx):
    return "expensive result"

wf = (
    Workflow()
    .branch(
        check_flag,
        LambdaStep("cheap", cheap, name="Cheap Path"),
        LambdaStep("expensive", expensive, name="Expensive Path"),
    )
    .run()
)
```

## Architecture

```
WorkflowEngine
│
├── DAG Construction
│   └── steps.register() → builds adjacency list from depends_on
│
├── Topological Sort (Kahn's algorithm)
│   └── Validates DAG, detects cycles, orders execution levels
│
└── Execution Levels (parallel batches)
    │
    ├── Level 0: [step_a, step_b]     ← no deps, run concurrently
    │           ↓
    ├── Level 1: [step_c]             ← waits for a, b
    │           ↓
    ├── Level 2: [step_d, step_e]     ← both depend on c, run concurrently
    │           ↓
    └── Level 3: [step_f]             ← waits for d, e
```

Each step lifecycle:
1. Check dependencies satisfied → wait if not
2. Evaluate `skip_on_condition` → SKIP if true
3. Fire `on_step_start` hook
4. Run step with timeout (retry loop)
5. On success → record result, persist, fire `on_step_success`
6. On failure → retry if attempts < retry_count, else DEAD_LETTER

## API Reference

### Classes

#### `Step` (abstract)
Base class for all workflow steps.

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `id` | `str` | *required* | Unique step identifier |
| `name` | `str` | *required* | Human-readable name |
| `depends_on` | `list[str]` | `[]` | Step IDs this step waits for |
| `retry_count` | `int` | `3` | Retries on failure |
| `retry_delay` | `float` | `1.0` | Base delay between retries (s) |
| `timeout` | `float` | `300.0` | Step timeout (s) |
| `skip_on_condition` | `str \| None` | `None` | Python expression evaluated against state |
| `approval_required` | `bool` | `False` | Pause for human approval before running |

#### `StepStatus` (enum)
`PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `RETRYING`, `SKIPPED`, `WAITING_APPROVAL`, `DEAD_LETTER`

#### `StepResult` (dataclass)
- `step_id: str`
- `status: StepStatus`
- `output: Any`
- `error: str | None`
- `attempts: int`
- `started_at: float`
- `completed_at: float`
- `duration_s: float`

#### `WorkflowContext` (dataclass)
- `state: dict` — shared mutable state across steps
- `results: dict[str, StepResult]` — step ID → result
- `started_at: float`
- `workflow_id: str`

#### `WorkflowEngine`
- `__init__(workflow_id=None, persist_path=None, event_hooks=None)`
- `register(step: Step) → self`
- `run(steps_order=None) → WorkflowContext`
- `approve(step_id) → None`
- `reject(step_id, reason) → None`
- `get_state() → dict`

#### `Workflow` (builder)
- `step(step: Step) → self`
- `steps(*steps) → self`
- `then(next_step) → self`
- `parallel(*steps) → self`
- `branch(condition, if_true, if_false) → self`
- `run() → WorkflowContext`

#### `LambdaStep`
Convenience wrapper: `LambdaStep(id, fn, name=None, depends_on=None, **kwargs)`

### Functions

- `load_workflow(persist_path) → WorkflowContext`
- `save_workflow(ctx, persist_path) → None`
- `_eval_condition(expr, ctx) → bool`
