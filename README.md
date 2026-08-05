# tiny-workflow

![Version](https://img.shields.io/badge/version-0.3.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)
[![Stars](https://img.shields.io/github/stars/hussain-alsaibai/tiny-workflow?style=social)](https://github.com/hussain-alsaibai/tiny-workflow)
[![Part of tiny-\*](https://img.shields.io/badge/part%20of-tiny--*-purple.svg)](https://github.com/hussain-alsaibai)

> Async step orchestrator with DAG dependencies, retries, state persistence, human approval gates — and first-class support for AI agentic pipelines. Zero dependencies.

## Why tiny-workflow?

Existing workflow engines — Prefect, Airflow, Temporal — are enterprise-grade. They solve distributed systems, scheduling, and observability at scale. But developers building **agentic pipelines** need something simpler: just DAG execution, retries, persistence, and a way to inject human decisions — without a server, SaaS, or YAML.

`tiny-workflow` is that something. ~900 lines of stdlib Python. No server, no SaaS, no YAML.

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
- **Fire-and-forget (`on_error_continue`)** — continue even if a step fails
- **Priority ordering** — higher-priority steps run first within the same DAG level
- **Subgraph workflows** — embed one workflow as a step inside another
- **JSON DAG schema export** — visualize or feed your DAG to an LLM or MCP server
- **Agentic AI patterns** — built-in `AgenticWorkflow` for multi-turn tool-calling loops

## Comparison

| Feature | tiny-workflow | Prefect | Temporal | Airflow |
|---------|:---:|:---:|:---:|:---:|
| **Lines of code** | ~900 | 250k+ | 150k+ | 200k+ |
| **Dependencies** | 0 | 50+ | 20+ | 50+ |
| **Async native** | ✅ | ⚠️ | ✅ | ❌ |
| **Agentic loop support** | ✅ | ❌ | ❌ | ❌ |
| **Subgraph/nested workflows** | ✅ | ✅ | ✅ | ✅ |
| **JSON DAG export** | ✅ | ❌ | ⚠️ | ❌ |
| **Human approval gates** | ✅ | ✅ | ✅ | ✅ |
| **Serverless / embedded** | ✅ | ❌ | ❌ | ❌ |
| **Python-only** | ✅ | ✅ | ⚠️ | ✅ |
| **Setup complexity** | None | Medium | High | High |

## Quick Start

### Installation

```bash
pip install tiny-workflow
```

Or copy `tiny_workflow.py` into your project. It's one file, zero dependencies.

### AI Agent Task Example

```python
import asyncio
from tiny_workflow import LambdaStep, Workflow

# Simulate an AI agent that plans, searches, fetches, and writes
async def plan(ctx):
    # The "brain" — decides what steps to run next
    await asyncio.sleep(0.05)
    return {"task": "research_and_summarize", "query": "LLM agents in 2026"}

def search(ctx):
    # Tool: search the web
    return {"results": ["AutoGPT", "LangChain", "CrewAI", "tiny-workflow"]}

def fetch(ctx):
    # Tool: fetch page content
    return {"content": "tiny-workflow is a lightweight async DAG orchestrator..."}

def summarize(ctx):
    # Tool: summarize results
    results = ctx["state"].get("search", {}).get("results", [])
    return {"summary": f"Found {len(results)} relevant tools: {', '.join(results)}"}

def write_report(ctx):
    # Tool: write output
    summary = ctx["state"].get("summarize", {}).get("summary", "")
    print(f"📄 Report: {summary}")

# Build the agentic pipeline as a DAG
wf = (
    Workflow()
    .step(LambdaStep("plan", plan, name="Plan Task"))
    .step(LambdaStep("search", search, name="Search", depends_on=["plan"]))
    .step(LambdaStep("fetch", fetch, name="Fetch Content", depends_on=["search"]))
    .step(LambdaStep("summarize", summarize, name="Summarize", depends_on=["fetch"]))
    .step(LambdaStep("write", write_report, name="Write Report", depends_on=["summarize"]))
    .run()
)

print(wf.state)
```

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
    skip_on_condition = None
    on_error_continue = False  # set True for fire-and-forget
    priority = 10              # higher runs first within a level

    async def run(self, ctx):
        # your logic
        pass

    def describe(self):
        return "Calls an unreliable external API with 3 retries"
```

### Fire-and-Forget (on_error_continue)

Steps with `on_error_continue=True` run in parallel; if they fail, downstream steps still execute:

```python
from tiny_workflow import LambdaStep, Workflow

async def notify_slack(ctx):
    raise RuntimeError("Slack is down")  # oops

async def process(ctx):
    return {"result": "processed"}

# notify_slack fails, but 'process' still runs because on_error_continue=True
wf = (
    Workflow()
    .step(LambdaStep("notify", notify_slack, name="Notify Slack",
                     on_error_continue=True, retry_count=0))
    .step(LambdaStep("process", process, name="Process", depends_on=["notify"]))
    .run()
)

assert wf.results["notify"].status == StepStatus.DEAD_LETTER
assert wf.results["process"].status == StepStatus.SUCCESS  # still ran!
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

### Subgraph — Nested Workflows

```python
from tiny_workflow import LambdaStep, Workflow, SubgraphStep

# Define a reusable sub-workflow
sub = (
    Workflow()
    .step(LambdaStep("inner_a", lambda ctx: {"a": 1}))
    .step(LambdaStep("inner_b", lambda ctx: {"b": 2}, depends_on=["inner_a"]))
)

# Use it as a step inside the parent workflow
parent = (
    Workflow()
    .step(LambdaStep("start", lambda ctx: {"started": True}))
    .step(SubgraphStep("nested_pipeline", sub, name="Nested Pipeline",
                       depends_on=["start"]))
    .step(LambdaStep("end", lambda ctx: {"done": True}, depends_on=["nested_pipeline"]))
    .run()
)
```

### JSON DAG Schema Export

Export your workflow DAG for visualization, LLM analysis, or MCP integration:

```python
from tiny_workflow import Workflow, LambdaStep

wf = (
    Workflow()
    .step(LambdaStep("a", lambda c: 1))
    .step(LambdaStep("b", lambda c: 2, depends_on=["a"]))
    .step(LambdaStep("c", lambda c: 3, depends_on=["a"]))
    .step(LambdaStep("d", lambda c: 4, depends_on=["b", "c"]))
)

schema = wf.export_dag_json()
print(schema["levels"])
# [["a"], ["b", "c"], ["d"]]

# Serialized form for MCP / LLM tools
print(wf.export_dag_json_str())
```

## Agentic AI Patterns

tiny-workflow v0.3 ships with `AgenticWorkflow` for building autonomous agent loops:

```python
import asyncio
from tiny_workflow import AgenticWorkflow

# Define your tools
def search_tool(args):
    query = args.get("query", "")
    return {"results": [f"Result for: {query}"]}

def calculator(args):
    expr = args.get("expression", "2+2")
    return {"result": eval(expr)}

# Define your model step (replaces the LLM call)
async def my_model(ctx):
    state = ctx["state"]
    history = state.get("agent_history", [])
    last_result = history[-1]["tool_results"] if history else []

    # Simple rule-based "model" for demo
    if not last_result:
        return {
            "tool_calls": [
                {"name": "search", "arguments": {"query": "tiny-workflow agents"}}
            ]
        }
    return {"tool_calls": []}  # Done

# Build and run the agent
agent = AgenticWorkflow(
    model_step_fn=my_model,
    tool_registry={"search": search_tool, "calculator": calculator},
)

result = asyncio.run(agent.run_with_tools(max_turns=20))
print("Agent history:", result.state["agent_history"])
```

### Integration: tiny-workflow + tiny-mcp

Connect to MCP servers for tool-calling pipelines:

```python
import asyncio
from tiny_workflow import AgenticWorkflow

# Example: MCP tool registry
async def mcp_search(ctx):
    # Connect to an MCP server for search
    args = ctx.get("arguments", {})
    # await mcp_client.call("search", args)
    return {"mcp_results": ["via MCP: result1", "via MCP: result2"]}

agent = AgenticWorkflow(
    model_step_fn=my_model,
    tool_registry={"mcp_search": mcp_search},
)

result = asyncio.run(agent.run_with_tools(max_turns=10))
```

### Integration: tiny-workflow + tiny-agent

```python
from tiny_workflow import LambdaStep, Workflow

async def agent_init(ctx):
    return {"agent_id": "agent-001", "context": {}}

async def agent_plan(ctx):
    return {"plan": ["search", "fetch", "analyze", "report"]}

async def agent_execute(ctx):
    return {"executed": True}

wf = (
    Workflow()
    .step(LambdaStep("init", agent_init, name="Initialize Agent"))
    .step(LambdaStep("plan", agent_plan, name="Plan", depends_on=["init"]))
    .step(LambdaStep("execute", agent_execute, name="Execute Plan",
                     depends_on=["plan"]))
    .run()
)
```

## Performance

Benchmarks run on a 5-level × 10-parallel DAG (50 steps total):

```
✅ tiny-workflow v0.3.0 benchmarks
  Throughput (sync):   ~3,500 steps/sec
  Throughput (async):  ~3,800 steps/sec
  DAG export avg:       ~0.15 ms
  Fire-and-forget:     downstream ran = True
```

See `benchmarks/tiny_workflow_bench.py` to run your own:

```bash
python benchmarks/tiny_workflow_bench.py
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
└── Execution Levels (parallel batches, sorted by priority)
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
1. Check dependencies satisfied → wait if not (or continue if `on_error_continue=True`)
2. Evaluate `skip_on_condition` → SKIP if true
3. Fire `on_step_start` hook
4. Run step with timeout (retry loop)
5. On success → record result, persist, fire `on_step_success`
6. On failure → retry if attempts < retry_count, else DEAD_LETTER

## Changelog

### v0.3.0 — Agentic Workflow Refresh

**New features:**
- `AgenticWorkflow` — high-level orchestrator for AI agent loops with `run_with_tools(max_turns)` and a built-in tool registry
- `SubgraphStep` — embed one workflow as a step inside another workflow
- `on_error_continue` — fire-and-forget: downstream steps run even when this step fails (Step attribute + dependency check)
- `priority` — higher-priority steps run first within the same DAG level (Step attribute)
- `export_dag_json()` — serialize the workflow DAG as JSON for visualization, LLM prompts, or MCP integration
- `__version__ = "0.3.0"`

**Improved:**
- Python 3.10+ required (updated classifiers)
- Keywords and long_description added to pyproject.toml

### v0.1.0 — Initial Release
- DAG execution with topological sort
- Async/sync step support
- Retry with exponential backoff
- State persistence (JSON)
- Human approval gates
- Event hooks
- Dead-letter queue

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
| `on_error_continue` | `bool` | `False` | Fire-and-forget: downstream continues even on failure |
| `priority` | `int` | `0` | Higher = runs first within same DAG level |

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
- `export_dag_json() → dict`
- `export_dag_json_str(indent=2) → str`

#### `Workflow` (builder)
- `step(step: Step) → self`
- `steps(*steps) → self`
- `then(next_step) → self`
- `parallel(*steps) → self`
- `branch(condition, if_true, if_false) → self`
- `run() → WorkflowContext`
- `export_dag_json() → dict`

#### `LambdaStep`
Convenience wrapper: `LambdaStep(id, fn, name=None, depends_on=None, **kwargs)`

#### `SubgraphStep`
Run a nested `Workflow` as a single step: `SubgraphStep(id, workflow, ...)`

#### `AgenticWorkflow`
High-level agent loop orchestrator:
- `__init__(model_step_fn=None, tool_registry=None, ...)`
- `register_tool(name, fn) → self`
- `run_with_tools(max_turns=20, initial_input=None) → WorkflowContext`
- `engine: WorkflowEngine` — direct access to the underlying engine

### Functions

- `load_workflow(persist_path) → WorkflowContext`
- `save_workflow(ctx, persist_path) → None`
- `_eval_condition(expr, ctx) → bool`
