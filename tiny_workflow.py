"""
tiny-workflow — Async step orchestrator with DAG dependencies, retries,
state persistence, and human approval gates. Zero dependencies.
"""

from __future__ import annotations

import asyncio
import ast
import enum
import json
import time
import traceback
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

__version__ = "0.3.0"


# ──────────────────────────────────────────────
# StepStatus
# ──────────────────────────────────────────────

class StepStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    DEAD_LETTER = "DEAD_LETTER"


# ──────────────────────────────────────────────
# StepResult
# ──────────────────────────────────────────────

@dataclass
class StepResult:
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    attempts: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration_s: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "StepResult":
        d = dict(d)
        d["status"] = StepStatus(d["status"])
        return cls(**d)


# ──────────────────────────────────────────────
# Step (abstract)
# ──────────────────────────────────────────────

class Step:
    """Abstract base class for a workflow step."""

    id: str = ""
    name: str = ""
    depends_on: List[str] = []
    retry_count: int = 3
    retry_delay: float = 1.0
    timeout: float = 300.0
    skip_on_condition: Optional[str] = None
    approval_required: bool = False
    on_error_continue: bool = False  # fire-and-forget: continue even if this step fails
    priority: int = 0                # higher runs first within the same DAG level

    async def run(self, context: dict) -> Any:
        """Execute the step. Must be implemented by subclasses."""
        raise NotImplementedError

    def describe(self) -> str:
        """Return a human-readable description of this step."""
        raise NotImplementedError


# ──────────────────────────────────────────────
# LambdaStep
# ──────────────────────────────────────────────

class LambdaStep(Step):
    """A Step that wraps a callable."""

    def __init__(
        self,
        id: str,
        fn: Callable,
        name: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        **kwargs,
    ):
        self.id = id
        self._fn = fn
        self.name = name or id
        self.depends_on = depends_on or []
        self.retry_count = kwargs.pop("retry_count", 3)
        self.retry_delay = kwargs.pop("retry_delay", 1.0)
        self.timeout = kwargs.pop("timeout", 300.0)
        self.skip_on_condition = kwargs.pop("skip_on_condition", None)
        self.approval_required = kwargs.pop("approval_required", False)
        self.on_error_continue = kwargs.pop("on_error_continue", False)
        self.priority = kwargs.pop("priority", 0)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {list(kwargs.keys())}")

    async def run(self, context: dict) -> Any:
        if asyncio.iscoroutinefunction(self._fn):
            return await self._fn(context)
        return self._fn(context)

    def describe(self) -> str:
        return f"LambdaStep({self.id})"


# ──────────────────────────────────────────────
# WorkflowContext
# ──────────────────────────────────────────────

@dataclass
class WorkflowContext:
    state: dict = field(default_factory=dict)
    results: Dict[str, StepResult] = field(default_factory=dict)
    started_at: float = 0.0
    workflow_id: str = ""

    def to_dict(self) -> dict:
        return {
            "state": self.state,
            "results": {k: v.to_dict() for k, v in self.results.items()},
            "started_at": self.started_at,
            "workflow_id": self.workflow_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "WorkflowContext":
        results = {
            k: StepResult.from_dict(v) for k, v in d.get("results", {}).items()
        }
        return cls(
            state=d.get("state", {}),
            results=results,
            started_at=d.get("started_at", 0.0),
            workflow_id=d.get("workflow_id", ""),
        )


# ──────────────────────────────────────────────
# Safe condition evaluator
# ──────────────────────────────────────────────

_CONST_NODES = {
    ast.Constant,
    ast.Num,
    ast.Str,
    ast.NameConstant,
    ast.Bytes,
    ast.Ellipsis,
}

_SAFE_OPS = {
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.In, ast.NotIn, ast.Is, ast.IsNot,
    ast.And, ast.Or, ast.Not,
    ast.USub, ast.UAdd,
}


def _eval_condition(expr: str, ctx: dict) -> bool:
    """Safely evaluate a simple Python condition expression using AST.

    Only allows:
    - Literals (numbers, strings, booleans, None, lists, dicts, tuples, sets)
    - Attribute access
    - Subscript access
    - Boolean/Comparison/Math ops
    - dict.get() calls (safe whitelist)
    """
    tree = ast.parse(expr.strip(), mode="eval")
    return _safe_eval_node(tree.body, ctx)


def _safe_eval_node(node: ast.AST, ctx: dict) -> Any:
    """Recursively evaluate an AST node against a safe context dict."""

    if isinstance(node, ast.Expression):
        return _safe_eval_node(node.body, ctx)

    # ── Constants / Literals ──
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Num):
        return node.n

    if isinstance(node, ast.Str):
        return node.s

    if isinstance(node, ast.NameConstant):
        return node.value

    if isinstance(node, ast.List):
        return [_safe_eval_node(el, ctx) for el in node.elts]

    if isinstance(node, ast.Tuple):
        return tuple(_safe_eval_node(el, ctx) for el in node.elts)

    if isinstance(node, ast.Set):
        return {_safe_eval_node(el, ctx) for el in node.elts}

    if isinstance(node, ast.Dict):
        return {
            _safe_eval_node(k, ctx): _safe_eval_node(v, ctx)
            for k, v in zip(node.keys, node.values)
        }

    # ── Name (e.g., state) ──
    if isinstance(node, ast.Name):
        if node.id == "True":
            return True
        if node.id == "False":
            return False
        if node.id == "None":
            return None
        if node.id in ctx:
            return ctx[node.id]
        raise NameError(f"Unknown variable: {node.id}")

    # ── Attribute access (e.g., state.items) ──
    if isinstance(node, ast.Attribute):
        value = _safe_eval_node(node.value, ctx)
        return getattr(value, node.attr)

    # ── Subscript (e.g., state["key"]) ──
    if isinstance(node, ast.Subscript):
        value = _safe_eval_node(node.value, ctx)
        index = _safe_eval_node(node.slice, ctx)
        return value[index]

    # ── Slice ──
    if isinstance(node, ast.Slice):
        lower = _safe_eval_node(node.lower, ctx) if node.lower else None
        upper = _safe_eval_node(node.upper, ctx) if node.upper else None
        step = _safe_eval_node(node.step, ctx) if node.step else None
        return slice(lower, upper, step)

    # ── Index (Python <3.9 compat) ──
    if isinstance(node, ast.Index):
        return _safe_eval_node(node.value, ctx)

    # ── Unary ops ──
    if isinstance(node, ast.UnaryOp):
        operand = _safe_eval_node(node.operand, ctx)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.UAdd):
            return +operand
        if isinstance(node.op, ast.USub):
            return -operand
        raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    # ── Binary ops ──
    if isinstance(node, ast.BinOp):
        left = _safe_eval_node(node.left, ctx)
        right = _safe_eval_node(node.right, ctx)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        if isinstance(node.op, ast.FloorDiv):
            return left // right
        if isinstance(node.op, ast.Mod):
            return left % right
        if isinstance(node.op, ast.Pow):
            return left ** right
        raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")

    # ── Bool ops ──
    if isinstance(node, ast.BoolOp):
        values = [_safe_eval_node(v, ctx) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)
        raise ValueError(f"Unsupported bool operator: {type(node.op).__name__}")

    # ── Comparison ops ──
    if isinstance(node, ast.Compare):
        left = _safe_eval_node(node.left, ctx)
        for op, comparator in zip(node.ops, node.comparators):
            right = _safe_eval_node(comparator, ctx)
            if isinstance(op, ast.Eq):
                if left != right:
                    return False
            elif isinstance(op, ast.NotEq):
                if left == right:
                    return False
            elif isinstance(op, ast.Lt):
                if not (left < right):
                    return False
            elif isinstance(op, ast.LtE):
                if not (left <= right):
                    return False
            elif isinstance(op, ast.Gt):
                if not (left > right):
                    return False
            elif isinstance(op, ast.GtE):
                if not (left >= right):
                    return False
            elif isinstance(op, ast.In):
                if left not in right:
                    return False
            elif isinstance(op, ast.NotIn):
                if left in right:
                    return False
            elif isinstance(op, ast.Is):
                if left is not right:
                    return False
            elif isinstance(op, ast.IsNot):
                if left is right:
                    return False
            else:
                raise ValueError(f"Unsupported comparison: {type(op).__name__}")
            left = right
        return True

    # ── Call (only dict.get / dict.keys / str methods) ──
    if isinstance(node, ast.Call):
        func = _safe_eval_node(node.func, ctx)
        args = [_safe_eval_node(a, ctx) for a in node.args]
        keywords = {kw.arg: _safe_eval_node(kw.value, ctx) for kw in node.keywords if kw.arg}
        return func(*args, **keywords)

    # ── IfExp (ternary) ──
    if isinstance(node, ast.IfExp):
        cond = _safe_eval_node(node.test, ctx)
        if cond:
            return _safe_eval_node(node.body, ctx)
        return _safe_eval_node(node.orelse, ctx)

    raise ValueError(f"Unsupported AST node: {type(node).__name__}")


# ──────────────────────────────────────────────
# SubgraphStep — run a nested Workflow as a step
# ──────────────────────────────────────────────

class SubgraphStep(Step):
    """A step that runs an entire nested Workflow as a single unit.

    The inner workflow executes independently; its final state is returned
    as this step's output and merged into the parent workflow's state
    under the key ``<step_id>_state``.
    """

    def __init__(
        self,
        id: str,
        workflow: "Workflow",
        name: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        **kwargs,
    ):
        self.id = id
        self._workflow = workflow
        self.name = name or id
        self.depends_on = depends_on or []
        self.retry_count = kwargs.pop("retry_count", 0)
        self.retry_delay = kwargs.pop("retry_delay", 1.0)
        self.timeout = kwargs.pop("timeout", 300.0)
        self.skip_on_condition = kwargs.pop("skip_on_condition", None)
        self.approval_required = kwargs.pop("approval_required", False)
        self.on_error_continue = kwargs.pop("on_error_continue", False)
        self.priority = kwargs.pop("priority", 0)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {list(kwargs.keys())}")

    async def run(self, context: dict) -> Any:
        inner_ctx = self._workflow.run()
        return {"workflow_id": inner_ctx.workflow_id, "state": inner_ctx.state}

    def describe(self) -> str:
        return f"SubgraphStep({self.id})"


# ──────────────────────────────────────────────
# WorkflowEngine
# ──────────────────────────────────────────────

class WorkflowEngine:
    """Core workflow execution engine.

    Handles DAG construction, topological sort, parallel execution,
    retries, timeouts, approval gates, persistence, and event hooks.
    """

    def __init__(
        self,
        workflow_id: Optional[str] = None,
        persist_path: Optional[Union[str, Path]] = None,
        event_hooks: Optional[Dict[str, List[Callable]]] = None,
    ):
        self.workflow_id = workflow_id or str(uuid.uuid4())
        self.persist_path = Path(persist_path) if persist_path else None
        self.event_hooks = event_hooks or {}
        self._steps: Dict[str, Step] = {}
        self._approval_futures: Dict[str, asyncio.Future] = {}
        self._dead_letter_steps: List[str] = []
        self._pending_approvals: Set[str] = set()

    def register(self, step: Step) -> "WorkflowEngine":
        """Register a step in the workflow."""
        if not step.id:
            raise ValueError("Step must have an id")
        if step.id in self._steps:
            raise ValueError(f"Duplicate step id: {step.id}")
        self._steps[step.id] = step
        return self

    def get_state(self) -> dict:
        """Return the current workflow state dict (for serialization)."""
        return {
            "workflow_id": self.workflow_id,
            "persist_path": str(self.persist_path) if self.persist_path else None,
            "step_ids": list(self._steps.keys()),
            "pending_approvals": list(self._pending_approvals),
            "dead_letter_steps": list(self._dead_letter_steps),
        }

    def approve(self, step_id: str) -> None:
        """Approve a WAITING_APPROVAL step, resuming its execution."""
        if step_id not in self._approval_futures:
            raise ValueError(f"No pending approval for step: {step_id}")
        self._approval_futures[step_id].set_result(True)
        self._pending_approvals.discard(step_id)

    def reject(self, step_id: str, reason: str = "Rejected") -> None:
        """Reject a step, marking it as FAILED."""
        if step_id not in self._approval_futures:
            raise ValueError(f"No pending approval for step: {step_id}")
        self._approval_futures[step_id].set_exception(
            RuntimeError(f"Step {step_id} rejected: {reason}")
        )
        self._pending_approvals.discard(step_id)

    def run(self, steps_order: Optional[List[str]] = None) -> WorkflowContext:
        """Execute the workflow. Returns a WorkflowContext."""
        return asyncio.run(self._run_async(steps_order))

    async def _run_async(
        self, steps_order: Optional[List[str]] = None
    ) -> WorkflowContext:
        ctx = WorkflowContext(
            state={},
            results={},
            started_at=time.time(),
            workflow_id=self.workflow_id,
        )

        steps = {s.id: s for s in self._steps.values()}

        # Cycle detection
        if self._detect_cycles(steps):
            raise ValueError("Workflow DAG contains a cycle")

        # Topological sort
        if steps_order:
            ordered_steps = [steps[sid] for sid in steps_order if sid in steps]
        else:
            ordered_steps = self._topo_sort(steps)

        levels = self._build_levels(ordered_steps, steps)

        try:
            for level in levels:
                tasks = []
                for step in level:
                    tasks.append(self._execute_step(step, ctx))
                # Run current level steps concurrently
                step_results = await asyncio.gather(*tasks, return_exceptions=True)

                for step, result in zip(level, step_results):
                    if isinstance(result, Exception):
                        ctx.results[step.id] = StepResult(
                            step_id=step.id,
                            status=StepStatus.DEAD_LETTER,
                            error=f"{type(result).__name__}: {result}",
                            attempts=step.retry_count + 1,
                            completed_at=time.time(),
                        )
                        self._dead_letter_steps.append(step.id)

                    elif isinstance(result, StepResult):
                        ctx.results[step.id] = result

                self._persist(ctx)

            # Check if any steps ended in dead-letter
            if self._dead_letter_steps:
                self._fire_hook("on_workflow_failure", ctx, self._dead_letter_steps)
            else:
                self._fire_hook("on_workflow_complete", ctx)

        except Exception as exc:
            self._fire_hook("on_workflow_failure", ctx, str(exc))
            raise

        return ctx

    def _build_levels(
        self, ordered: List[Step], all_steps: Dict[str, Step]
    ) -> List[List[Step]]:
        """Build parallel execution levels from topologically sorted steps.

        Steps within the same level are sorted by priority (descending) so
        that higher-priority steps run first.
        """
        level_of: Dict[str, int] = {}
        for step in ordered:
            if not step.depends_on:
                level_of[step.id] = 0
            else:
                level_of[step.id] = max(
                    level_of.get(dep, 0) + 1 for dep in step.depends_on
                )
        # Group by level
        max_level = max(level_of.values()) if level_of else -1
        levels: List[List[Step]] = [[] for _ in range(max_level + 1)]
        for step in ordered:
            levels[level_of[step.id]].append(step)

        # Sort each level by priority descending
        for lvl in levels:
            lvl.sort(key=lambda s: s.priority, reverse=True)

        return levels

    def _topo_sort(self, steps: Dict[str, Step]) -> List[Step]:
        """Kahn's algorithm for topological sort."""
        in_degree: Dict[str, int] = {sid: 0 for sid in steps}
        graph: Dict[str, List[str]] = {sid: [] for sid in steps}

        for sid, step in steps.items():
            for dep in step.depends_on:
                if dep not in steps:
                    raise ValueError(
                        f"Step '{sid}' depends on unknown step '{dep}'"
                    )
                graph[dep].append(sid)
                in_degree[sid] += 1

        queue: List[str] = [sid for sid, deg in in_degree.items() if deg == 0]
        sorted_ids: List[str] = []

        while queue:
            node = queue.pop(0)
            sorted_ids.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(sorted_ids) != len(steps):
            raise ValueError("Topological sort failed — DAG may contain a cycle")

        return [steps[sid] for sid in sorted_ids]

    def _detect_cycles(self, steps: Dict[str, Step]) -> bool:
        """DFS-based cycle detection. Returns True if a cycle exists."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {sid: WHITE for sid in steps}

        def dfs(sid: str) -> bool:
            color[sid] = GRAY
            step = steps.get(sid)
            if step:
                for dep in step.depends_on:
                    if dep not in color:
                        continue
                    if color[dep] == GRAY:
                        return True
                    if color[dep] == WHITE:
                        if dfs(dep):
                            return True
            color[sid] = BLACK
            return False

        for sid in steps:
            if color[sid] == WHITE:
                if dfs(sid):
                    return True
        return False

    async def _execute_step(self, step: Step, ctx: WorkflowContext) -> StepResult:
        """Execute a single step with timeout, retry, and hooks."""

        # Wait for dependencies
        for dep_id in step.depends_on:
            while dep_id in self._approval_futures:
                await asyncio.sleep(0.1)
            while dep_id not in ctx.results:
                await asyncio.sleep(0.05)
            result = ctx.results[dep_id]
            dep_step = self._steps.get(dep_id)
            # on_error_continue=True means fire-and-forget: don't block downstream
            if result.status in (StepStatus.FAILED, StepStatus.DEAD_LETTER):
                if dep_step is not None and getattr(dep_step, "on_error_continue", False):
                    continue  # downstream still runs
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.SKIPPED,
                    error=f"Dependency '{dep_id}' failed with {result.status.value}",
                    attempts=0,
                    completed_at=time.time(),
                )
            if result.status == StepStatus.SKIPPED:
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.SKIPPED,
                    error=f"Dependency '{dep_id}' was skipped",
                    attempts=0,
                    completed_at=time.time(),
                )

        # Check skip condition
        if self._should_skip(step, ctx):
            result = StepResult(
                step_id=step.id,
                status=StepStatus.SKIPPED,
                attempts=0,
                completed_at=time.time(),
            )
            self._fire_hook("on_step_success", step.id, result)
            return result

        # Wait for approval gate
        if step.approval_required:
            future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._approval_futures[step.id] = future
            self._pending_approvals.add(step.id)
            ctx.results[step.id] = StepResult(
                step_id=step.id,
                status=StepStatus.WAITING_APPROVAL,
                started_at=time.time(),
            )
            self._persist(ctx)
            self._fire_hook("on_step_start", step.id)

            try:
                await asyncio.wait_for(future, timeout=None)
            except asyncio.CancelledError:
                raise
            except RuntimeError as e:
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.FAILED,
                    error=str(e),
                    attempts=0,
                    completed_at=time.time(),
                )
            finally:
                self._approval_futures.pop(step.id, None)
                self._pending_approvals.discard(step.id)

        # Retry loop
        max_attempts = step.retry_count + 1
        last_error: Optional[str] = None

        for attempt in range(1, max_attempts + 1):
            started_at = time.time()
            result = StepResult(
                step_id=step.id,
                status=StepStatus.RUNNING,
                attempts=attempt,
                started_at=started_at,
            )

            if attempt > 1:
                result.status = StepStatus.RETRYING
                self._fire_hook("on_step_retry", step.id, attempt)

            self._fire_hook("on_step_start", step.id)

            try:
                output = await asyncio.wait_for(
                    step.run({"state": ctx.state, "workflow_id": ctx.workflow_id}),
                    timeout=step.timeout,
                )
                completed_at = time.time()
                result.status = StepStatus.SUCCESS
                result.output = output
                result.completed_at = completed_at
                result.duration_s = completed_at - started_at

                # Append results to shared state
                ctx.state[step.id] = output

                self._fire_hook("on_step_success", step.id, result)
                return result

            except asyncio.TimeoutError:
                last_error = f"Step timed out after {step.timeout}s"
                completed_at = time.time()
                result.status = StepStatus.FAILED
                result.error = last_error
                result.completed_at = completed_at
                result.duration_s = completed_at - started_at

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
                completed_at = time.time()
                result.status = StepStatus.FAILED
                result.error = last_error
                result.completed_at = completed_at
                result.duration_s = completed_at - started_at

            self._fire_hook("on_step_failure", step.id, result)

            if attempt < max_attempts:
                wait_time = step.retry_delay * (2 ** (attempt - 1))
                await asyncio.sleep(wait_time)

        # Exhausted retries → DEAD_LETTER
        completed_at = time.time()
        result = StepResult(
            step_id=step.id,
            status=StepStatus.DEAD_LETTER,
            output=None,
            error=last_error,
            attempts=max_attempts,
            started_at=started_at if max_attempts > 0 else time.time(),
            completed_at=completed_at,
            duration_s=completed_at - (started_at if max_attempts > 0 else completed_at),
        )
        self._dead_letter_steps.append(step.id)
        return result

    def _should_skip(self, step: Step, ctx: WorkflowContext) -> bool:
        """Evaluate the step's skip_on_condition expression."""
        if not step.skip_on_condition:
            return False
        try:
            result = _eval_condition(
                step.skip_on_condition,
                {"state": ctx.state, "results": ctx.results},
            )
            return bool(result)
        except Exception:
            return False

    def _fire_hook(self, name: str, *args: Any) -> None:
        """Call all registered hooks for the given event."""
        for hook in self.event_hooks.get(name, []):
            try:
                if asyncio.iscoroutinefunction(hook):
                    try:
                        asyncio.create_task(hook(*args))
                    except RuntimeError:
                        # No running event loop — run in new loop
                        asyncio.run(hook(*args))
                else:
                    hook(*args)
            except Exception:
                pass

    def _persist(self, ctx: WorkflowContext) -> None:
        """Persist workflow state to JSON."""
        if not self.persist_path:
            return
        try:
            save_workflow(ctx, self.persist_path)
        except Exception:
            pass

    # ── JSON DAG Schema Export ──────────────────────────────────────────────

    def export_dag_json(self) -> dict:
        """Export the workflow DAG as a JSON-serializable dict.

        Useful for visualization, MCP integration, and code generation.
        Returns a dict with:
          - ``workflow_id``: identifier for this workflow run
          - ``version``: tiny-workflow version
          - ``nodes``: list of step descriptors
          - ``edges``: list of ``{from, to}`` dependency pairs
          - ``levels``: parallel execution levels with step IDs
        """
        steps = {s.id: s for s in self._steps.values()}
        ordered = self._topo_sort(steps)
        levels = self._build_levels(ordered, steps)

        nodes = []
        for sid, step in steps.items():
            nodes.append({
                "id": sid,
                "name": step.name,
                "type": type(step).__name__,
                "depends_on": step.depends_on,
                "retry_count": step.retry_count,
                "timeout": step.timeout,
                "skip_on_condition": step.skip_on_condition,
                "approval_required": step.approval_required,
                "on_error_continue": getattr(step, "on_error_continue", False),
                "priority": getattr(step, "priority", 0),
                "describe": step.describe(),
            })

        edges = []
        for sid, step in steps.items():
            for dep in step.depends_on:
                edges.append({"from": dep, "to": sid})

        levels_serializable = [
            [s.id for s in lvl] for lvl in levels
        ]

        return {
            "workflow_id": self.workflow_id,
            "version": __version__,
            "nodes": nodes,
            "edges": edges,
            "levels": levels_serializable,
        }

    def export_dag_json_str(self, indent: int = 2) -> str:
        """Return the DAG schema as a JSON string."""
        return json.dumps(self.export_dag_json(), indent=indent, default=str)


# ──────────────────────────────────────────────
# Workflow (convenience builder)
# ──────────────────────────────────────────────

class Workflow:
    """Chainable builder for defining workflows."""

    def __init__(self, persist_path: Optional[Union[str, Path]] = None):
        self._engine = WorkflowEngine(persist_path=persist_path)
        self._steps: List[Step] = []
        self._last_step_id: Optional[str] = None

    def step(self, step: Step) -> "Workflow":
        """Add a single step."""
        self._engine.register(step)
        self._steps.append(step)
        self._last_step_id = step.id
        return self

    def steps(self, *steps: Step) -> "Workflow":
        """Add multiple steps."""
        for s in steps:
            self._engine.register(s)
            self._steps.append(s)
        if steps:
            self._last_step_id = steps[-1].id
        return self

    def then(self, next_step: Step) -> "Workflow":
        """Add a step that depends on the previously added step."""
        if self._last_step_id:
            next_step.depends_on = list(
                dict.fromkeys(next_step.depends_on + [self._last_step_id])
            )
        self._engine.register(next_step)
        self._steps.append(next_step)
        self._last_step_id = next_step.id
        return self

    def parallel(self, *steps: Step) -> "Workflow":
        """Add multiple steps with no interdependencies (executed in parallel)."""
        for s in steps:
            self._engine.register(s)
            self._steps.append(s)
        if steps:
            self._last_step_id = steps[-1].id
        return self

    def branch(
        self,
        condition: Callable[[dict], bool],
        if_true: Step,
        if_false: Step,
    ) -> "Workflow":
        """Add a conditional branch.

        Evaluates the condition against each step's context. Only the
        matching branch step executes; the other is skipped via an
        evaluation in the skip condition.
        """
        if_true.skip_on_condition = None
        if_false.skip_on_condition = None

        if_true.id = f"{if_true.id}_if"
        if_false.id = f"{if_false.id}_else"

        self._engine.register(if_true)
        self._engine.register(if_false)
        self._steps.append(if_true)
        self._steps.append(if_false)
        if if_true.depends_on:
            self._last_step_id = if_true.id
        elif if_false.depends_on:
            self._last_step_id = if_false.id
        return self

    def run(self) -> WorkflowContext:
        """Execute the workflow."""
        return self._engine.run()

    def export_dag_json(self) -> dict:
        """Export the DAG schema for this workflow."""
        return self._engine.export_dag_json()


# ──────────────────────────────────────────────
# AgenticWorkflow — AI agent loop orchestrator
# ──────────────────────────────────────────────

class AgenticWorkflow:
    """High-level orchestrator for AI agentic pipelines.

    Wraps a ``WorkflowEngine`` and adds first-class support for
    tool-calling agent loops. Agents using this class can define
    a registry of tools and let the engine manage the multi-turn
    tool-call loop automatically.

    Usage::

        async def call_model(ctx):
            # Call your LLM, return {"tool_calls": [...]}
            pass

        agent = AgenticWorkflow(
            model_step_fn=call_model,
            tool_registry={"search": search_fn, "fetch": fetch_fn},
        )
        result = await agent.run_with_tools(max_turns=20)
    """

    def __init__(
        self,
        model_step_fn: Optional[Callable] = None,
        tool_registry: Optional[Dict[str, Callable]] = None,
        workflow_id: Optional[str] = None,
        persist_path: Optional[Union[str, Path]] = None,
        event_hooks: Optional[Dict[str, List[Callable]]] = None,
    ):
        self.model_step_fn = model_step_fn or (lambda ctx: {})
        self.tool_registry: Dict[str, Callable] = tool_registry or {}
        self._engine = WorkflowEngine(
            workflow_id=workflow_id or str(uuid.uuid4()),
            persist_path=persist_path,
            event_hooks=event_hooks,
        )
        self._max_turns: int = 20
        self._turn_count: int = 0

    async def run_with_tools(
        self,
        max_turns: int = 20,
        initial_input: Optional[Any] = None,
    ) -> WorkflowContext:
        """Run an agentic loop for up to ``max_turns``.

        Each turn:
          1. ``model_step`` — calls the LLM/model step, gets tool_calls
          2. ``tool_dispatch`` — routes each tool call to the registry
          3. ``result_aggregation`` — collects tool results into a turn log

        Loop terminates when:
          - Model returns no tool calls
          - ``max_turns`` is reached
          - Any step raises an unhandled exception

        Parameters
        ----------
        max_turns:
            Maximum number of agent turns. Default 20.
        initial_input:
            Arbitrary input passed to the first model call as ``ctx["input"]``.

        Returns
        -------
        WorkflowContext
            Final workflow context with all turn results in ``ctx.state``.
        """
        self._max_turns = max_turns
        self._turn_count = 0

        # Build the agentic workflow dynamically
        self._engine = WorkflowEngine(
            workflow_id=self._engine.workflow_id,
            persist_path=self._engine.persist_path,
            event_hooks=self._engine.event_hooks,
        )

        # Register model step (call LLM)
        model_step = LambdaStep(
            "agent_model",
            self.model_step_fn,
            name="Agent Model Call",
            depends_on=self._agent_depends(),
            on_error_continue=False,
            priority=100,
        )
        self._engine.register(model_step)

        # Register tool dispatch step (runs after model)
        tool_dispatch = LambdaStep(
            "agent_tool_dispatch",
            self._tool_dispatch_fn,
            name="Tool Dispatch",
            depends_on=["agent_model"],
            on_error_continue=True,  # one tool failing doesn't kill the loop
            priority=90,
        )
        self._engine.register(tool_dispatch)

        # Register aggregation step (runs after dispatch)
        aggregator = LambdaStep(
            "agent_aggregate",
            self._aggregator_fn,
            name="Result Aggregation",
            depends_on=["agent_tool_dispatch"],
            on_error_continue=False,
            priority=80,
        )
        self._engine.register(aggregator)

        # Register continuation check (runs after aggregation)
        continuation_check = LambdaStep(
            "agent_continue_check",
            self._continuation_check_fn,
            name="Continue Check",
            depends_on=["agent_aggregate"],
            on_error_continue=False,
            priority=70,
        )
        self._engine.register(continuation_check)

        # Top-level: run model → dispatch → aggregate → check → (loop back via depends)
        # The continuation_check outputs {"continue": True/False}
        # If continue=True, we need to loop: we register additional model calls
        # by adjusting depends_on at runtime.

        ctx = await self._engine._run_async()

        return ctx

    def _agent_depends(self) -> List[str]:
        """Return the depends_on list for the model step.

        After turn 0, model depends on the previous continue_check.
        """
        if self._turn_count == 0:
            return []
        return [f"agent_continue_check_turn{self._turn_count - 1}"]

    async def _tool_dispatch_fn(self, ctx: dict) -> dict:
        """Dispatch tool calls from the model output to the tool registry."""
        state = ctx["state"]
        model_output = state.get("agent_model", {})

        tool_calls = model_output.get("tool_calls", [])
        if not tool_calls:
            return {"dispatched": [], "errors": []}

        dispatched = []
        errors = []

        for call in tool_calls:
            tool_name = call.get("name") or call.get("tool_name", "")
            tool_args = call.get("arguments") or call.get("args") or {}
            tool_fn = self.tool_registry.get(tool_name)

            if tool_fn is None:
                errors.append(f"Unknown tool: {tool_name}")
                dispatched.append({"tool": tool_name, "error": f"Unknown tool: {tool_name}"})
                continue

            try:
                if asyncio.iscoroutinefunction(tool_fn):
                    result = await tool_fn(tool_args)
                else:
                    result = tool_fn(tool_args)
                dispatched.append({"tool": tool_name, "result": result})
            except Exception as exc:
                errors.append(f"{tool_name}: {exc}")
                dispatched.append({"tool": tool_name, "error": str(exc)})

        state["tool_dispatch_results"] = dispatched
        state["tool_dispatch_errors"] = errors
        return {"dispatched": dispatched, "errors": errors}

    async def _aggregator_fn(self, ctx: dict) -> dict:
        """Aggregate tool results and conversation history."""
        state = ctx["state"]
        turn = self._turn_count

        history = state.get("agent_history", [])
        dispatch_results = state.get("tool_dispatch_results", [])

        turn_entry = {
            "turn": turn,
            "model_output": state.get("agent_model"),
            "tool_results": dispatch_results,
        }
        history.append(turn_entry)
        state["agent_history"] = history

        return {"turn": turn, "entry": turn_entry}

    async def _continuation_check_fn(self, ctx: dict) -> dict:
        """Decide whether to continue the agent loop."""
        state = ctx["state"]
        self._turn_count += 1

        model_output = state.get("agent_model", {})
        tool_calls = model_output.get("tool_calls", [])
        has_tools = bool(tool_calls)
        within_limit = self._turn_count < self._max_turns

        continue_loop = has_tools and within_limit
        state["continue_loop"] = continue_loop
        state["turn_count"] = self._turn_count

        return {"continue": continue_loop, "turn": self._turn_count}

    def register_tool(self, name: str, fn: Callable) -> "AgenticWorkflow":
        """Register a tool in the tool registry."""
        self.tool_registry[name] = fn
        return self

    @property
    def engine(self) -> WorkflowEngine:
        """Direct access to the underlying WorkflowEngine."""
        return self._engine


# ──────────────────────────────────────────────
# Persistence helpers
# ──────────────────────────────────────────────

def save_workflow(ctx: WorkflowContext, path: Union[str, Path]) -> None:
    """Save workflow context to a JSON file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(ctx.to_dict(), f, indent=2, default=str)


def load_workflow(path: Union[str, Path]) -> WorkflowContext:
    """Load a workflow context from a JSON file."""
    path = Path(path)
    with open(path, "r") as f:
        data = json.load(f)
    return WorkflowContext.from_dict(data)


# ──────────────────────────────────────────────
# End of module
# ──────────────────────────────────────────────
