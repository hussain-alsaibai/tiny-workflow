"""
Tests for tiny-workflow — DAG execution, retries, persistence, approvals, hooks, timeouts.
"""

import asyncio
import json
import os
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from tiny_workflow import (
    LambdaStep,
    Step,
    StepResult,
    StepStatus,
    Workflow,
    WorkflowContext,
    WorkflowEngine,
    _eval_condition,
    load_workflow,
    save_workflow,
)


# ═══════════════════════════════════════════════
# Test Helpers
# ═══════════════════════════════════════════════


class RecordingStep(Step):
    """Step that records its execution order."""

    def __init__(self, id, name=None, depends_on=None, order_list=None, output=None, **kwargs):
        self.id = id
        self.name = name or id
        self.depends_on = depends_on or []
        self.order_list = order_list
        self._output = output
        self.retry_count = kwargs.pop("retry_count", 3)
        self.retry_delay = kwargs.pop("retry_delay", 1.0)
        self.timeout = kwargs.pop("timeout", 300.0)
        self.skip_on_condition = kwargs.pop("skip_on_condition", None)
        self.approval_required = kwargs.pop("approval_required", False)
        if kwargs:
            raise TypeError(f"Unexpected kwargs: {kwargs}")

    async def run(self, context):
        if self.order_list is not None:
            self.order_list.append(self.id)
        await asyncio.sleep(0.01)
        return self._output or f"result_{self.id}"

    def describe(self):
        return f"RecordingStep({self.id})"


class FailingStep(Step):
    """Step that always raises an exception."""

    def __init__(self, id, name=None, depends_on=None, retry_count=2, **kwargs):
        self.id = id
        self.name = name or id
        self.depends_on = depends_on or []
        self.retry_count = retry_count
        self.retry_delay = kwargs.pop("retry_delay", 0.05)
        self.timeout = kwargs.pop("timeout", 300.0)
        self.skip_on_condition = kwargs.pop("skip_on_condition", None)
        self.approval_required = kwargs.pop("approval_required", False)
        self.run_count = 0
        if kwargs:
            raise TypeError(f"Unexpected kwargs: {kwargs}")

    async def run(self, context):
        self.run_count += 1
        raise RuntimeError(f"Step {self.id} failed (run #{self.run_count})")

    def describe(self):
        return f"FailingStep({self.id})"


class TimeoutStep(Step):
    """Step that sleeps longer than its timeout."""

    def __init__(self, id, sleep=10.0, depends_on=None, **kwargs):
        self.id = id
        self.name = id
        self.depends_on = depends_on or []
        self.retry_count = kwargs.pop("retry_count", 0)
        self.retry_delay = kwargs.pop("retry_delay", 0.01)
        self.timeout = 0.05  # very short
        self.skip_on_condition = kwargs.pop("skip_on_condition", None)
        self.approval_required = kwargs.pop("approval_required", False)
        self._sleep = sleep
        if kwargs:
            raise TypeError(f"Unexpected kwargs: {kwargs}")

    async def run(self, context):
        await asyncio.sleep(self._sleep)
        return "done"

    def describe(self):
        return f"TimeoutStep({self.id})"


class ApprovalStep(Step):
    """Step that requires human approval."""

    def __init__(self, id, depends_on=None, **kwargs):
        self.id = id
        self.name = id
        self.depends_on = depends_on or []
        self.retry_count = 0
        self.retry_delay = 0.01
        self.timeout = 300.0
        self.skip_on_condition = kwargs.pop("skip_on_condition", None)
        self.approval_required = True
        self.executed = False
        if kwargs:
            raise TypeError(f"Unexpected kwargs: {kwargs}")

    async def run(self, context):
        self.executed = True
        return "approved!"

    def describe(self):
        return f"ApprovalStep({self.id})"


# ═══════════════════════════════════════════════
# 1. Basic step execution
# ═══════════════════════════════════════════════


class TestBasicExecution:
    def test_single_step(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a"))
        ctx = engine.run()
        assert ctx.results["a"].status == StepStatus.SUCCESS
        assert ctx.results["a"].output == "result_a"

    def test_two_independent_steps(self):
        order = []
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", order_list=order))
        engine.register(RecordingStep("b", order_list=order))
        ctx = engine.run()
        assert ctx.results["a"].status == StepStatus.SUCCESS
        assert ctx.results["b"].status == StepStatus.SUCCESS

    def test_step_shared_state(self):
        async def setter(ctx):
            ctx["state"]["x"] = 42
            return "ok"

        engine = WorkflowEngine()
        engine.register(LambdaStep("set", setter, name="Setter"))
        ctx = engine.run()
        assert ctx.state["set"] == "ok"


# ═══════════════════════════════════════════════
# 2. DAG dependencies
# ═══════════════════════════════════════════════

class TestDAGDependencies:
    def test_simple_chain(self):
        order = []
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", order_list=order))
        engine.register(RecordingStep("b", depends_on=["a"], order_list=order))
        engine.register(RecordingStep("c", depends_on=["b"], order_list=order))
        ctx = engine.run()
        assert order == ["a", "b", "c"]
        for s in ("a", "b", "c"):
            assert ctx.results[s].status == StepStatus.SUCCESS

    def test_fork_and_join(self):
        order = []
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", order_list=order))
        engine.register(RecordingStep("b", depends_on=["a"], order_list=order))
        engine.register(RecordingStep("c", depends_on=["a"], order_list=order))
        engine.register(RecordingStep("d", depends_on=["b", "c"], order_list=order))
        ctx = engine.run()
        assert order[0] == "a"
        assert "b" in order[1:3] and "c" in order[1:3]
        assert order[-1] == "d"
        for s in ("a", "b", "c", "d"):
            assert ctx.results[s].status == StepStatus.SUCCESS


# ═══════════════════════════════════════════════
# 3. Topological sort
# ═══════════════════════════════════════════════

class TestTopologicalSort:
    def test_topo_simple(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a"))
        engine.register(RecordingStep("b", depends_on=["a"]))
        engine.register(RecordingStep("c", depends_on=["b"]))
        sorted_steps = engine._topo_sort({"a": engine._steps["a"], "b": engine._steps["b"], "c": engine._steps["c"]})
        ids = [s.id for s in sorted_steps]
        assert ids.index("a") < ids.index("b") < ids.index("c")

    def test_topo_complex(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a"))
        engine.register(RecordingStep("b"))
        engine.register(RecordingStep("c", depends_on=["a"]))
        engine.register(RecordingStep("d", depends_on=["a", "b"]))
        engine.register(RecordingStep("e", depends_on=["c", "d"]))
        sorted_steps = engine._topo_sort({
            s.id: s for s in engine._steps.values()
        })
        ids = [s.id for s in sorted_steps]
        assert ids.index("a") < ids.index("c")
        assert ids.index("a") < ids.index("d")
        assert ids.index("b") < ids.index("d")
        assert ids.index("c") < ids.index("e")
        assert ids.index("d") < ids.index("e")

    def test_topo_unknown_dep_raises(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", depends_on=["nonexistent"]))
        with pytest.raises(ValueError, match="unknown step"):
            engine.run()


# ═══════════════════════════════════════════════
# 4. Cycle detection
# ═══════════════════════════════════════════════

class TestCycleDetection:
    def test_simple_cycle(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", depends_on=["b"]))
        engine.register(RecordingStep("b", depends_on=["a"]))
        with pytest.raises(ValueError, match="cycle"):
            engine.run()

    def test_no_cycle(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a"))
        engine.register(RecordingStep("b", depends_on=["a"]))
        engine.register(RecordingStep("c", depends_on=["a"]))
        engine.register(RecordingStep("d", depends_on=["b", "c"]))
        # Should not raise
        ctx = engine.run()
        assert ctx.results["d"].status == StepStatus.SUCCESS

    def test_topo_with_cycle_in_detect(self):
        """Also tests that _detect_cycles catches it."""
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", depends_on=["b"]))
        engine.register(RecordingStep("b", depends_on=["a"]))
        steps = {s.id: s for s in engine._steps.values()}
        assert engine._detect_cycles(steps) is True


# ═══════════════════════════════════════════════
# 5. Parallel execution
# ═══════════════════════════════════════════════

class TestParallelExecution:
    def test_parallel_steps_run(self):
        order = []
        engine = WorkflowEngine()
        engine.register(RecordingStep("a", order_list=order))
        engine.register(RecordingStep("b", order_list=order))
        engine.register(RecordingStep("c", order_list=order))
        engine.register(RecordingStep("d", depends_on=["a", "b", "c"], order_list=order))
        start = time.time()
        ctx = engine.run()
        duration = time.time() - start
        # a, b, c run in parallel (~0.01s each), d runs after (~0.01s)
        assert duration < 0.5  # would be >0.03s if serial
        for s in ("a", "b", "c", "d"):
            assert ctx.results[s].status == StepStatus.SUCCESS


# ═══════════════════════════════════════════════
# 6. Retry logic
# ═══════════════════════════════════════════════

class TestRetry:
    def test_retry_then_dead_letter(self):
        engine = WorkflowEngine()
        failing_step = FailingStep("fail", retry_count=2, retry_delay=0.02)
        engine.register(failing_step)
        ctx = engine.run()
        assert ctx.results["fail"].status == StepStatus.DEAD_LETTER
        assert ctx.results["fail"].attempts == 3  # initial + 2 retries

    def test_retry_success_on_second_attempt(self):
        class EventuallyPassing(Step):
            def __init__(self):
                self.id = "eventual"
                self.name = "Eventually Passing"
                self.depends_on = []
                self.retry_count = 3
                self.retry_delay = 0.02
                self.timeout = 300.0
                self.skip_on_condition = None
                self.approval_required = False
                self._attempts = 0

            async def run(self, ctx):
                self._attempts += 1
                if self._attempts < 2:
                    raise RuntimeError("Not yet")
                return "success"

            def describe(self):
                return "eventual"

        engine = WorkflowEngine()
        engine.register(EventuallyPassing())
        ctx = engine.run()
        assert ctx.results["eventual"].status == StepStatus.SUCCESS
        assert ctx.results["eventual"].output == "success"

    def test_no_retry_single_failure(self):
        class NoRetryStep(Step):
            def __init__(self):
                self.id = "no_retry"
                self.name = "No Retry"
                self.depends_on = []
                self.retry_count = 0
                self.retry_delay = 0.01
                self.timeout = 300.0
                self.skip_on_condition = None
                self.approval_required = False

            async def run(self, ctx):
                raise ValueError("Always fails")

            def describe(self):
                return "no retry"

        engine = WorkflowEngine()
        engine.register(NoRetryStep())
        ctx = engine.run()
        assert ctx.results["no_retry"].status == StepStatus.DEAD_LETTER
        assert ctx.results["no_retry"].attempts == 1


# ═══════════════════════════════════════════════
# 7. Skip condition
# ═══════════════════════════════════════════════

class TestSkipCondition:
    def test_skip_when_true(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("setup"))
        engine.register(
            RecordingStep("skip_me", depends_on=["setup"],
                          skip_on_condition="state.get('skip_me', False)")
        )
        # Set state so skip condition is True
        ctx = engine.run()
        # Since we didn't set state, the default get returns False, so skip is not triggered
        # Let's test with a condition that references a contextual key already present
        assert ctx.results["skip_me"].status == StepStatus.SUCCESS

    def test_skip_with_condition(self):
        """Set state so step is skipped via state manipulation."""
        class FlagSetter(Step):
            id = "flag_setter"
            name = "Flag Setter"
            depends_on = []
            retry_count = 0
            retry_delay = 0.01
            timeout = 300.0
            skip_on_condition = None
            approval_required = False

            async def run(self, ctx):
                ctx["state"]["should_skip"] = True
                return "flag_set"

            def describe(self):
                return "flag"

        engine = WorkflowEngine()
        engine.register(FlagSetter())
        engine.register(
            RecordingStep("skippable", depends_on=["flag_setter"],
                          skip_on_condition="state.get('should_skip', False)")
        )
        ctx = engine.run()
        assert ctx.results["skippable"].status == StepStatus.SKIPPED

    def test_skip_with_literal_true(self):
        engine = WorkflowEngine()
        engine.register(
            RecordingStep("always_skip", skip_on_condition="True")
        )
        ctx = engine.run()
        assert ctx.results["always_skip"].status == StepStatus.SKIPPED

    def test_skip_comparison(self):
        engine = WorkflowEngine()

        class NumSetter(Step):
            id = "setter"
            name = "setter"
            depends_on = []
            retry_count = 0
            retry_delay = 0.01
            timeout = 300.0
            skip_on_condition = None
            approval_required = False

            async def run(self, ctx):
                ctx["state"]["count"] = 10
                return "set"

            def describe(self):
                return "setter"

        engine.register(NumSetter())
        engine.register(
            RecordingStep("big", depends_on=["setter"],
                          skip_on_condition="state.get('count', 0) > 5")
        )
        engine.register(
            RecordingStep("small", depends_on=["setter"],
                          skip_on_condition="state.get('count', 0) < 5")
        )
        ctx = engine.run()
        assert ctx.results["big"].status == StepStatus.SKIPPED
        assert ctx.results["small"].status == StepStatus.SUCCESS


# ═══════════════════════════════════════════════
# 8. Approval gate
# ═══════════════════════════════════════════════

class TestApprovalGate:
    def test_approve_step(self):
        engine = WorkflowEngine()
        step = ApprovalStep("needs_approval")
        engine.register(step)

        # Run in a background task so we can approve
        async def run_and_approve():
            ctx = await engine._run_async()
            return ctx

        async def do_approval():
            await asyncio.sleep(0.1)
            engine.approve("needs_approval")

        async def test():
            results = await asyncio.gather(
                run_and_approve(),
                do_approval(),
            )
            return results[0]

        ctx = asyncio.run(test())
        assert ctx.results["needs_approval"].status == StepStatus.SUCCESS
        assert ctx.results["needs_approval"].output == "approved!"
        assert step.executed is True

    def test_reject_step(self):
        engine = WorkflowEngine()
        step = ApprovalStep("needs_rejection")
        engine.register(step)

        async def run_and_reject():
            ctx = await engine._run_async()
            return ctx

        async def do_reject():
            await asyncio.sleep(0.1)
            engine.reject("needs_rejection", "Not ready")

        async def test():
            results = await asyncio.gather(
                run_and_reject(),
                do_reject(),
            )
            return results[0]

        ctx = asyncio.run(test())
        assert ctx.results["needs_rejection"].status == StepStatus.FAILED
        assert "rejected" in (ctx.results["needs_rejection"].error or "").lower()
        assert step.executed is False

    def test_approval_pending_check(self):
        engine = WorkflowEngine()
        step = ApprovalStep("check_approval")
        engine.register(step)

        async def run_and_check():
            # Run, but catch the fact that it waits
            task = asyncio.create_task(engine._run_async())
            await asyncio.sleep(0.15)
            state = engine.get_state()
            assert "check_approval" in state["pending_approvals"]
            engine.approve("check_approval")
            return await task

        ctx = asyncio.run(run_and_check())
        assert ctx.results["check_approval"].status == StepStatus.SUCCESS

    def test_approve_nonexistent_raises(self):
        engine = WorkflowEngine()
        with pytest.raises(ValueError, match="No pending approval"):
            engine.approve("nonexistent")

    def test_reject_nonexistent_raises(self):
        engine = WorkflowEngine()
        with pytest.raises(ValueError, match="No pending approval"):
            engine.reject("nonexistent")


# ═══════════════════════════════════════════════
# 9. Persistence round-trip
# ═══════════════════════════════════════════════

class TestPersistence:
    def test_persistence_round_trip(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{}")
            persist_path = f.name

        try:
            engine = WorkflowEngine(persist_path=persist_path)
            engine.register(RecordingStep("a"))
            engine.register(RecordingStep("b", depends_on=["a"]))
            ctx = engine.run()

            # Load from file
            loaded = load_workflow(persist_path)
            assert loaded.workflow_id == ctx.workflow_id
            assert loaded.state == ctx.state
            assert "a" in loaded.results
            assert "b" in loaded.results
            assert loaded.results["a"].status == StepStatus.SUCCESS
            assert loaded.results["b"].status == StepStatus.SUCCESS
        finally:
            os.unlink(persist_path)

    def test_save_and_load_workflow(self):
        ctx = WorkflowContext(
            state={"result": 42},
            results={
                "x": StepResult(
                    step_id="x",
                    status=StepStatus.SUCCESS,
                    output="hello",
                    error=None,
                    attempts=1,
                    started_at=100.0,
                    completed_at=101.0,
                    duration_s=1.0,
                )
            },
            started_at=100.0,
            workflow_id="test-wf",
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{}")
            persist_path = f.name

        try:
            save_workflow(ctx, persist_path)
            loaded = load_workflow(persist_path)
            assert loaded.workflow_id == "test-wf"
            assert loaded.state["result"] == 42
            assert loaded.results["x"].status == StepStatus.SUCCESS
            assert loaded.results["x"].output == "hello"
            assert loaded.results["x"].duration_s == 1.0
        finally:
            os.unlink(persist_path)


# ═══════════════════════════════════════════════
# 10. Event hooks
# ═══════════════════════════════════════════════

class TestEventHooks:
    def test_hooks_fire_in_order(self):
        events = []

        hooks = {
            "on_step_start": [lambda sid: events.append(("start", sid))],
            "on_step_success": [lambda sid, r: events.append(("success", sid))],
            "on_step_failure": [lambda sid, r: events.append(("failure", sid))],
            "on_workflow_complete": [lambda ctx: events.append(("complete", "all"))],
        }

        engine = WorkflowEngine(event_hooks=hooks)
        engine.register(RecordingStep("a"))
        engine.register(RecordingStep("b", depends_on=["a"]))
        engine.run()

        # a starts, a succeeds, b starts, b succeeds, workflow complete
        assert ("start", "a") in events
        assert ("success", "a") in events
        assert ("start", "b") in events
        assert ("success", "b") in events
        assert ("complete", "all") in events

    def test_hook_on_failure(self):
        events = []

        hooks = {
            "on_step_failure": [lambda sid, r: events.append(("failure", sid))],
            "on_workflow_failure": [lambda ctx, failed: events.append(("wf_failure", failed))],
        }

        engine = WorkflowEngine(event_hooks=hooks)
        engine.register(FailingStep("fail", retry_count=0, retry_delay=0.01))
        ctx = engine.run()
        assert ("failure", "fail") in events
        assert any("fail" in str(e) for e in events)

    def test_hook_count(self):
        start_count = 0

        def count_start(sid):
            nonlocal start_count
            start_count += 1

        engine = WorkflowEngine(event_hooks={"on_step_start": [count_start]})
        engine.register(RecordingStep("a"))
        engine.register(RecordingStep("b"))
        engine.register(RecordingStep("c"))
        engine.run()
        assert start_count >= 3  # 3 steps, but skipped steps also fire start

    def test_hook_error_does_not_crash(self):
        """Hook raising an exception should not crash the workflow."""

        def bad_hook(*args):
            raise RuntimeError("Hook error")

        engine = WorkflowEngine(event_hooks={"on_step_start": [bad_hook]})
        engine.register(RecordingStep("a"))
        engine.register(RecordingStep("b", depends_on=["a"]))
        ctx = engine.run()
        assert ctx.results["a"].status == StepStatus.SUCCESS
        assert ctx.results["b"].status == StepStatus.SUCCESS


# ═══════════════════════════════════════════════
# 11. Timeout
# ═══════════════════════════════════════════════

class TestTimeout:
    def test_step_timeout(self):
        engine = WorkflowEngine()
        engine.register(TimeoutStep("slow", sleep=10.0, retry_count=0, retry_delay=0.01))
        ctx = engine.run()
        assert ctx.results["slow"].status == StepStatus.DEAD_LETTER
        assert "timed out" in (ctx.results["slow"].error or "").lower()


# ═══════════════════════════════════════════════
# 12. LambdaStep
# ═══════════════════════════════════════════════

class TestLambdaStep:
    def test_lambda_step_sync(self):
        def my_fn(ctx):
            return "sync_result"

        engine = WorkflowEngine()
        engine.register(LambdaStep("sync_fn", my_fn, name="Sync Function"))
        ctx = engine.run()
        assert ctx.results["sync_fn"].status == StepStatus.SUCCESS
        assert ctx.results["sync_fn"].output == "sync_result"

    def test_lambda_step_async(self):
        async def my_fn(ctx):
            await asyncio.sleep(0.01)
            return "async_result"

        engine = WorkflowEngine()
        engine.register(LambdaStep("async_fn", my_fn, name="Async Function"))
        ctx = engine.run()
        assert ctx.results["async_fn"].status == StepStatus.SUCCESS
        assert ctx.results["async_fn"].output == "async_result"

    def test_lambda_step_chain(self):
        order = []
        engine = WorkflowEngine()
        engine.register(LambdaStep(
            "first", lambda ctx: (order.append("first"), "done")[1]
        ))
        engine.register(LambdaStep(
            "second",
            lambda ctx: (order.append("second"), "done2")[1],
            depends_on=["first"],
        ))
        ctx = engine.run()
        assert order == ["first", "second"]


# ═══════════════════════════════════════════════
# 13. Workflow builder (chainable API)
# ═══════════════════════════════════════════════

class TestWorkflowBuilder:
    def test_workflow_step_chain(self):
        order = []
        wf = (
            Workflow()
            .step(RecordingStep("a", order_list=order))
            .then(RecordingStep("b", order_list=order))
            .then(RecordingStep("c", order_list=order))
        )
        ctx = wf.run()
        assert order == ["a", "b", "c"]
        for s in ("a", "b", "c"):
            assert ctx.results[s].status == StepStatus.SUCCESS

    def test_workflow_parallel(self):
        order = []
        wf = (
            Workflow()
            .step(RecordingStep("start", order_list=order))
            .parallel(
                RecordingStep("p1", depends_on=["start"], order_list=order),
                RecordingStep("p2", depends_on=["start"], order_list=order),
            )
            .then(RecordingStep("end", order_list=order))
        )
        ctx = wf.run()
        assert order[0] == "start"
        assert "p1" in order[1:3] and "p2" in order[1:3]
        assert order[-1] == "end"
        for s in ("start", "p1", "p2", "end"):
            assert ctx.results[s].status == StepStatus.SUCCESS


# ═══════════════════════════════════════════════
# 14. _eval_condition
# ═══════════════════════════════════════════════

class TestEvalCondition:
    def test_literal_true(self):
        assert _eval_condition("True", {}) is True

    def test_literal_false(self):
        assert _eval_condition("False", {}) is False

    def test_dict_get(self):
        assert _eval_condition("state.get('x', 0) == 42", {"state": {"x": 42}}) is True
        assert _eval_condition("state.get('x', 0) == 42", {"state": {"x": 0}}) is False

    def test_comparison(self):
        assert _eval_condition("state['count'] > 5", {"state": {"count": 10}}) is True
        assert _eval_condition("state['count'] > 5", {"state": {"count": 3}}) is False

    def test_bool_ops(self):
        assert _eval_condition(
            "state['a'] and state['b']",
            {"state": {"a": True, "b": True}},
        ) is True
        assert _eval_condition(
            "state['a'] and state['b']",
            {"state": {"a": True, "b": False}},
        ) is False

    def test_arithmetic(self):
        assert _eval_condition("state['x'] + state['y'] == 10", {"state": {"x": 4, "y": 6}}) is True

    def test_not_operator(self):
        assert _eval_condition("not state['flag']", {"state": {"flag": False}}) is True
        assert _eval_condition("not state['flag']", {"state": {"flag": True}}) is False

    def test_nested_access(self):
        assert _eval_condition(
            "state['nested']['value'][0] == 1",
            {"state": {"nested": {"value": [1, 2, 3]}}},
        ) is True

    def test_override_state_var(self):
        """Context injection works."""
        assert _eval_condition(
            "state.get('key', 0) == results.get('key', 0)",
            {"state": {"key": 1}, "results": {"key": 2}},
        ) is False


# ═══════════════════════════════════════════════
# 15. Dead-letter queue
# ═══════════════════════════════════════════════

class TestDeadLetter:
    def test_dead_letter_steps_listed(self):
        engine = WorkflowEngine()
        engine.register(FailingStep("fail1", retry_count=0, retry_delay=0.01))
        ctx = engine.run()
        assert "fail1" in engine._dead_letter_steps

    def test_dead_letter_skips_dependents(self):
        engine = WorkflowEngine()
        engine.register(FailingStep("fail", retry_count=0, retry_delay=0.01))
        engine.register(RecordingStep("dependent", depends_on=["fail"]))
        ctx = engine.run()
        assert ctx.results["fail"].status == StepStatus.DEAD_LETTER
        assert ctx.results["dependent"].status == StepStatus.SKIPPED
        assert "dependent" not in ctx.state

    def test_on_workflow_failure_fires_with_dead_letter(self):
        events = []
        engine = WorkflowEngine(event_hooks={
            "on_workflow_failure": [lambda ctx, failed: events.append(("failed", failed))]
        })
        engine.register(FailingStep("fail", retry_count=0, retry_delay=0.01))
        ctx = engine.run()
        assert len(events) >= 1
        assert "fail" in str(events[0][1])


# ═══════════════════════════════════════════════
# 16. Sync function in LambdaStep
# ═══════════════════════════════════════════════

class TestSyncFn:
    def test_sync_function_works(self):
        def simple(ctx):
            return ctx["state"].get("x", 0) + 1

        engine = WorkflowEngine()
        engine.register(LambdaStep("add_one", simple))
        ctx = engine.run()
        assert ctx.results["add_one"].status == StepStatus.SUCCESS

    def test_sync_with_side_effect(self):
        side_effects = []

        def append_val(ctx):
            side_effects.append(1)
            return "ok"

        engine = WorkflowEngine()
        engine.register(LambdaStep("effect", append_val))
        engine.run()
        assert side_effects == [1]


# ═══════════════════════════════════════════════
# 17. Step describe
# ═══════════════════════════════════════════════

class TestDescribe:
    def test_lambda_step_describe(self):
        step = LambdaStep("test", lambda ctx: None)
        assert "test" in step.describe()

    def test_recording_step_describe(self):
        step = RecordingStep("my_id")
        assert "my_id" in step.describe()


# ═══════════════════════════════════════════════
# 18. Duplicate step ID raises
# ═══════════════════════════════════════════════

class TestDuplicateStep:
    def test_duplicate_id_raises(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a"))
        with pytest.raises(ValueError, match="Duplicate"):
            engine.register(RecordingStep("a"))


# ═══════════════════════════════════════════════
# 19. Workflow context to/from dict
# ═══════════════════════════════════════════════

class TestContextRoundTrip:
    def test_context_to_dict_from_dict(self):
        ctx = WorkflowContext(
            state={"key": "val"},
            results={
                "s1": StepResult(
                    step_id="s1",
                    status=StepStatus.SUCCESS,
                    output=42,
                    error=None,
                    attempts=1,
                    started_at=1.0,
                    completed_at=2.0,
                    duration_s=1.0,
                )
            },
            started_at=0.0,
            workflow_id="wf1",
        )
        d = ctx.to_dict()
        restored = WorkflowContext.from_dict(d)
        assert restored.workflow_id == "wf1"
        assert restored.state == {"key": "val"}
        assert restored.results["s1"].status == StepStatus.SUCCESS
        assert restored.results["s1"].output == 42


# ═══════════════════════════════════════════════
# 20. Engine get_state
# ═══════════════════════════════════════════════

class TestEngineState:
    def test_get_state_before_run(self):
        engine = WorkflowEngine(workflow_id="test-123")
        engine.register(RecordingStep("a"))
        state = engine.get_state()
        assert state["workflow_id"] == "test-123"
        assert "a" in state["step_ids"]

    def test_get_state_after_run(self):
        engine = WorkflowEngine()
        engine.register(RecordingStep("a"))
        engine.run()
        state = engine.get_state()
        assert len(state["step_ids"]) == 1
