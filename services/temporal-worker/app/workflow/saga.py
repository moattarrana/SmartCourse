"""A Saga / rollback manager for Temporal workflows.

Register how to undo each step with add() after it succeeds; call rollback() to
run every registered compensation in reverse (LIFO) order if a later step fails.

Contract: compensation activities MUST be idempotent. rollback()'s retry policy
may invoke a compensation more than once, and the forward step it undoes may have
only partially completed, so running a compensation twice must be safe.

Deterministic and replay-safe: holds a plain list and calls activities by name.
"""
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

#a tiny container that remembers one undo step
@dataclass
class _Compensation:
    activity: str
    args: list[Any]
    timeout: timedelta


class CompensationError(Exception):
    """One or more compensations failed during rollback (inconsistent state)."""

    def __init__(self, failures: list[tuple[str, str]]) -> None:  #constructor: the code that runs when the error is created
        self.failures = failures
        names = ", ".join(name for name, _ in failures)
        super().__init__(f"{len(failures)} compensation(s) failed: {names}")


class RollbackManager:
    def __init__(
        self,
        max_attempts: int = 5,
        default_timeout: timedelta = timedelta(seconds=30),
    ) -> None:
        self._steps: list[_Compensation] = []   #this is the list of undos to run
        self._default_timeout = default_timeout
        self._retry = RetryPolicy(maximum_attempts=max_attempts)
        self._compensated = False #It remembers whether rollback has already run.

    def add(self, activity: str, args: list, timeout: timedelta | None = None) -> None:
        """Register undos"""
        self._steps.append(
            _Compensation(activity, list(args), timeout or self._default_timeout)
        )

    async def rollback(self, raise_on_failure: bool = False) -> None: #called when a step fails, to undo what already succeeded.
        """Run every registered compensation in reverse order, at most once.

        Best-effort: a failing compensation is logged and the rest still run.
        Set raise_on_failure=True to raise CompensationError if any failed
        (signals an inconsistent state needing attention). guarantees the undos run at most once.
        """
        if self._compensated:
            workflow.logger.warning("rollback() already ran; ignoring repeat call")
            return
        self._compensated = True

        failures: list[tuple[str, str]] = []   #list of failed undos
        for comp in reversed(self._steps): #LIFO order: the last step added is the first to be undone. 
            #This is important because some steps may depend on previous steps, 
            # so we want to undo them in reverse order to maintain consistency.
            try:
                workflow.logger.info("Compensating: %s", comp.activity)
                await workflow.execute_activity(
                    comp.activity,
                    args=comp.args,
                    start_to_close_timeout=comp.timeout,
                    retry_policy=self._retry,
                )
            except Exception as exc:  # best-effort: record and keep unwinding
                workflow.logger.error("Compensation %s failed: %s", comp.activity, exc)
                failures.append((comp.activity, str(exc)))

        if failures and raise_on_failure:
            raise CompensationError(failures)
