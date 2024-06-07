"""Tests for UWS exception classes."""

from __future__ import annotations

import pickle

from vocutouts.uws.exceptions import (
    TaskError,
    TaskFatalError,
    TaskTransientError,
    TaskUserError,
)
from vocutouts.uws.models import ErrorCode


def test_pickle() -> None:
    """Test that the `TaskError` exceptions can be pickled.

    Some care has to be taken in defining exceptions to ensure that they can
    be pickled and unpickled, since `BaseException` provides a ``__reduce__``
    implementation with somewhat unexpected properties. Make sure that this
    support doesn't regress, since arq uses pickle to convey errors from
    backend workers to anything that recovers their results.
    """

    def nonnegative(arg: int) -> int:
        if arg < 0:
            raise ValueError("Negative integers not supported")
        return arg

    def raise_exception(arg: int, exc_class: type[TaskError]) -> None:
        try:
            nonnegative(arg)
        except Exception as e:
            raise exc_class(
                ErrorCode.ERROR,
                "some message",
                "some detail",
                add_traceback=True,
            ) from e

    for exc_class in (
        TaskError,
        TaskFatalError,
        TaskTransientError,
        TaskUserError,
    ):
        exc: TaskError = exc_class(ErrorCode.ERROR, "some message", "detail")
        pickled_exc = pickle.loads(pickle.dumps(exc))
        assert exc.to_job_error() == pickled_exc.to_job_error()

        # Try with tracebacks.
        try:
            raise_exception(-1, exc_class)
        except TaskError as e:
            exc = e
        assert exc.traceback
        assert "nonnegative" in exc.traceback
        assert "TaskError" not in exc.traceback
        job_error = exc.to_job_error()
        assert job_error.detail
        assert "some detail\n\n" in job_error.detail
        assert "nonnegative" in job_error.detail
        pickled_exc = pickle.loads(pickle.dumps(exc))
        assert exc.to_job_error() == pickled_exc.to_job_error()
