"""PulseTrace: an async-first observability SDK for Python services.

Monitoring must never become a source of failure for the software it monitors.
"""

from pulsetrace._version import __version__
from pulsetrace.client import PulseTrace
from pulsetrace.config import ConfigurationError, Options
from pulsetrace.context import bind_context, suppress_instrumentation
from pulsetrace.integrations.asgi import PulseTraceMiddleware
from pulsetrace.scope import Scope
from pulsetrace.tracing import Span, TraceParent
from pulsetrace.transport.base import ShutdownReport

__all__ = [
    "ConfigurationError",
    "Options",
    "PulseTrace",
    "PulseTraceMiddleware",
    "Scope",
    "ShutdownReport",
    "Span",
    "TraceParent",
    "__version__",
    "bind_context",
    "suppress_instrumentation",
]
