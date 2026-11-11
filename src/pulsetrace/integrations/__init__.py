"""Integrations hook PulseTrace into libraries the application already uses.

PulseTrace never patches anything implicitly. An integration is active only if
it was passed to ``PulseTrace(integrations=[...])`` (or, for the ASGI
middleware, added to the app), and ``shutdown()`` undoes every patch.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pulsetrace.client import PulseTrace


class Integration(ABC):
    identifier: ClassVar[str]

    @abstractmethod
    def setup(self, client: PulseTrace) -> None:
        """Install hooks. Called once from ``PulseTrace.__init__``."""

    def teardown(self) -> None:  # noqa: B027 - optional hook
        """Remove hooks. Called from ``PulseTrace.shutdown()``."""

    def __repr__(self) -> str:
        return f"<{type(self).__name__}>"
