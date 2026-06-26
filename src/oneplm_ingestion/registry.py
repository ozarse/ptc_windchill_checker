"""Registry for Python check functions (the escape hatch for complex checks).

A check defined in checks.json with ``"kind": "python"`` and ``"function": "<name>"``
dispatches to the function registered here under ``<name>``. Each function takes
an open sqlite3 connection and returns a list of CheckResult objects.

To add a Python check:

    from oneplm_ingestion.registry import register_check

    @register_check("my_check")
    def my_check(conn) -> list[CheckResult]:
        ...

Then reference it from checks.json:

    { "name": "My Check", "kind": "python", "function": "my_check" }

Modules holding registered functions must be imported for registration to take
effect; ``load_builtin_checks`` imports the ones shipped with the project.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable

from oneplm_ingestion.models import CheckResult

CheckFn = Callable[[object], list[CheckResult]]

CHECK_REGISTRY: dict[str, CheckFn] = {}

# Modules that register check functions on import.
_BUILTIN_MODULES = ("oneplm_ingestion.content_checks",)

_builtins_loaded = False


def register_check(name: str) -> Callable[[CheckFn], CheckFn]:
    """Decorator: register a check function under ``name``."""

    def decorator(fn: CheckFn) -> CheckFn:
        CHECK_REGISTRY[name] = fn
        return fn

    return decorator


def load_builtin_checks() -> None:
    """Import builtin modules so their @register_check functions are registered."""
    global _builtins_loaded
    if _builtins_loaded:
        return
    for mod in _BUILTIN_MODULES:
        importlib.import_module(mod)
    _builtins_loaded = True


def get_check_function(name: str) -> CheckFn | None:
    """Look up a registered check function, loading builtins first."""
    load_builtin_checks()
    return CHECK_REGISTRY.get(name)
