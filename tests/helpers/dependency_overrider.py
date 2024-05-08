from typing import Any, Callable, Mapping

from mavedb.server_main import app


class DependencyOverrider:
    """
    See: https://stackoverflow.com/questions/65259085/best-way-to-override-fastapi-dependencies-for-testing-with-a-different-dependenc,
         which has been re-implemented here and simplified for our application.

    TODO(#183) - We could eliminate this context manager by moving to an app factory.
    """

    def __init__(self, overrides: Mapping[Callable, Callable]) -> None:
        self.overrides = overrides
        self._old_overrides: dict[Callable, Callable] = {}

    def __enter__(self):
        for dep, new_dep in self.overrides.items():
            if dep in app.dependency_overrides:
                # Save existing overrides
                self._old_overrides[dep] = app.dependency_overrides[dep]
            app.dependency_overrides[dep] = new_dep

        return self

    def __exit__(self, *args: Any) -> None:
        for dep in self.overrides.keys():
            if dep in self._old_overrides:
                # Restore previous overrides
                app.dependency_overrides[dep] = self._old_overrides.pop(dep)
            else:
                # Just delete the entry
                del app.dependency_overrides[dep]
