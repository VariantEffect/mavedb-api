"""
Centralized model rebuilding for view models with circular dependencies.

This module handles the rebuilding of all Pydantic models that have forward references
to other models, resolving circular import issues by performing the rebuilds after
all modules have been imported.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
from pathlib import Path

from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _discover_and_sort_models():
    """Discover all Pydantic models and sort them by dependencies."""
    import mavedb.view_models

    view_models_path = Path(mavedb.view_models.__file__).parent
    models_by_module = {}

    # Discover all models grouped by module
    for module_info in pkgutil.walk_packages([str(view_models_path)], "mavedb.view_models."):
        module_name = module_info.name
        if module_name.endswith(".model_rebuild"):
            continue

        try:
            module = importlib.import_module(module_name)
            module_models = []

            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, BaseModel) and obj.__module__ == module_name and hasattr(obj, "model_rebuild"):
                    module_models.append((f"{module_name}.{name}", obj))

            if module_models:
                models_by_module[module_name] = module_models

        except ImportError as e:
            logger.warning("Could not import %s: %s", module_name, e)

    # Sort models within each module by dependency (base classes first)
    sorted_models = []
    for module_name, module_models in models_by_module.items():

        def dependency_count(item):
            _, model_class = item
            # Count base classes within the same module
            count = 0
            for base in model_class.__bases__:
                if issubclass(base, BaseModel) and base != BaseModel and any(base == mc for _, mc in module_models):
                    count += 1
            return count

        module_models.sort(key=dependency_count)
        sorted_models.extend(module_models)

    return sorted_models


def rebuild_all_models():
    """
    Rebuild all Pydantic models in the view_models package.

    Discovers models, sorts by dependencies, and rebuilds with multi-pass
    approach to achieve 0 circular dependencies.
    """
    # Discover and sort models by dependencies
    models_to_rebuild = _discover_and_sort_models()

    # Create registry for forward reference resolution
    model_registry = {name.split(".")[-1]: cls for name, cls in models_to_rebuild}

    logger.debug("Rebuilding %d Pydantic models...", len(models_to_rebuild))

    successful_rebuilds = 0
    remaining_models = models_to_rebuild[:]

    # Multi-pass rebuild to handle complex dependencies
    for pass_num in range(3):
        if not remaining_models:
            break

        models_for_next_pass = []

        for model_name, model_class in remaining_models:
            try:
                # Temporarily inject all models into the module for forward references
                module = importlib.import_module(model_class.__module__)
                injected = {}

                for simple_name, ref_class in model_registry.items():
                    if simple_name not in module.__dict__:
                        injected[simple_name] = module.__dict__.get(simple_name)
                        module.__dict__[simple_name] = ref_class

                try:
                    model_class.model_rebuild()
                    successful_rebuilds += 1
                    logger.debug("Rebuilt %s", model_name)
                finally:
                    # Restore original module state
                    for name, original in injected.items():
                        if original is None:
                            module.__dict__.pop(name, None)
                        else:
                            module.__dict__[name] = original

            except Exception as e:
                if "is not defined" in str(e) and pass_num < 2:
                    models_for_next_pass.append((model_name, model_class))
                    logger.debug("Deferring %s to next pass", model_name)
                else:
                    logger.error("Failed to rebuild %s: %s", model_name, e)

        remaining_models = models_for_next_pass

    logger.debug(
        "Rebuilt %d Pydantic models successfully, %d models with circular dependencies remain.",
        successful_rebuilds,
        len(remaining_models),
    )


# Automatically rebuild all models when this module is imported
rebuild_all_models()
