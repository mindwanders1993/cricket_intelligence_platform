import importlib

import pytest
from cip.common.settings import invalidate_settings_cache


@pytest.fixture(autouse=True)
def clear_settings_cache():
    invalidate_settings_cache()
    yield
    invalidate_settings_cache()


@pytest.fixture(autouse=True)
def reload_settings_after_env_patch():
    """Re-evaluate module-level constants after any env mutation."""
    yield
    import cip.common.settings as s

    importlib.reload(s)
