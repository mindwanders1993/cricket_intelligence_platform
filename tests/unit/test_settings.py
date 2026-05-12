# tests/unit/test_settings.py
"""
Guard-rail tests for PlatformSettings.
These tests exist to catch the exact class of bugs fixed on 2026-05-11:
  - localhost leakage into Docker service name positions
  - _REPO_ROOT resolving to wrong path when CIP_REPO_ROOT is set
  - DSN construction producing malformed connection strings
  - lru_cache serving stale settings after env var changes
"""

import importlib

import pytest

import cip.common.settings as s


@pytest.fixture(autouse=True)
def clear_settings_cache():
    """Always start each test with a clean settings cache."""
    s.invalidate_settings_cache()
    yield
    s.invalidate_settings_cache()


# ---------------------------------------------------------------------------
# 1. _REPO_ROOT resolution
# ---------------------------------------------------------------------------


class TestRepoRootResolution:
    def test_repo_root_uses_env_var_when_set(self, tmp_path, monkeypatch):
        """CIP_REPO_ROOT env var must take priority over __file__ resolution."""
        monkeypatch.setenv("CIP_REPO_ROOT", str(tmp_path))
        # Re-import to pick up the env var (settings module caches _REPO_ROOT at import time)
        importlib.reload(s)
        assert s._REPO_ROOT == tmp_path

    def test_repo_root_parents3_points_to_repo(self):
        """Without CIP_REPO_ROOT, parents[3] must resolve to the actual repo root."""
        assert (s._REPO_ROOT / "pyproject.toml").exists(), (
            f"_REPO_ROOT={s._REPO_ROOT} does not contain pyproject.toml. "
            "parents[3] count is wrong — update after any directory restructure."
        )

    def test_env_file_is_found(self):
        """_ENV_FILE must point to an existing .env or .env.example."""
        env_exists = s._ENV_FILE.exists() or (s._REPO_ROOT / ".env.example").exists()
        assert env_exists, f"Neither .env nor .env.example found at {s._REPO_ROOT}"


# ---------------------------------------------------------------------------
# 2. Docker service name defaults — the "localhost leakage" guard
# ---------------------------------------------------------------------------


class TestDockerServiceNameDefaults:
    """
    These tests assert that when NO env vars are set (simulating a fresh
    Docker container with only compose env injected), the settings defaults
    are safe Docker service names — NOT localhost.

    If any of these fail, a container will crash on startup.
    """

    @pytest.fixture(autouse=True)
    def isolate_from_env_file(self, tmp_path, monkeypatch):
        """Ensure these tests don't pick up the local .env file."""
        monkeypatch.setenv("CIP_REPO_ROOT", str(tmp_path))
        importlib.reload(s)
        yield

    def test_postgres_default_host_is_not_localhost(self, monkeypatch):
        """PostgresSettings.host default must be a Docker service name, not localhost."""
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        cfg = s.get_settings()
        assert cfg.postgres.host != "localhost", (
            "POSTGRES_HOST defaults to 'localhost'. Inside Docker containers this "
            "resolves to the container itself, not the postgres service. "
            "Change default to 'postgres' or ensure compose always injects POSTGRES_HOST."
        )

    def test_minio_endpoint_default_is_not_localhost(self, monkeypatch):
        monkeypatch.delenv("MINIO_S3_ENDPOINT", raising=False)
        monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
        cfg = s.get_settings()
        assert (
            "localhost" not in cfg.storage.endpoint
        ), "MinIO endpoint defaults to localhost. Containers cannot reach MinIO this way."

    def test_iceberg_rest_uri_default_is_not_localhost(self, monkeypatch):
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        cfg = s.get_settings()
        assert (
            "localhost" not in cfg.iceberg.rest_uri
        ), "Iceberg REST URI defaults to localhost. The iceberg-rest container will fail."

    def test_mlflow_tracking_uri_default_is_not_localhost(self, monkeypatch):
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        cfg = s.get_settings()
        assert (
            "localhost" not in cfg.mlflow.tracking_uri
        ), "MLflow tracking URI defaults to localhost. MLflow container will fail."


# ---------------------------------------------------------------------------
# 3. DSN construction correctness
# ---------------------------------------------------------------------------


class TestDSNConstruction:
    def test_postgres_dsn_format(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "postgres")
        monkeypatch.setenv("POSTGRES_USER", "cricket_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "cricket_pass")
        monkeypatch.setenv("POSTGRES_DB", "cricket_platform")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        cfg = s.get_settings()
        dsn = cfg.postgres.dsn
        assert dsn.startswith("postgresql+psycopg2://"), f"Wrong driver prefix: {dsn}"
        assert "@postgres:5432/" in dsn, f"Service name not in DSN: {dsn}"
        assert "cricket_platform" in dsn, f"DB name missing from DSN: {dsn}"
        assert "cricket_pass" in dsn, "Password not in DSN"

    def test_postgres_dsn_does_not_contain_localhost_when_host_is_postgres(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "postgres")
        cfg = s.get_settings()
        assert "localhost" not in cfg.postgres.dsn

    def test_async_dsn_uses_asyncpg_driver(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "postgres")
        cfg = s.get_settings()
        assert cfg.postgres.async_dsn.startswith("postgresql+asyncpg://")


# ---------------------------------------------------------------------------
# 4. env var override priority
# ---------------------------------------------------------------------------


class TestEnvVarOverridePriority:
    def test_postgres_host_overridden_by_env(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "my-custom-postgres-host")
        cfg = s.get_settings()
        assert cfg.postgres.host == "my-custom-postgres-host"

    def test_minio_endpoint_overridden_by_minio_s3_endpoint(self, monkeypatch):
        monkeypatch.setenv("MINIO_S3_ENDPOINT", "http://minio:9000")
        cfg = s.get_settings()
        assert cfg.storage.endpoint == "http://minio:9000"

    def test_lru_cache_does_not_serve_stale_settings(self, monkeypatch):
        """Verify invalidate_settings_cache() actually resets between calls."""
        monkeypatch.setenv("POSTGRES_HOST", "host-a")
        cfg1 = s.get_settings()
        assert cfg1.postgres.host == "host-a"

        s.invalidate_settings_cache()
        monkeypatch.setenv("POSTGRES_HOST", "host-b")
        cfg2 = s.get_settings()
        assert cfg2.postgres.host == "host-b", (
            "lru_cache served stale settings after invalidation. "
            "Always call invalidate_settings_cache() before re-reading settings in tests."
        )

    def test_env_name_normalised_to_lowercase(self, monkeypatch):
        monkeypatch.setenv("ENV_NAME", "DEV")
        cfg = s.get_settings()
        assert cfg.env_name == "dev"


# ---------------------------------------------------------------------------
# 5. S3 path properties
# ---------------------------------------------------------------------------


class TestStoragePathProperties:
    def test_landing_register_csv_path(self):
        cfg = s.get_settings()
        assert cfg.storage.landing_register_csv.startswith("s3://cricket-landing/")

    def test_iceberg_warehouse_uri_ends_with_slash(self):
        cfg = s.get_settings()
        assert cfg.storage.iceberg_warehouse_uri.endswith(
            "/"
        ), "Iceberg warehouse URI must end with / for PyIceberg catalog resolution"
