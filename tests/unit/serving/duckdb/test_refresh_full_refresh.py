from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from cip.serving.duckdb.refresh import DuckDBRefresh


def _make_refresh() -> DuckDBRefresh:
    r = object.__new__(DuckDBRefresh)
    cfg = MagicMock()
    cfg.dbt.project_dir = Path("/fake/project")
    cfg.dbt.profiles_dir = Path("/fake/profiles")
    cfg.dbt.target = "dev"
    cfg.dbt.threads = 4
    r._cfg = cfg
    r._db_path = Path("/fake/cricket.duckdb")
    return r


def _mock_proc(returncode: int = 0) -> MagicMock:
    p = MagicMock()
    p.returncode = returncode
    p.stdout = ""
    p.stderr = ""
    return p


def test_run_dbt_default_does_not_include_full_refresh_flag():
    r = _make_refresh()
    with patch("subprocess.run", return_value=_mock_proc()) as mock_run:
        r.run_dbt(command="run")

    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "dbt"
    assert cmd[1] == "run"
    assert "--full-refresh" not in cmd


def test_run_dbt_full_refresh_true_adds_flag_before_profiles_dir():
    r = _make_refresh()
    with patch("subprocess.run", return_value=_mock_proc()) as mock_run:
        r.run_dbt(command="run", full_refresh=True)

    cmd = mock_run.call_args[0][0]
    assert "--full-refresh" in cmd
    assert cmd.index("--full-refresh") < cmd.index("--profiles-dir")
