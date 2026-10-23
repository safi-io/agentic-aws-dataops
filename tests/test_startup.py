"""Regression checks for launching from outside the repository root."""

import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest

from config.settings import Settings, clear_settings_cache, get_settings
from scripts import run


ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    for name in Settings.model_fields:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("DATAOPS_ENV_FILE", raising=False)
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    path = tmp_path / "chosen.env"
    path.write_text(
        "DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:5432/test\n"
        "S3_BUCKET_NAME=startup-test\n"
        "DECIDER_MODE=cli\n",
        encoding="utf-8",
    )
    clear_settings_cache()
    yield path
    clear_settings_cache()


def test_default_env_path_is_anchored_to_project():
    assert Settings.model_config["env_file"] == ROOT / ".env"


def test_selected_env_loads_from_another_directory(env_file, monkeypatch):
    monkeypatch.setenv("DATAOPS_ENV_FILE", str(env_file))
    monkeypatch.chdir(ROOT / "dashboard")
    settings = get_settings()
    assert settings.S3_BUCKET_NAME == "startup-test"
    assert settings.DECIDER_MODE == "cli"
    assert settings.AWS_ENDPOINT_URL is None
    assert settings.AWS_ACCESS_KEY_ID is None


def test_explicit_missing_env_does_not_fall_back_to_local(env_file, monkeypatch):
    monkeypatch.setenv("DATAOPS_ENV_FILE", str(env_file.parent / "missing.env"))
    with pytest.raises(FileNotFoundError, match="environment file not found"):
        get_settings()


def test_dagster_launcher_preserves_storage_and_selected_environment(env_file, monkeypatch):
    storage = env_file.parent / "dagster_home"
    monkeypatch.setenv("DAGSTER_HOME", str(storage))
    monkeypatch.setattr(sys, "argv", ["run.py", "dagster", "--env-file", str(env_file)])
    with patch.object(run.subprocess, "call", return_value=0) as launch:
        assert run.main() == 0
        call = launch.call_args
        assert call.kwargs["cwd"] == storage
        assert call.kwargs["env"]["DAGSTER_HOME"] == str(storage)
        assert call.kwargs["env"]["DATAOPS_ENV_FILE"] == str(env_file)
        assert call.kwargs["env"]["S3_BUCKET_NAME"] == "startup-test"
        assert str(ROOT / "workspace.yaml") in call.args[0]
        assert (storage / "dagster.yaml").is_file()
        # A subsequent launch must retain existing instance configuration/history.
        (storage / "history-marker").write_text("keep", encoding="utf-8")
        assert run.main() == 0
        assert (storage / "history-marker").read_text(encoding="utf-8") == "keep"


def test_workspace_loads_only_this_project():
    from dagster import Definitions
    from dagster._core.workspace.load import location_origins_from_yaml_paths

    origins = location_origins_from_yaml_paths([str(ROOT / "workspace.yaml")])
    assert len(origins) == 1
    target = origins[0].loadable_target_origin
    assert target.module_name == "dagster_project"
    assert target.attribute == "defs"
    from dagster_project import defs

    Definitions.validate_loadable(defs)
    assert defs.get_job_def("orders_data_quality_job") is not None
    assert defs.get_job_def("incident_diagnosis_job") is not None


def test_dashboard_imports_and_renders_without_project_on_pythonpath(env_file):
    # A fresh isolated interpreter reproduces Streamlit's script-directory import
    # path. Stub only database reads so this regression needs no running services.
    code = """
from unittest.mock import patch
from streamlit.testing.v1 import AppTest
with patch('sqlalchemy.orm.Session.execute') as query:
    query.return_value.scalars.return_value.all.return_value = []
    app = AppTest.from_file('app.py', default_timeout=30).run()
    assert not app.exception, [str(item.value) for item in app.exception]
    assert not app.error, [item.value for item in app.error]
    assert app.title[0].value == 'Agentic AWS DataOps'
    assert app.metric[0].value == '0'
"""
    child_env = os.environ.copy()
    child_env.pop("PYTHONPATH", None)
    child_env["DATAOPS_ENV_FILE"] = str(env_file)
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=ROOT / "dashboard",
        env=child_env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
