"""The cloud source package must never ship credentials or unrelated local data."""

from pathlib import Path
import tarfile

from scripts import deploy_team


def test_source_package_excludes_secrets_datasets_and_other_projects(tmp_path, monkeypatch):
    monkeypatch.setattr(deploy_team, "ROOT", tmp_path)
    monkeypatch.setattr(deploy_team, "STATE", tmp_path / ".deployment")
    files = {
        "requirements.txt": "boto3",
        "workspace.yaml": "load_from: []",
        "config/settings.py": "DEPLOY_THIS = True",
        "config/.env": "DO_NOT_SHIP_CONFIG_SECRET",
        ".env.aws.local": "DO_NOT_SHIP_AWS_SECRET",
        ".aws/credentials": "DO_NOT_SHIP_AWS_KEYS",
        "run_configs/example.yml": "ops: {}",
        "run_configs/.tmp_dagster_home/history/runs/private.db": "PRIVATE_RUN_DATA",
        "run_configs/.tmp_dagster_home/dagster.yaml": "PRIVATE_RUNTIME_CONFIG",
        "sample_data/orders.csv": "PRIVATE_DATASET",
        "friend-agentic-aws-dataops/app.py": "UNRELATED_PROJECT_SECRET",
    }
    for relative, content in files.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    archive, key = deploy_team.package_source()
    with tarfile.open(archive) as bundle:
        names = set(bundle.getnames())
        payload = b"\n".join(bundle.extractfile(member).read() for member in bundle if member.isfile())
    assert {"config/settings.py", "run_configs/example.yml"} <= names
    assert not any(token in payload for token in (
        b"DO_NOT_SHIP", b"PRIVATE_", b"UNRELATED_PROJECT",
    ))
    assert len(Path(key).name.removesuffix(".tar.gz")) == 64
