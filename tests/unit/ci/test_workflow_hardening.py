"""Static checks for GitHub Actions workflow hardening.

These tests read the workflow files only. They do not contact GitHub.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

WORKFLOW_DIR = Path(__file__).resolve().parents[3] / ".github" / "workflows"
WORKFLOWS = sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml"))
PINNED_ACTION = re.compile(r"^[\w.-]+/[\w./-]+@[0-9a-f]{40}$")
# Expression contexts whose values can be chosen by whoever triggers a run.
UNTRUSTED_CONTEXT = re.compile(r"\$\{\{[^}]*\b(inputs|github\.event|github\.head_ref|needs|steps)\b")


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _steps(workflow: dict):
    for job_name, job in workflow.get("jobs", {}).items():
        for step in job.get("steps", []):
            yield job_name, step


def test_workflows_found():
    names = {path.name for path in WORKFLOWS}
    assert {"onpush.yml", "publish-gh-pages.yml", "codeql-analysis.yml"} <= names


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_actions_are_pinned_to_commit_sha(path):
    for job_name, step in _steps(_load(path)):
        uses = step.get("uses")
        if uses is None or uses.startswith("./"):
            continue
        assert PINNED_ACTION.match(uses), f"{path.name}:{job_name} uses unpinned action {uses!r}"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_token_permissions_are_explicit(path):
    workflow = _load(path)
    assert workflow.get("permissions") == {}, f"{path.name} must default to no permissions"
    for job_name, job in workflow["jobs"].items():
        assert "permissions" in job, f"{path.name}:{job_name} must declare permissions"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_run_steps_do_not_interpolate_untrusted_expressions(path):
    for job_name, step in _steps(_load(path)):
        script = step.get("run")
        if script is None:
            continue
        assert not UNTRUSTED_CONTEXT.search(script), (
            f"{path.name}:{job_name} step {step.get('name')!r} interpolates an expression into a shell script; "
            "pass it through env instead"
        )


def test_docs_dispatch_version_is_validated_before_setup():
    workflow = _load(WORKFLOW_DIR / "publish-gh-pages.yml")
    validate = workflow["jobs"]["validate-version"]
    publish = workflow["jobs"]["publish-gh-pages"]
    assert publish["needs"] == "validate-version"
    assert validate["permissions"] == {"contents": "read"}
    validation_step = next(step for step in validate["steps"] if step.get("id") == "validate")
    assert validation_step["env"] == {"INPUT_VERSION": "${{ inputs.version }}"}
    assert 'validate_release_version.sh "$INPUT_VERSION"' in validation_step["run"]
    publish_step = publish["steps"][-1]
    assert 'make deploy-docs "version=$DOCS_VERSION"' in publish_step["run"]


def test_release_build_has_no_publish_credentials():
    workflow = _load(WORKFLOW_DIR / "onpush.yml")
    jobs = workflow["jobs"]
    build_text = yaml.safe_dump(jobs["build"])
    assert "secrets." not in build_text
    assert jobs["build"]["permissions"] == {"contents": "read"}
    publish = jobs["publish-pypi"]
    assert publish["permissions"] == {}
    assert not any("actions/checkout" in step.get("uses", "") for step in publish["steps"])
    assert jobs["publish-docs"]["needs"] == ["build", "publish-pypi"]


def test_kafka_archive_is_checksum_verified():
    workflow = _load(WORKFLOW_DIR / "onpush.yml")
    assert re.fullmatch(r"[0-9a-f]{128}", workflow["env"]["KAFKA_SHA512"])
    kafka_step = next(step for step in workflow["jobs"]["unittesting"]["steps"] if step.get("name") == "Setup Kafka")
    script = kafka_step["run"]
    assert "| tar" not in script
    assert script.index("sha512sum --check") < script.index("tar -xzf")


def test_forks_retain_coverage_without_upstream_upload():
    job = _load(WORKFLOW_DIR / "onpush.yml")["jobs"]["unittesting"]
    steps = job["steps"]
    tests = next(step for step in steps if step.get("name") == "Install, lint and test")
    assert "make cov" in tests["run"]
    assert not tests.get("continue-on-error", False)
    artifact = next(step for step in steps if step.get("name") == "Retain coverage report")
    assert artifact["uses"].startswith("actions/upload-artifact@")
    assert artifact["with"]["path"] == "coverage.xml"
    assert artifact["with"]["if-no-files-found"] == "error"
    assert "if" not in artifact
    upload = next(step for step in steps if step.get("name") == "Upload coverage reports to Codecov")
    assert upload["if"] == "github.repository == 'Nike-Inc/spark-expectations'"
    assert upload["with"]["fail_ci_if_error"] is True
    assert not upload.get("continue-on-error", False)
    assert steps.index(tests) < steps.index(artifact) < steps.index(upload)
