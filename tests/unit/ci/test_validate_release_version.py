"""Regression tests for release-version validation used by docs publishing.

The docs workflows pass a version string to ``make deploy-docs``. These tests
check that the validation script and the Makefile target treat that value as
data: valid versions reach ``hatch`` as one literal value, and inputs with shell
or make metacharacters fail before ``hatch`` runs and never execute anything.

Every test uses a stub ``hatch`` on ``PATH`` and a temporary ``HOME``. Nothing
is published and no network access is needed.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github" / "ci" / "validate_release_version.sh"
MAKEFILE = REPO_ROOT / "makefile"

VALID_VERSIONS = ["v2.10.1", "2.10.1", "v0.8.0", "0.0.0", "v10.20.30", "V2.10.1", "main", "2.11.0a1", "2.11.0b2", "2.11.0rc1", "v2.10.1.dev3", "2.10.1.post1", "2.10.1+gabc", "2.11.0rc1.post2.dev3+build.1"]

# Each payload tries to create ``{marker}`` if it is ever evaluated.
INVALID_TEMPLATES = [
    "",
    "v2.10",
    "latest",
    "v02.1.0",
    "-v2.10.1",
    "--help",
    "v2.10.1 latest",
    " v2.10.1",
    "v2.10.1 ",
    "v2.10.1\t",
    "v2.10.1\n",
    "v2.10.1\r",
    "v2.10.1\nv2.10.2",
    "v2.10.1\ntouch {marker}",
    "v2.10.1; touch {marker}",
    "v2.10.1;touch {marker};#",
    "v2.10.1 && touch {marker}",
    "v2.10.1 | touch {marker}",
    "v2.10.1 > {marker}",
    "$(touch {marker})",
    "v2.10.1$(touch {marker})",
    "`touch {marker}`",
    "v2.10.1`touch {marker}`",
    "${{IFS}}touch${{IFS}}{marker}",
    "$(shell touch {marker})",
    "v2.10.1$(shell touch {marker})",
    "$$(touch {marker})",
    "$(eval $$(shell touch {marker}))",
    "'; touch {marker}; '",
    '"; touch {marker}; "',
    "v2.10.1\\",
    "v2.10.1#",
    "v2.10.1*",
    "../../v2.10.1",
    "1" * 40 + ".0.0",
]


def _write_stub_hatch(bin_dir: Path, record: Path) -> None:
    """Create a ``hatch`` stub that records how it was called."""
    stub = bin_dir / "hatch"
    stub.write_text(
        "#!/bin/sh\n"
        f"printf 'VERSION=%s\\n' \"$VERSION\" >> '{record}'\n"
        f"printf 'ARGS=%s\\n' \"$*\" >> '{record}'\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)


@pytest.fixture(name="sandbox")
def fixture_sandbox(tmp_path: Path) -> dict:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    record = tmp_path / "hatch-calls.txt"
    _write_stub_hatch(bin_dir, record)
    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '/usr/bin:/bin')}",
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_CACHE_HOME": str(home / ".cache"),
        "LC_ALL": "C",
        "GITHUB_TOKEN": "synthetic-token-not-real",
    }
    return {"tmp": tmp_path, "env": env, "record": record}


def _run_script(value: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(SCRIPT), value],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=30,
    )


def _run_make(args: list, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["make", "-s", "-f", str(MAKEFILE), "-C", str(REPO_ROOT), "deploy-docs", *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )


def _invalid_values(marker: Path) -> list:
    return [template.format(marker=marker) for template in INVALID_TEMPLATES]


@pytest.mark.parametrize("version", VALID_VERSIONS)
def test_script_accepts_release_versions(version, sandbox):
    result = _run_script(version, sandbox["env"])
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"{version}\n"


@pytest.mark.parametrize("index", range(len(INVALID_TEMPLATES)))
def test_script_rejects_invalid_versions_without_executing(index, sandbox):
    marker = sandbox["tmp"] / "pwned-script"
    value = _invalid_values(marker)[index]
    result = _run_script(value, sandbox["env"])
    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid release version" in result.stderr
    assert not marker.exists()


def test_script_requires_exactly_one_argument(sandbox):
    for argv in ([str(SCRIPT)], [str(SCRIPT), "v1.2.3", "v1.2.4"]):
        result = subprocess.run(argv, capture_output=True, text=True, env=sandbox["env"], check=False, timeout=30)
        assert result.returncode == 2
        assert result.stdout == ""


make_required = pytest.mark.skipif(shutil.which("make") is None, reason="make is not installed")


@make_required
@pytest.mark.parametrize("version", VALID_VERSIONS)
def test_make_deploy_docs_passes_valid_version_as_one_value(version, sandbox):
    result = _run_make([f"version={version}"], sandbox["env"])
    assert result.returncode == 0, result.stderr
    calls = sandbox["record"].read_text().splitlines()
    assert calls == [f"VERSION={version}", "ARGS=run dev.py3.12:deploy-docs"]


@make_required
@pytest.mark.parametrize("index", range(len(INVALID_TEMPLATES)))
def test_make_deploy_docs_rejects_invalid_version_before_hatch(index, sandbox):
    marker = sandbox["tmp"] / "pwned-make"
    value = _invalid_values(marker)[index]
    if value != value.lstrip(" \t"):
        pytest.skip("make strips leading blanks from command-line assignments; covered separately")
    result = _run_make([f"version={value}"], sandbox["env"])
    assert result.returncode != 0
    assert not sandbox["record"].exists(), "hatch must not run for an invalid version"
    assert not marker.exists(), "payload was executed"


@make_required
def test_make_strips_leading_blanks_then_validates(sandbox):
    # GNU make drops blanks after "=" in a command-line assignment, so the
    # value that reaches validation is the trimmed, valid version. The
    # workflows validate the raw input before calling make, so they still
    # reject it.
    result = _run_make(["version= v2.10.1"], sandbox["env"])
    assert result.returncode == 0, result.stderr
    assert sandbox["record"].read_text().splitlines()[0] == "VERSION=v2.10.1"


@make_required
def test_make_deploy_docs_rejects_version_from_environment(sandbox):
    marker = sandbox["tmp"] / "pwned-env"
    env = dict(sandbox["env"], version=f"$(shell touch {marker})")
    result = _run_make([], env)
    assert result.returncode != 0
    assert not sandbox["record"].exists()
    assert not marker.exists()


@make_required
def test_make_deploy_docs_requires_version(sandbox):
    result = _run_make([], sandbox["env"])
    assert result.returncode != 0
    assert not sandbox["record"].exists()
