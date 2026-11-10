"""Regression tests for the local development stack in ``containers/``.

The Compose stack runs Jupyter Lab, which gives full code execution in the
container. These tests keep its ports on the host loopback interface and keep
Jupyter authentication enabled.
"""

import ast
from pathlib import Path

import pytest
import yaml

CONTAINERS_DIR = Path(__file__).resolve().parents[2] / "containers"
COMPOSE_FILE = CONTAINERS_DIR / "compose.yaml"
JUPYTER_CONFIG_FILE = CONTAINERS_DIR / "jupyterlab" / "config" / "jupyter_lab_config.py"

EXPECTED_PORTS = {
    "spark-notebooks": {"8888"},
    "mailpit": {"8025", "1025"},
    "kafka": {"9092"},
}

# Settings that disable or weaken authentication when set to these values.
AUTH_SETTINGS = {
    ("ServerApp", "token"),
    ("ServerApp", "password"),
    ("IdentityProvider", "token"),
    ("PasswordIdentityProvider", "hashed_password"),
}


def _parse_port(entry):
    """Return (host_ip, published_port) for a Compose port entry."""
    if isinstance(entry, dict):
        return entry.get("host_ip"), str(entry.get("published"))
    parts = str(entry).split(":")
    if len(parts) == 3:
        return parts[0], parts[1]
    return None, parts[0]


def _config_assignments():
    """Yield (section, option, value_node) for each ``c.<Section>.<option> = ...``."""
    tree = ast.parse(JUPYTER_CONFIG_FILE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Attribute)
                and isinstance(target.value.value, ast.Name)
                and target.value.value.id == "c"
            ):
                yield target.value.attr, target.attr, node.value


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))


@pytest.mark.parametrize("service", sorted(EXPECTED_PORTS))
def test_compose_ports_bind_to_loopback(compose, service):
    ports = compose["services"][service].get("ports", [])
    published = set()
    for entry in ports:
        host_ip, port = _parse_port(entry)
        assert host_ip == "127.0.0.1", f"{service} publishes {entry!r} on all host interfaces"
        published.add(port)
    assert published == EXPECTED_PORTS[service]


def test_every_published_port_binds_to_loopback(compose):
    for name, service in compose["services"].items():
        for entry in service.get("ports", []):
            host_ip, _ = _parse_port(entry)
            assert host_ip == "127.0.0.1", f"{name} publishes {entry!r} on all host interfaces"


def test_jupyter_command_does_not_disable_auth(compose):
    command = compose["services"]["spark-notebooks"].get("command", "")
    command = " ".join(command) if isinstance(command, list) else str(command)
    for flag in ("token=", "password=", "IdentityProvider", "disable_check_xsrf"):
        assert flag not in command, f"Jupyter command overrides {flag}"


def test_compose_does_not_mount_host_paths(compose):
    for name, service in compose["services"].items():
        assert not service.get("volumes"), f"{name} mounts host paths into the container"


def test_jupyter_config_does_not_disable_authentication():
    for section, option, value in _config_assignments():
        if (section, option) in AUTH_SETTINGS:
            assert not (isinstance(value, ast.Constant) and value.value in ("", None)), (
                f"c.{section}.{option} is set to an empty value, which disables authentication"
            )
        assert not (section == "ServerApp" and option == "disable_check_xsrf"), "XSRF checks are disabled"


def test_jupyter_config_does_not_allow_any_origin():
    for section, option, value in _config_assignments():
        if section == "ServerApp" and option in ("allow_origin", "allow_origin_pat"):
            assert not (isinstance(value, ast.Constant) and value.value in ("*", ".*")), (
                f"c.{section}.{option} allows requests from any origin"
            )
