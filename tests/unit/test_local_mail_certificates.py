"""Development certificates must authenticate the hosts used by the notebooks."""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_generated_mail_certificate_names_and_trust(tmp_path):
    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("openssl is required")
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path)}
    output = tmp_path / "certs"
    command = ["sh", str(ROOT / "containers/mailserver/generate-certs.sh"), str(output)]
    subprocess.run(command, env=env, check=True, capture_output=True)
    for host in ("mailpit", "localhost"):
        subprocess.run([openssl, "verify", "-CAfile", str(output / "mailpit-ca.crt"),
                        "-verify_hostname", host, str(output / "mailpit.crt")],
                       env=env, check=True, capture_output=True)
    subprocess.run([openssl, "verify", "-CAfile", str(output / "mailpit-ca.crt"),
                    "-verify_ip", "127.0.0.1", str(output / "mailpit.crt")],
                   env=env, check=True, capture_output=True)
    wrong = subprocess.run([openssl, "verify", "-CAfile", str(output / "mailpit-ca.crt"),
                            "-verify_hostname", "unrelated.invalid", str(output / "mailpit.crt")],
                           env=env, capture_output=True)
    assert wrong.returncode != 0
    assert {p.name for p in output.iterdir()} == {"mailpit-ca.crt", "mailpit.crt", "mailpit.key"}
    assert output.joinpath("mailpit.key").stat().st_mode & 0o077 == 0
    original = output.joinpath("mailpit.crt").read_bytes()
    assert subprocess.run(command, env=env, capture_output=True).returncode != 0
    assert output.joinpath("mailpit.crt").read_bytes() == original


def test_notebooks_explicitly_trust_local_mail_ca():
    for name in ("spark_expectation_basic_mail.ipynb", "spark_expectation_basic_mail_templates.ipynb"):
        notebook = json.loads((ROOT / "examples/notebooks" / name).read_text())
        sources = ["".join(cell.get("source", [])) for cell in notebook["cells"]]
        configured = [source for source in sources if "mailpit-ca.crt" in source]
        assert len(configured) == 1
        compile(configured[0], name, "exec")
        assert '"spark.expectations.notifications.email.smtp.ca.file"' in configured[0]


def test_notebook_and_kafka_images_do_not_copy_private_keys():
    assert "containers/certs/*.key" in (ROOT / "containers/jupyterlab/Dockerfile.notebook.dockerignore").read_text()
    assert "COPY containers/certs/*.crt " in (ROOT / "containers/kafka/Dockerfile.kafka").read_text()
