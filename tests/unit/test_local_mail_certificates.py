"""Development certificates must authenticate the hosts used by the notebooks."""
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def verify_tls_certificate(output, host, *, trust_ca=True):
    """Verify an in-memory TLS handshake without CLI-specific hostname flags."""
    client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_context.verify_flags |= ssl.VERIFY_X509_STRICT
    if trust_ca:
        client_context.load_verify_locations(cafile=str(output / "mailpit-ca.crt"))
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(str(output / "mailpit.crt"), str(output / "mailpit.key"))
    client_in, client_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    server_in, server_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    client = client_context.wrap_bio(client_in, client_out, server_hostname=host)
    server = server_context.wrap_bio(server_in, server_out, server_side=True)
    for _ in range(10):
        completed = 0
        for peer, outgoing, incoming in (
            (client, client_out, server_in), (server, server_out, client_in)
        ):
            try:
                peer.do_handshake()
                completed += 1
            except ssl.SSLWantReadError:
                pass
            incoming.write(outgoing.read())
        if completed == 2:
            return
    pytest.fail("TLS handshake did not complete")


@pytest.mark.parametrize("long_path", [False, True], ids=["short-path", "long-absolute-path"])
def test_generated_mail_certificate_names_and_trust(tmp_path, long_path):
    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("openssl is required")
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path)}
    output = tmp_path / "certs"
    if long_path:
        output = output / ("long-output-" + "a" * 90) / ("nested-" + "b" * 90) / ("c" * 90)
        assert output.is_absolute() and len(str(output)) > 256
    command = ["sh", str(ROOT / "containers/mailserver/generate-certs.sh"), str(output)]
    subprocess.run(command, env=env, check=True, capture_output=True)
    for host in ("mailpit", "localhost", "127.0.0.1"):
        verify_tls_certificate(output, host)
    with pytest.raises(ssl.SSLCertVerificationError):
        verify_tls_certificate(output, "unrelated.invalid")
    with pytest.raises(ssl.SSLCertVerificationError):
        verify_tls_certificate(output, "mailpit", trust_ca=False)
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


def test_generator_rejects_dangling_symlink(tmp_path):
    output = tmp_path / "certs"
    output.mkdir()
    target = tmp_path / "must-not-be-created"
    (output / "mailpit-ca.crt").symlink_to(target)
    result = subprocess.run(["sh", str(ROOT / "containers/mailserver/generate-certs.sh"), str(output)], capture_output=True)
    assert result.returncode != 0
    assert not target.exists()
    assert (output / "mailpit-ca.crt").is_symlink()


def test_partial_publication_is_rolled_back(tmp_path):
    if not shutil.which("openssl"):
        pytest.skip("openssl is required")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    real_python = shutil.which("python3")
    stub = bindir / "python3"
    stub.write_text('#!/bin/sh\ncase "$4" in */mailpit.crt) exit 1;; esac\nexec "' + real_python + '" "$@"\n')
    stub.chmod(0o755)
    output = tmp_path / "certs"
    env = {"PATH": str(bindir) + os.pathsep + os.environ["PATH"], "HOME": str(tmp_path)}
    result = subprocess.run(["sh", str(ROOT / "containers/mailserver/generate-certs.sh"), str(output)], env=env, capture_output=True)
    assert result.returncode != 0
    assert list(output.iterdir()) == []
    assert output.stat().st_mode & 0o777 == 0o755


@pytest.mark.parametrize("name", ["mailpit-ca.crt", "mailpit.crt", "mailpit.key"])
def test_publication_rejects_late_directory(tmp_path, name):
    if not shutil.which("openssl"):
        pytest.skip("openssl is required")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    real_python = shutil.which("python3")
    stub = bindir / "python3"
    stub.write_text(
        '#!/bin/sh\ncase "$4" in */' + name + ') mkdir "$4";; esac\n'
        + 'exec "' + real_python + '" "$@"\n'
    )
    stub.chmod(0o755)
    output = tmp_path / "certs"
    env = {"PATH": str(bindir) + os.pathsep + os.environ["PATH"], "HOME": str(tmp_path)}
    command = ["sh", str(ROOT / "containers/mailserver/generate-certs.sh"), str(output)]
    result = subprocess.run(command, env=env, capture_output=True)
    assert result.returncode != 0
    assert "Created development" not in result.stdout.decode()
    assert list(output.iterdir()) == [output / name]
    assert list((output / name).iterdir()) == []
    (output / name).rmdir()
    env["PATH"] = os.environ["PATH"]
    subprocess.run(command, env=env, check=True, capture_output=True)
