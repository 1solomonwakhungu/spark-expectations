"""
Regression tests for SMTP transport security in the email notification plugin.

A loopback-only SMTP server with throwaway certificates (generated with the ``openssl`` CLI into a
temporary directory) checks that the plugin verifies the server certificate and host name before it
sends credentials, that a custom CA bundle works, and that unresponsive servers time out.
No external host is contacted and no real credentials are used.
"""

import base64
import os
import shutil
import socket
import ssl
import subprocess
import threading
import time
from types import SimpleNamespace
from typing import List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsEmailException, SparkExpectationsMiscException
from spark_expectations.notifications.plugins.email import SparkExpectationsEmailPluginImpl

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl CLI is required")

SYNTHETIC_USER = "synthetic-user@example.invalid"
SYNTHETIC_PASSWORD = "synthetic-password-0000-not-a-secret"
LOOPBACK = "127.0.0.1"

_OPENSSL_CONFIG = """
[ ca ]
default_ca = test_ca

[ test_ca ]
dir = {dir}
database = $dir/index.txt
new_certs_dir = $dir/newcerts
serial = $dir/serial
certificate = $dir/ca.crt
private_key = $dir/ca.key
default_md = sha256
policy = policy_any
unique_subject = no
copy_extensions = none
default_days = 30

[ policy_any ]
commonName = supplied

[ req ]
distinguished_name = dn
prompt = no

[ dn ]
CN = placeholder

[ v3_ca ]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always

[ leaf_loopback ]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
subjectAltName = DNS:localhost,IP:127.0.0.1

[ leaf_wrong_host ]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
subjectAltName = DNS:wrong-host.example.invalid
"""


def _openssl(*args: str) -> None:
    subprocess.run(["openssl", *args], check=True, capture_output=True, timeout=60)


class _TestCA:
    """A throwaway certificate authority that lives in a temporary directory."""

    def __init__(self, directory: str, name: str) -> None:
        self.dir = directory
        os.makedirs(os.path.join(directory, "newcerts"))
        with open(os.path.join(directory, "index.txt"), "w", encoding="utf-8"):
            pass
        with open(os.path.join(directory, "serial"), "w", encoding="utf-8") as serial:
            serial.write("1000\n")
        self.config = os.path.join(directory, "openssl.cnf")
        with open(self.config, "w", encoding="utf-8") as cfg:
            cfg.write(_OPENSSL_CONFIG.format(dir=directory))
        self.cert = os.path.join(directory, "ca.crt")
        _openssl(
            "req", "-x509", "-new", "-config", self.config, "-extensions", "v3_ca",
            "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
            "-keyout", os.path.join(directory, "ca.key"), "-out", self.cert,
            "-days", "30", "-subj", f"/CN={name}",
        )  # fmt: skip

    def issue(self, name: str, extensions: str, startdate: Optional[str] = None,
              enddate: Optional[str] = None) -> Tuple[str, str]:  # fmt: skip
        key = os.path.join(self.dir, f"{name}.key")
        csr = os.path.join(self.dir, f"{name}.csr")
        cert = os.path.join(self.dir, f"{name}.crt")
        _openssl(
            "req", "-new", "-config", self.config, "-newkey", "ec",
            "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
            "-keyout", key, "-out", csr, "-subj", f"/CN={name}",
        )  # fmt: skip
        args = ["ca", "-batch", "-notext", "-config", self.config, "-extensions", extensions, "-in", csr, "-out", cert]
        if startdate and enddate:
            args += ["-startdate", startdate, "-enddate", enddate]
        else:
            args += ["-days", "30"]
        _openssl(*args)
        return cert, key


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    base = tmp_path_factory.mktemp("smtp_pki")
    trusted = _TestCA(str(base / "trusted"), "Synthetic Trusted Test CA")
    untrusted = _TestCA(str(base / "untrusted"), "Synthetic Untrusted Test CA")
    return SimpleNamespace(
        ca_file=trusted.cert,
        valid=trusted.issue("valid", "leaf_loopback"),
        wrong_host=trusted.issue("wrong_host", "leaf_wrong_host"),
        expired=trusted.issue("expired", "leaf_loopback", "20200101000000Z", "20210101000000Z"),
        untrusted=untrusted.issue("untrusted", "leaf_loopback"),
    )


class LoopbackSMTPServer:
    """Minimal single-connection SMTP server on 127.0.0.1 that records which commands arrived over TLS."""

    def __init__(self, cert_and_key: Tuple[str, str], implicit_tls: bool = False) -> None:
        self.tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.tls_context.load_cert_chain(*cert_and_key)
        self.implicit_tls = implicit_tls
        self.events: List[Tuple[bool, str]] = []
        self.auth_payloads: List[Tuple[bool, str]] = []
        self.messages: List[bytes] = []
        self.handshake_error: Optional[BaseException] = None
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.bind((LOOPBACK, 0))
        self._listener.listen(1)
        self._listener.settimeout(10)
        self.port = self._listener.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def __enter__(self) -> "LoopbackSMTPServer":
        return self

    def __exit__(self, *exc) -> None:
        self._listener.close()
        self._thread.join(timeout=10)

    def _wrap(self, conn: socket.socket) -> Optional[ssl.SSLSocket]:
        try:
            return self.tls_context.wrap_socket(conn, server_side=True)
        except (ssl.SSLError, OSError) as e:
            self.handshake_error = e
            conn.close()
            return None

    def _serve(self) -> None:
        try:
            conn, _ = self._listener.accept()
        except OSError:
            return
        conn.settimeout(10)
        tls = False
        if self.implicit_tls:
            conn = self._wrap(conn)
            if conn is None:
                return
            tls = True
        try:
            self._session(conn, tls)
        except OSError:
            pass
        finally:
            conn.close()

    def _session(self, conn: socket.socket, tls: bool) -> None:
        reader = conn.makefile("rb")
        conn.sendall(b"220 localhost ESMTP synthetic\r\n")
        while True:
            line = reader.readline()
            if not line:
                return
            command = line.decode("ascii", "replace").strip()
            verb = command.split(" ", 1)[0].upper()
            self.events.append((tls, verb))
            if verb in ("EHLO", "HELO"):
                features = ["localhost"] + ([] if tls else ["STARTTLS"]) + ["AUTH PLAIN LOGIN"]
                reply = "".join(f"250-{f}\r\n" for f in features[:-1]) + f"250 {features[-1]}\r\n"
                conn.sendall(reply.encode())
            elif verb == "STARTTLS":
                conn.sendall(b"220 ready to start TLS\r\n")
                reader.close()
                wrapped = self._wrap(conn)
                if wrapped is None:
                    return
                conn, tls = wrapped, True
                reader = conn.makefile("rb")
            elif verb == "AUTH":
                self.auth_payloads.append((tls, command))
                conn.sendall(b"235 authenticated\r\n")
            elif verb == "DATA":
                conn.sendall(b"354 end with <CRLF>.<CRLF>\r\n")
                body = b""
                while not body.endswith(b"\r\n.\r\n"):
                    chunk = reader.readline()
                    if not chunk:
                        return
                    body += chunk
                self.messages.append(body)
                conn.sendall(b"250 queued\r\n")
            elif verb == "QUIT":
                conn.sendall(b"221 bye\r\n")
                return
            else:
                conn.sendall(b"250 ok\r\n")


def _synthetic_context(port: int, ca_file: Optional[str], tls_mode: str = "starttls",
                       timeout: float = 5.0, host: str = LOOPBACK) -> SimpleNamespace:  # fmt: skip
    return SimpleNamespace(
        get_enable_mail=True,
        get_mail_from="sender@example.invalid",
        get_to_mail="recipient@example.invalid",
        get_mail_subject="synthetic subject",
        get_mail_smtp_server=host,
        get_mail_smtp_port=port,
        get_mail_smtp_tls_mode=tls_mode,
        get_mail_smtp_ca_file=ca_file,
        get_mail_smtp_timeout=timeout,
        get_enable_smtp_server_auth=True,
        get_mail_smtp_user_name=SYNTHETIC_USER,
        get_mail_smtp_password=SYNTHETIC_PASSWORD,
        get_smtp_creds_dict={},
        get_enable_templated_basic_email_body=False,
    )


_CONFIG_ARGS = {"message": "synthetic message", "content_type": "plain", "email_notification_type": "detailed"}


def _send(context: SimpleNamespace) -> None:
    SparkExpectationsEmailPluginImpl().send_notification(_context=context, _config_args=_CONFIG_ARGS)


def _assert_no_password(text: str) -> None:
    assert SYNTHETIC_PASSWORD not in text
    assert base64.b64encode(SYNTHETIC_PASSWORD.encode()).decode() not in text


@pytest.mark.parametrize("tls_mode", ["starttls", "ssl"])
def test_trusted_custom_ca_authenticates_only_after_tls(pki, tls_mode):
    with LoopbackSMTPServer(pki.valid, implicit_tls=tls_mode == "ssl") as server:
        _send(_synthetic_context(server.port, pki.ca_file, tls_mode=tls_mode))

    assert server.handshake_error is None
    assert len(server.auth_payloads) == 1
    auth_over_tls, auth_line = server.auth_payloads[0]
    assert auth_over_tls is True
    assert SYNTHETIC_PASSWORD.encode() in base64.b64decode(auth_line.split()[-1])
    assert server.messages and b"synthetic message" in server.messages[0]
    # Nothing beyond the greeting/STARTTLS negotiation happened in plaintext.
    assert {verb for over_tls, verb in server.events if not over_tls} <= {"EHLO", "STARTTLS"}
    assert all(over_tls for over_tls, verb in server.events if verb in ("AUTH", "MAIL", "RCPT", "DATA"))


@pytest.mark.parametrize("tls_mode", ["starttls", "ssl"])
@pytest.mark.parametrize("certificate", ["untrusted", "expired", "wrong_host"])
def test_bad_certificate_fails_before_login(pki, certificate, tls_mode):
    with LoopbackSMTPServer(getattr(pki, certificate), implicit_tls=tls_mode == "ssl") as server:
        with pytest.raises(SparkExpectationsEmailException) as excinfo:
            _send(_synthetic_context(server.port, pki.ca_file, tls_mode=tls_mode))

    assert isinstance(excinfo.value.__cause__, ssl.SSLCertVerificationError)
    assert "certificate verify failed" in str(excinfo.value)
    assert server.auth_payloads == []
    assert server.messages == []
    assert {verb for _, verb in server.events} <= {"EHLO", "STARTTLS"}
    _assert_no_password(str(excinfo.value))


def test_system_trust_store_rejects_private_ca_by_default(pki):
    with LoopbackSMTPServer(pki.valid) as server:
        with pytest.raises(SparkExpectationsEmailException) as excinfo:
            _send(_synthetic_context(server.port, ca_file=None))

    assert isinstance(excinfo.value.__cause__, ssl.SSLCertVerificationError)
    assert server.auth_payloads == []


def test_starttls_not_offered_does_not_fall_back_to_plaintext(pki):
    class NoStartTLSServer(LoopbackSMTPServer):
        def _session(self, conn, tls):
            reader = conn.makefile("rb")
            conn.sendall(b"220 localhost ESMTP synthetic\r\n")
            while True:
                line = reader.readline()
                if not line:
                    return
                verb = line.decode().strip().split(" ", 1)[0].upper()
                self.events.append((tls, verb))
                if verb == "EHLO":
                    conn.sendall(b"250-localhost\r\n250 AUTH PLAIN LOGIN\r\n")
                elif verb == "AUTH":
                    self.auth_payloads.append((tls, line.decode()))
                    conn.sendall(b"235 ok\r\n")
                elif verb == "QUIT":
                    conn.sendall(b"221 bye\r\n")
                    return
                else:
                    conn.sendall(b"250 ok\r\n")

    with NoStartTLSServer(pki.valid) as server:
        with pytest.raises(SparkExpectationsEmailException, match="STARTTLS"):
            _send(_synthetic_context(server.port, pki.ca_file))

    assert server.auth_payloads == []


def test_unresponsive_server_times_out():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind((LOOPBACK, 0))
    listener.listen(1)  # accepts the TCP handshake but never sends the SMTP greeting
    try:
        started = time.monotonic()
        with pytest.raises(SparkExpectationsEmailException) as excinfo:
            _send(_synthetic_context(listener.getsockname()[1], ca_file=None, timeout=0.5))
        elapsed = time.monotonic() - started
    finally:
        listener.close()

    # smtplib reports a greeting timeout as SMTPServerDisconnected("... timed out").
    assert "timed out" in str(excinfo.value)
    assert elapsed < 5
    _assert_no_password(str(excinfo.value))


def test_unsupported_tls_mode_is_rejected_before_connecting():
    with pytest.raises(SparkExpectationsEmailException, match="Unsupported SMTP TLS mode"):
        # Port 9 on loopback is never contacted because the mode is checked first.
        _send(_synthetic_context(9, ca_file=None, tls_mode="none"))


# --- context accessor validation -------------------------------------------------------------------


@pytest.fixture
def se_context():
    return SparkExpectationsContext(product_id="smtp_tls_test", spark=MagicMock())


def test_context_smtp_transport_defaults(se_context):
    assert se_context.get_mail_smtp_tls_mode == "starttls"
    assert se_context.get_mail_smtp_ca_file is None
    assert se_context.get_mail_smtp_timeout == 30.0


@pytest.mark.parametrize(
    "value, expected", [(None, "starttls"), ("", "starttls"), (" SSL ", "ssl"), ("StartTLS", "starttls")]
)
def test_context_tls_mode_normalised(se_context, value, expected):
    se_context.set_mail_smtp_tls_mode(value)
    assert se_context.get_mail_smtp_tls_mode == expected


@pytest.mark.parametrize("value", ["none", "plain", "tls1.0", True])
def test_context_tls_mode_rejects_unknown(se_context, value):
    with pytest.raises(SparkExpectationsMiscException, match="Unsupported SMTP TLS mode"):
        se_context.set_mail_smtp_tls_mode(value)


@pytest.mark.parametrize("value, expected", [(None, 30.0), ("", 30.0), (10, 10.0), ("2.5", 2.5), (300, 300.0)])
def test_context_timeout_accepts_finite_values(se_context, value, expected):
    se_context.set_mail_smtp_timeout(value)
    assert se_context.get_mail_smtp_timeout == expected


@pytest.mark.parametrize("value", [0, -1, 301, "abc", float("nan"), float("inf"), True, [5]])
def test_context_timeout_rejects_invalid_values(se_context, value):
    with pytest.raises(SparkExpectationsMiscException, match="SMTP timeout"):
        se_context.set_mail_smtp_timeout(value)


def test_context_ca_file(se_context, tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("synthetic")
    se_context.set_mail_smtp_ca_file(str(ca_file))
    assert se_context.get_mail_smtp_ca_file == str(ca_file)
    se_context.set_mail_smtp_ca_file("")
    assert se_context.get_mail_smtp_ca_file is None
    with pytest.raises(SparkExpectationsMiscException, match="SMTP CA file"):
        se_context.set_mail_smtp_ca_file(str(tmp_path / "missing.pem"))
