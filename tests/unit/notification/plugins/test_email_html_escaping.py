"""
Regression tests: untrusted values rendered into HTML email bodies are escaped exactly once.

The packaged templates and user-supplied template strings are rendered for real (no Jinja mocks)
with synthetic payloads. No mail is sent.
"""

import json
from types import SimpleNamespace

import pytest

from spark_expectations.notifications.plugins.email import SparkExpectationsEmailPluginImpl

PAYLOAD = '<script>alert("x")</script>'
PAYLOAD_ESCAPED = "&lt;script&gt;alert(&#34;x&#34;)&lt;/script&gt;"
UNICODE = "café ✓ 数据 — naïve"
PRE_ESCAPED = "a &amp; b"

CUSTOM_STRING_TEMPLATE = "<p>{{ product_id }}</p><p>{{ table_name }}</p>"
BASIC_STRING_TEMPLATE = (
    "{% macro render_table(rows) %}<table>{% for row in rows %}<tr>{% for cell in row %}"
    "<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}</table>{% endmacro %}"
    "<h3>{{ title }}</h3>{{ render_table(rows) }}"
)


def _context(subject="synthetic subject", basic_template="", custom_template=""):
    return SimpleNamespace(
        get_mail_subject=subject,
        get_enable_templated_basic_email_body=True,
        get_basic_default_template=basic_template,
        get_enable_templated_custom_email=True,
        get_custom_default_template=custom_template,
    )


def _render(context, message):
    return SparkExpectationsEmailPluginImpl()._process_message(context, {"message": message})


def _assert_no_raw_markup(content):
    assert "<script>" not in content
    assert "<img" not in content


@pytest.mark.parametrize("basic_template", ["", BASIC_STRING_TEMPLATE], ids=["packaged", "string"])
def test_basic_email_escapes_subject_title_and_fields(basic_template):
    context = _context(subject='<img src=x onerror="alert(1)">', basic_template=basic_template)
    message = f"{PAYLOAD} title\nrule: {PAYLOAD}\n{PAYLOAD}: value\nunicode: {UNICODE}\nentity: {PRE_ESCAPED}"

    content, content_type = _render(context, message)

    assert content_type == "html"
    _assert_no_raw_markup(content)
    assert content.startswith("<h2>&lt;img src=x onerror=&quot;alert(1)&quot;&gt;</h2>")
    assert f"<h3>{PAYLOAD_ESCAPED} title</h3>" in content
    assert content.count(PAYLOAD_ESCAPED) == 3
    assert UNICODE in content
    # Escaped exactly once: "&amp;" in the data becomes "&amp;amp;", never "&amp;amp;amp;".
    assert "a &amp;amp; b" in content
    assert "&amp;amp;amp;" not in content
    # The table macro output is markup, not escaped text.
    assert "<table" in content and "<td>" in content and "&lt;td&gt;" not in content


def test_basic_email_plain_subject_unchanged():
    content, _ = _render(_context(subject=f"DQ alert {UNICODE}"), "title\nkey: value")
    assert content.startswith(f"<h2>DQ alert {UNICODE}</h2>")


@pytest.mark.parametrize("custom_template", ["", CUSTOM_STRING_TEMPLATE], ids=["packaged", "string"])
def test_custom_email_escapes_fields(custom_template):
    data = {
        "product_id": PAYLOAD,
        "table_name": f"{UNICODE} {PRE_ESCAPED}",
        "source_query_dq_results": [{"rule": PAYLOAD, "description": PAYLOAD, "status": PAYLOAD}],
        "source_agg_dq_results": [{"rule": PAYLOAD, "description": UNICODE, "status": "fail"}],
        "meta_dq_run_datetime": PAYLOAD,
    }
    content, content_type = _render(
        _context(custom_template=custom_template), "CUSTOM EMAIL\n" + json.dumps(data, ensure_ascii=False)
    )

    assert content_type == "html"
    _assert_no_raw_markup(content)
    assert PAYLOAD_ESCAPED in content
    assert UNICODE in content
    assert "a &amp;amp; b" in content
    assert "&amp;amp;amp;" not in content
    if not custom_template:
        # Every untrusted field in the packaged template is escaped: product, 3 query fields, agg rule, timestamp.
        assert content.count(PAYLOAD_ESCAPED) == 6
