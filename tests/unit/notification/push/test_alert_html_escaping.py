"""Regression tests: report values must be HTML-escaped in alert email templates."""

from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from jinja2 import BaseLoader, Environment, PackageLoader
from markupsafe import Markup

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.notifications.push.alert import SparkExpectationsAlert, _get_detailed_report_template

LINK = '<a href="https://attacker.invalid/login">click</a>'
IMAGE = '<img src="https://attacker.invalid/pixel.png">'
ATTR_BREAKOUT = '" onmouseover="alert(1)" x="'
SINGLE_QUOTE = "' onerror='x"
TABLE_BREAKOUT = "</td></tr></table><h1>forged</h1><table><tr><td>"
MALFORMED = "<b <i>unclosed <!-- comment"
SCRIPT = "<script>alert(1)</script>"
UNICODE = "règle_ünïcode_✓_数据"

PAYLOADS = [LINK, IMAGE, ATTR_BREAKOUT, SINGLE_QUOTE, TABLE_BREAKOUT, MALFORMED, SCRIPT]

# Elements and attributes that the trusted templates (packaged and the custom ones in this file) emit.
ALLOWED_TAGS = {
    "!doctype", "html", "head", "meta", "title", "style", "body", "dl", "dt", "dd",
    "h2", "h3", "p", "strong", "em", "br", "ul", "li", "table", "thead", "tbody", "tr", "th", "td",
}
ALLOWED_ATTRS = {"class", "border", "style", "lang", "charset"}

TEMPLATES_DIR = Path(__file__).resolve().parents[4] / "spark_expectations" / "config" / "templates"


class _MarkupCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: list = []
        self.attrs: list = []
        self.comments: list = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        self.attrs.extend(attrs)

    def handle_decl(self, decl):
        self.tags.append("!" + decl.split()[0].lower())

    def handle_comment(self, data):
        self.comments.append(data)


def _assert_no_injected_markup(html: str) -> None:
    """Parse the output and check that no element, attribute, or comment came from a value."""
    parser = _MarkupCollector()
    parser.feed(html)
    parser.close()
    unexpected_tags = set(parser.tags) - ALLOWED_TAGS
    assert not unexpected_tags, f"injected elements {unexpected_tags} in output:\n{html}"
    unexpected_attrs = {name for name, _ in parser.attrs} - ALLOWED_ATTRS
    assert not unexpected_attrs, f"injected attributes {unexpected_attrs} in output:\n{html}"
    assert not any("attacker.invalid" in (value or "") for _, value in parser.attrs), html
    assert not any("alert(1)" in (value or "") for _, value in parser.attrs), html
    assert not parser.comments, f"injected comment in output:\n{html}"


def _detailed_report_kwargs() -> dict:
    headers = ["rule", "rule_description", "column_name", "table_name", LINK, "Validation_Status"]
    rows = [
        (LINK, IMAGE, ATTR_BREAKOUT, TABLE_BREAKOUT, SINGLE_QUOTE, "fail"),
        (MALFORMED, SCRIPT, UNICODE, "dq.table", 42, "pass"),
    ]
    return {"title": IMAGE, "headers": headers, "rows": rows}


@pytest.mark.parametrize("payload", PAYLOADS)
def test_detailed_default_template_escapes_each_payload(payload):
    template = _get_detailed_report_template(None)
    html = template.render(
        render_table=template.module.render_table, title=payload, headers=[payload], rows=[(payload, "fail")]
    )
    _assert_no_injected_markup(html)
    assert str(Markup.escape(payload)) in html


def test_detailed_default_template_escapes_all_fields_and_keeps_layout():
    template = _get_detailed_report_template(None)
    html = template.render(render_table=template.module.render_table, **_detailed_report_kwargs())

    _assert_no_injected_markup(html)
    assert "&lt;a href=&#34;https://attacker.invalid/login&#34;&gt;click&lt;/a&gt;" in html
    assert "&lt;/td&gt;&lt;/tr&gt;&lt;/table&gt;" in html
    assert "&#39; onerror=&#39;x" in html
    # Trusted template markup and status styling still render.
    assert "<style>" in html
    assert html.count("<table border=1>") == 1
    assert html.count("</table>") == 1
    assert '<td class="fail">fail</td>' in html
    assert "<td>pass</td>" in html
    assert "<td>42</td>" in html
    # Unicode passes through unchanged.
    assert UNICODE in html


def test_custom_string_template_is_autoescaped_and_keeps_trusted_markup():
    custom = (
        '{% macro render_table(headers, rows) %}<table class="se">{% for r in rows %}<tr>'
        "{% for c in r %}<td>{{ c }}</td>{% endfor %}</tr>{% endfor %}</table>{% endmacro %}"
        '<h3 style="color: red">{{ title }}</h3>{{ render_table(headers, rows) }}'
    )
    template = _get_detailed_report_template(custom)
    html = template.render(render_table=template.module.render_table, **_detailed_report_kwargs())

    _assert_no_injected_markup(html)
    assert '<h3 style="color: red">' in html
    assert '<table class="se">' in html
    assert html.count("</table>") == 1
    assert UNICODE in html


def test_no_double_escaping():
    template = _get_detailed_report_template(None)
    html = template.render(
        render_table=template.module.render_table,
        title="A & B",
        headers=["h"],
        rows=[("a &amp; b",), (Markup("<em>trusted</em>"),)],
    )
    # Plain text is escaped exactly once.
    assert "<h2>A &amp; B</h2>" in html
    assert "<td>a &amp;amp; b</td>" in html
    # Values already marked safe and macro output are not escaped again.
    assert "<td><em>trusted</em></td>" in html
    assert "&lt;td" not in html and "&lt;table" not in html and "&amp;lt;" not in html


@pytest.mark.parametrize(
    "template_name, render_kwargs",
    [
        ("advanced_email_alert_template.jinja", {"title": LINK, "headers": [IMAGE], "rows": [(TABLE_BREAKOUT, "fail")]}),
        ("basic_email_alert_template.jinja", {"title": LINK, "rows": [[IMAGE, ATTR_BREAKOUT], [MALFORMED, SCRIPT]]}),
    ],
)
@pytest.mark.parametrize("autoescape", [False, True])
def test_packaged_table_templates_escape_regardless_of_environment(template_name, render_kwargs, autoescape):
    env = Environment(loader=PackageLoader("spark_expectations", "config/templates"), autoescape=autoescape)
    template = env.get_template(template_name)
    # Callers pass the module-level macro back in; it must stay exported.
    html = template.render(render_table=template.module.render_table, **render_kwargs)

    _assert_no_injected_markup(html)
    assert "<table border=1>" in html
    assert html.count("</table>") == 1
    assert "&lt;a href=" in html


@pytest.mark.parametrize("autoescape", [False, True])
def test_packaged_custom_template_escapes_regardless_of_environment(autoescape):
    env = Environment(loader=PackageLoader("spark_expectations", "config/templates"), autoescape=autoescape)
    template = env.get_template("custom_email_alert_template.jinja")
    item = {"rule": LINK, "description": IMAGE, "status": ATTR_BREAKOUT}
    html = template.render(
        product_id=SCRIPT,
        table_name=TABLE_BREAKOUT,
        source_query_dq_results=[item],
        source_agg_dq_results=[{"rule": MALFORMED, "description": SINGLE_QUOTE, "status": UNICODE}],
        meta_dq_run_datetime=LINK,
    )

    _assert_no_injected_markup(html)
    assert html.startswith("<!DOCTYPE html>")
    assert "<dl>" in html and "</dl>" in html
    assert UNICODE in html


def test_packaged_templates_enable_autoescape_inside_template_source():
    for name in (
        "advanced_email_alert_template.jinja",
        "basic_email_alert_template.jinja",
        "custom_email_alert_template.jinja",
    ):
        source = (TEMPLATES_DIR / name).read_text(encoding="utf-8")
        compiled = Environment(loader=BaseLoader()).from_string(source)
        assert "{% autoescape true %}" in source, name
        assert compiled is not None


@pytest.fixture(name="alert_with_injected_report")
def fixture_alert_with_injected_report():
    context = Mock(spec=SparkExpectationsContext)
    context.spark = MagicMock()
    context.get_mail_subject = f"DQ report {LINK}{IMAGE}"
    context.get_to_mail = "receiver@example.invalid"
    context.get_detailed_default_template = None

    reports = {
        "header": (["snapshot_date", "product_id", "job", "status"], [("2026-01-01", SCRIPT, LINK, "FAIL")], 3),
        "summary": (["product_id", "rule", "status_summary"], [(UNICODE, IMAGE, "Pass:1 / Fail:1")], 2),
        "detailed": (
            ["rule", "rule_description", "column_name", "Validation_Status"],
            [(LINK, TABLE_BREAKOUT, ATTR_BREAKOUT, "fail")],
            3,
        ),
    }
    alert = SparkExpectationsAlert(context)
    with (
        patch.object(SparkExpectationsAlert, "get_report_data", side_effect=lambda kind: reports[kind]),
        patch("spark_expectations.notifications.push.alert.SparkExpectationsEmailPluginImpl") as plugin,
    ):
        yield alert, plugin


def test_prep_report_data_escapes_subject_and_report_fields(alert_with_injected_report):
    alert, plugin = alert_with_injected_report
    html, subject, receivers = alert.prep_report_data()

    assert html, "report rendering failed"
    _assert_no_injected_markup(html)
    assert html.startswith("<h2>DQ report &lt;a href=")
    assert '<td class="fail">fail</td>' in html
    assert '<td class="fail">FAIL</td>' in html
    assert UNICODE in html
    # The raw subject is still used for the mail header; only the HTML body copy is escaped.
    assert subject == f"DQ report {LINK}{IMAGE}"
    assert receivers == "receiver@example.invalid"
    sent_args = plugin.return_value.send_notification.call_args[0][1]
    assert sent_args["message"] == html
    assert sent_args["content_type"] == "html"


def test_prep_report_data_autoescapes_custom_template_string(alert_with_injected_report):
    alert, _ = alert_with_injected_report
    alert._context.get_detailed_default_template = (
        "{% macro render_table(headers, rows) %}<ul>{% for r in rows %}<li>{{ r | join(' | ') }}</li>"
        "{% endfor %}</ul>{% endmacro %}<p><strong>{{ title }}</strong></p>{{ render_table(headers, rows) }}"
    )
    html, _, _ = alert.prep_report_data()

    assert html, "report rendering failed"
    _assert_no_injected_markup(html)
    assert "<p><strong>Summary by data_rule:</strong></p>" in html
    assert "<ul><li>" in html
