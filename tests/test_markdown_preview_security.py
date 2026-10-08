from nonebot_plugin_xiuxian_2.adapters.web.markdown_preview import (
    render_markdown_preview,
)


def test_raw_html_and_event_attributes_are_rendered_as_text() -> None:
    rendered = render_markdown_preview(
        '<script>alert(1)</script>\n\n'
        '<img src="x" onerror="alert(2)">\n\n'
        '<b onclick="alert(3)">raw</b>'
    )

    assert "<script" not in rendered
    assert "<img" not in rendered
    assert "&lt;script&gt;" in rendered
    assert '&lt;b onclick=&quot;alert(3)&quot;&gt;raw&lt;/b&gt;' in rendered
    assert '&lt;img src=&quot;x&quot; onerror=&quot;alert(2)&quot;&gt;' in rendered


def test_dangerous_markdown_link_is_kept_without_href() -> None:
    rendered = render_markdown_preview(
        "[run](javascript:alert%281%29) "
        "[data](data:text/html,alert%281%29) "
        "[encoded](java&#x73;cript:alert%281%29)"
    )

    assert "<a>run</a>" in rendered
    assert "<a>data</a>" in rendered
    assert "<a>encoded</a>" in rendered
    assert "javascript:" not in rendered.lower()
    assert "data:text/html" not in rendered.lower()


def test_tables_and_fenced_code_remain_formatted() -> None:
    rendered = render_markdown_preview(
        "| Name | Count |\n| --- | ---: |\n| herbs | 3 |\n\n"
        "```python\nvalue = 1 < 2\n```"
    )

    assert "<table>" in rendered
    assert "<th>Name</th>" in rendered
    assert "<td>herbs</td>" in rendered
    assert '<pre><code class="language-python">' in rendered
    assert "value = 1 &lt; 2" in rendered


def test_safe_links_keep_only_approved_urls() -> None:
    rendered = render_markdown_preview(
        "[web](https://example.invalid/path) [local](/messages) [mail](mailto:a@example.invalid)"
    )

    assert 'href="https://example.invalid/path"' in rendered
    assert 'href="/messages"' in rendered
    assert 'href="mailto:a@example.invalid"' in rendered
