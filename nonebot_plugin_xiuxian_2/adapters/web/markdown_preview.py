"""Safe HTML rendering for administrator Markdown previews."""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from urllib.parse import urlsplit


_ALLOWED_TAGS = frozenset(
    {
        "a",
        "blockquote",
        "br",
        "code",
        "del",
        "dd",
        "div",
        "dl",
        "dt",
        "em",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "img",
        "li",
        "ol",
        "p",
        "pre",
        "s",
        "span",
        "strong",
        "sub",
        "sup",
        "table",
        "tbody",
        "td",
        "tfoot",
        "th",
        "thead",
        "tr",
        "ul",
    }
)
_VOID_TAGS = frozenset({"br", "hr", "img"})
_LANGUAGE_CLASS = re.compile(r"language-[A-Za-z0-9_+-]+\Z")
_URL_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def _safe_url(value: str) -> str | None:
    url = value.strip()
    if not url or any(ord(char) <= 0x20 or ord(char) == 0x7F for char in url):
        return None

    # Browsers treat backslashes as slashes for special URLs.
    slash_normalized = url.replace("\\", "/")
    if slash_normalized.startswith("//"):
        return None

    scheme_match = _URL_SCHEME.match(url)
    if scheme_match:
        scheme = scheme_match.group(0)[:-1].lower()
        if scheme not in {"http", "https", "mailto"}:
            return None
        try:
            parsed = urlsplit(url)
        except ValueError:
            return None
        if scheme in {"http", "https"} and not parsed.netloc:
            return None
        if scheme == "mailto" and not parsed.path:
            return None
    return url


class _SafeHtmlRenderer(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.open_tags: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in _ALLOWED_TAGS:
            return

        attributes = dict(attrs)
        safe_attrs: list[tuple[str, str]] = []

        if tag == "a":
            href = attributes.get("href")
            safe_href = _safe_url(href) if href is not None else None
            if safe_href is not None:
                safe_attrs.append(("href", safe_href))
            title = attributes.get("title")
            if title is not None:
                safe_attrs.append(("title", title))
        elif tag == "img":
            src = attributes.get("src")
            safe_src = _safe_url(src) if src is not None else None
            if safe_src is None:
                return
            safe_attrs.append(("src", safe_src))
            for name in ("alt", "title"):
                value = attributes.get(name)
                if value is not None:
                    safe_attrs.append((name, value))
        elif tag == "code":
            class_value = attributes.get("class")
            if class_value and _LANGUAGE_CLASS.fullmatch(class_value):
                safe_attrs.append(("class", class_value))
        elif tag in {"td", "th"}:
            align = attributes.get("align")
            if align in {"left", "center", "right"}:
                safe_attrs.append(("align", align))

        rendered_attrs = "".join(
            f' {name}="{html.escape(value, quote=True)}"'
            for name, value in safe_attrs
        )
        self.parts.append(f"<{tag}{rendered_attrs}>")
        if tag not in _VOID_TAGS:
            self.open_tags.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag not in self.open_tags:
            return
        while self.open_tags:
            current = self.open_tags.pop()
            self.parts.append(f"</{current}>")
            if current == tag:
                break

    def handle_data(self, data: str) -> None:
        self.parts.append(html.escape(data))

    def close(self) -> None:
        super().close()
        while self.open_tags:
            self.parts.append(f"</{self.open_tags.pop()}>")


def _sanitize_html(rendered: str) -> str:
    parser = _SafeHtmlRenderer()
    parser.feed(rendered)
    parser.close()
    return "".join(parser.parts)


def render_markdown_preview(text: str) -> str:
    """Render Markdown while keeping raw HTML and active URLs inert."""
    try:
        import markdown

        renderer = markdown.Markdown(
            extensions=["extra", "tables", "fenced_code", "nl2br"]
        )
        # Raw HTML is shown as text; generated Markdown markup is sanitized below.
        renderer.preprocessors.deregister("html_block")
        renderer.inlinePatterns.deregister("html")
        rendered = renderer.convert(text)
    except Exception:
        rendered = "<pre>" + html.escape(text) + "</pre>"
    return _sanitize_html(rendered)


__all__ = ["render_markdown_preview"]
