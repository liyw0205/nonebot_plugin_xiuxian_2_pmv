"""Headless browser smoke for the refactored admin web UI.

The architecture P6 acceptance criteria require the isolated web instance to be
driven through a real browser in both a desktop and a mobile viewport, with no
horizontal overflow.  This script makes that check reproducible instead of an
ad-hoc terminal session.

Usage::

    python scripts/browser_smoke.py --base-url http://127.0.0.1:5889 \
        --admin-id 12345 --out /tmp/browser-smoke

It writes screenshots and a JSON receipt into ``--out`` (defaults to a temp
directory).  Nothing is written into the repository, and the admin id is never
persisted to disk.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

VIEWPORTS = {
    "desktop": {"width": 1440, "height": 900},
    "mobile": {"width": 390, "height": 844},
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="browser-smoke")
    parser.add_argument("--base-url", required=True, help="base URL of the isolated instance")
    parser.add_argument("--admin-id", required=True, help="admin id used to log in")
    parser.add_argument("--out", help="directory for screenshots and the receipt")
    parser.add_argument("--timeout-ms", type=int, default=15000)
    return parser


def _overflow(page) -> tuple[int, int]:
    return page.evaluate(
        "() => [document.documentElement.scrollWidth, window.innerWidth]"
    )


def _wait_until_ready(base: str, timeout_ms: int) -> None:
    """Block until the isolated instance reports ``/health/ready``.

    Driving the UI before the runtime finishes booting produces misleading
    redirects, so the smoke refuses to start against a half-started instance.
    """
    import time
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout_ms / 1000
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/health/ready", timeout=5) as response:
                if response.status == 200:
                    return
                last_error = f"HTTP {response.status}"
        except urllib.error.URLError as exc:  # not up yet
            last_error = str(exc)
        except Exception as exc:  # noqa: BLE001 - report whatever blocked us
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.5)
    raise RuntimeError(f"{base}/health/ready did not become ready: {last_error}")


def _run(args: argparse.Namespace, out_dir: Path) -> dict:
    from playwright.sync_api import sync_playwright

    base = args.base_url.rstrip("/")
    receipt: dict = {"base_url": base, "viewports": {}, "errors": []}
    _wait_until_ready(base, args.timeout_ms)
    receipt["readiness"] = f"{base}/health/ready"

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            for name, viewport in VIEWPORTS.items():
                context = browser.new_context(viewport=viewport)
                page = context.new_page()
                page.set_default_timeout(args.timeout_ms)
                try:
                    # Unauthenticated visitors are bounced to the login page.
                    page.goto(f"{base}/", wait_until="networkidle")
                    login_url = page.url
                    if not login_url.endswith("/login"):
                        receipt["errors"].append(
                            f"{name}: expected redirect to /login, landed on {login_url}"
                        )
                    page.screenshot(path=str(out_dir / f"login-{name}.png"))

                    page.fill("#admin_id", args.admin_id)
                    page.click("button[type=submit]")
                    page.wait_for_url(f"{base}/", timeout=args.timeout_ms)
                    page.screenshot(path=str(out_dir / f"home-{name}.png"))

                    scroll_width, inner_width = _overflow(page)
                    receipt["viewports"][name] = {
                        "viewport": viewport,
                        "login_url": login_url,
                        "home_url": page.url,
                        "scroll_width": scroll_width,
                        "inner_width": inner_width,
                        "overflow": scroll_width > inner_width,
                        "title": page.title(),
                    }
                    if scroll_width > inner_width:
                        receipt["errors"].append(
                            f"{name}: horizontal overflow {scroll_width} > {inner_width}"
                        )
                finally:
                    context.close()
        finally:
            browser.close()

    receipt["ok"] = not receipt["errors"]
    return receipt


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.out:
        out_dir = Path(args.out).expanduser().resolve()
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="xiuxian-browser-smoke-"))
    out_dir.mkdir(parents=True, exist_ok=True)
    # The web runtime logs a diagnostic line to stdout on import; keep stdout
    # machine-readable by routing stray output to stderr.
    with redirect_stdout(sys.stderr):
        receipt = _run(args, out_dir)
    receipt["out_dir"] = str(out_dir)
    (out_dir / "receipt.json").write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0 if receipt["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
