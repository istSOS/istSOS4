# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A navy-themed /docs route, replacing FastAPI's default Swagger UI page.

`swagger_ui_parameters` (see api.py) only controls Swagger UI's *behavior*
-- search, deep linking, persisted auth. It has no mechanism for colors or
typography. Getting an actual visual theme requires injecting real CSS,
which FastAPI has no built-in hook for either -- so this replicates
FastAPI's own default /docs + /docs/oauth2-redirect wiring (see
`fastapi.applications.FastAPI.setup`) byte-for-byte, except the returned
HTML has a <style> block inserted before </head>.

Usage: construct the FastAPI app with ``docs_url=None`` (so its own
automatic route is never registered), then call
``register_custom_docs(app)`` once, right after construction.
"""

from fastapi import FastAPI, Request
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse

_OAUTH2_REDIRECT_PATH = "/docs/oauth2-redirect"

# Navy + gold "chrome" -- topbar, tag headers, buttons, links, table
# headers, headings. Deliberately does NOT touch the GET/POST/PATCH/DELETE
# method-badge colors: those are a real usability convention (scan by
# color), not decoration, and overriding them to a single theme color
# would make the page *harder* to scan, not classier.
_NAVY_THEME_CSS = """
:root {
    --istsos-navy: #0b2545;
    --istsos-navy-dark: #071a33;
    --istsos-navy-light: #1d4370;
    --istsos-gold: #c9a24b;
    --istsos-bg: #f7f9fc;
    --istsos-border: #dde3ec;
}

body { background: var(--istsos-bg); }

.swagger-ui { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }

/* Top bar */
.swagger-ui .topbar {
    background-color: var(--istsos-navy-dark);
    box-shadow: 0 2px 10px rgba(0,0,0,.18);
}
.swagger-ui .topbar .download-url-wrapper { display: none; }

/* Landing description */
.swagger-ui .info .title { color: var(--istsos-navy); font-weight: 700; }
.swagger-ui .info a { color: var(--istsos-navy-light); }
.swagger-ui .info a:hover { color: var(--istsos-gold); }
.swagger-ui .info li, .swagger-ui .info p { line-height: 1.6; }

/* Authorize / scheme bar */
.swagger-ui .scheme-container {
    background: var(--istsos-navy);
    box-shadow: none;
    padding: 18px 0;
}
.swagger-ui .scheme-container .schemes-title,
.swagger-ui .scheme-container label,
.swagger-ui .scheme-container .schemes > label { color: #fff; }
.swagger-ui .btn.authorize {
    background-color: var(--istsos-gold);
    color: var(--istsos-navy-dark);
    border-color: var(--istsos-gold);
    font-weight: 700;
}
.swagger-ui .btn.authorize svg { fill: var(--istsos-navy-dark); }
.swagger-ui .btn.authorize:hover { background-color: #ddb968; border-color: #ddb968; }

/* Tag / section headers */
.swagger-ui .opblock-tag {
    border-bottom: 2px solid var(--istsos-navy-light);
    color: var(--istsos-navy);
}
.swagger-ui .opblock-tag:hover { background: rgba(11,37,69,.04); }
.swagger-ui .opblock-tag small { color: #5b6b82; }

/* Operation cards */
.swagger-ui .opblock {
    border-radius: 8px;
    box-shadow: 0 1px 4px rgba(11,37,69,.08);
}
.swagger-ui .btn.execute {
    background-color: var(--istsos-navy);
    border-color: var(--istsos-navy);
}
.swagger-ui .btn.execute:hover { background-color: var(--istsos-navy-light); }
.swagger-ui select, .swagger-ui input[type=text], .swagger-ui textarea {
    border-radius: 6px;
}
.swagger-ui .parameter__name { font-weight: 600; color: var(--istsos-navy); }

/* Markdown content -- descriptions, this app's landing text */
.swagger-ui .markdown table, .swagger-ui .renderedMarkdown table {
    border-collapse: collapse;
    width: 100%;
    margin: 10px 0 18px;
}
.swagger-ui .markdown table th, .swagger-ui .markdown table td,
.swagger-ui .renderedMarkdown table th, .swagger-ui .renderedMarkdown table td {
    border: 1px solid var(--istsos-border);
    padding: 7px 12px;
    text-align: left;
}
.swagger-ui .markdown table th, .swagger-ui .renderedMarkdown table th {
    background: var(--istsos-navy);
    color: #fff;
}
.swagger-ui .markdown hr, .swagger-ui .renderedMarkdown hr {
    border: none;
    border-top: 1px solid var(--istsos-border);
    margin: 22px 0;
}

/* Schemas panel */
.swagger-ui section.models { border-color: var(--istsos-navy-light); }
.swagger-ui section.models h4 { color: var(--istsos-navy); }
.swagger-ui section.models .model-box { background: var(--istsos-bg); }
"""


def register_custom_docs(app: FastAPI) -> None:
    """Register /docs and /docs/oauth2-redirect with the navy theme.

    app must be constructed with docs_url=None -- otherwise FastAPI's own
    setup() has already claimed the same path and this route would never
    be reached.
    """

    async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
        # Same root_path handling as fastapi.applications.FastAPI.setup,
        # so this works correctly whether the app is mounted under a
        # SUBPATH/VERSION prefix or served at the root.
        root_path = req.scope.get("root_path", "").rstrip("/")
        openapi_url = root_path + app.openapi_url
        oauth2_redirect_url = root_path + _OAUTH2_REDIRECT_PATH

        base = get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{app.title} - Swagger UI",
            oauth2_redirect_url=oauth2_redirect_url,
            init_oauth=app.swagger_ui_init_oauth,
            swagger_ui_parameters=app.swagger_ui_parameters,
        )
        html = base.body.decode("utf-8").replace(
            "</head>", f"<style>{_NAVY_THEME_CSS}</style></head>"
        )
        return HTMLResponse(html)

    app.add_route("/docs", custom_swagger_ui_html, include_in_schema=False)

    async def swagger_ui_redirect(req: Request) -> HTMLResponse:
        return get_swagger_ui_oauth2_redirect_html()

    app.add_route(
        _OAUTH2_REDIRECT_PATH, swagger_ui_redirect, include_in_schema=False
    )
