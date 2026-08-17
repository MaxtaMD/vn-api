"""
vidnest.fun stream extractor — FastAPI service.

Endpoints:
  GET /                                    -> simple HTML test page
  GET /api/stream/movie/{tmdb_id}
        -> stream URLs from ALL backends
  GET /api/stream/tv/{tmdb_id}/season/{season}/episode/{episode}
        -> stream URLs from ALL backends
  GET /proxy?token=<encrypted>
        -> HLS manifest proxy (Referer spoofing + segment URL rewriting)

IMPORTANT — proxy_url is only ever attached to actual HLS manifests
(.m3u8, or files served as one regardless of extension — some backends
disguise manifests with a .txt extension, e.g. KlikXXI's
cf-master.<id>.txt). Direct video files (.mkv, .mp4, .webm — e.g. Vidzee's
streamflixserver.site links) are left as their raw URL with no proxy_url,
since those already play directly with no Referer spoofing needed.

proxy_url now carries an opaque, encrypted `?token=` instead of plaintext
`?url=&referer=` — the real upstream URL/referer are AES-256-GCM encrypted
(see proxy_token.py) so they never appear in the query string, browser
devtools, shared links, or access logs. Same scheme as the anime-api
project's signedProxy.ts.

Access is restricted the same way as the anime-api project too: set
ALLOWED_ORIGINS (comma-separated) and only requests whose Origin/Referer
matches are allowed to call /api/stream/*. /proxy is excluded from this
gate — it's hit directly by <video>/hls.js for every segment/playlist
and browsers often omit Origin/Referer for plain media requests, so it's
protected by the signed token instead (see proxy_token.py). If
ALLOWED_ORIGINS is unset, the gate is skipped entirely (open/dev mode).

/proxy also folds in everything the separate universal-proxy Railway
service does — DASH (.mpd) manifest rewriting alongside HLS, a byte-capped
in-memory LRU cache for manifests/segments (MAX_CACHE_BYTES), and
background prefetching of upcoming segments (PREFETCH_COUNT /
PREFETCH_CONCURRENCY / ENABLE_PREFETCH) — so VidNest's inbuilt proxy is a
superset of universal-proxy, no separate deployment needed.
"""

import os
import re
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import urljoin, urlsplit, quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from vidnest_core import VidnestError, VIDNEST_REFERER, extract_all, extract_anime
from proxy_token import create_proxy_token, verify_proxy_token

app = FastAPI(
    title="VidNest Stream Extractor",
    description="Extracts streaming sources from vidnest.fun's backends, with a manifest-only proxy.",
    version="1.0.0",
)

# ── Allowed origins ──────────────────────────────────────────────────────────
# Comma-separated list of origins/hostnames allowed to use this API, mirrors
# the anime-api project's ALLOWED_ORIGINS. Example:
#   ALLOWED_ORIGINS=https://clixarena.com,https://www.clixarena.com
_ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()]


def _extract_hostname(value: str) -> Optional[str]:
    try:
        hostname = urlsplit(value).hostname
        return hostname.lower() if hostname else None
    except Exception:
        return None


_ALLOWED_HOSTNAMES = [h for h in (_extract_hostname(o) for o in _ALLOWED_ORIGINS) if h]


def _is_allowed_origin(origin: Optional[str]) -> bool:
    if not _ALLOWED_HOSTNAMES:
        return True  # no allowlist configured → open (dev mode)
    if not origin:
        return False
    hostname = _extract_hostname(origin) or origin.lower()
    return hostname in _ALLOWED_HOSTNAMES


app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS or ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# Origin/Referer gate — blocks API access from anything but the allowed
# site(s). NOTE: this checks the Origin/Referer headers, which a non-browser
# client (curl, scripts) can freely spoof. It stops other websites' front-ends
# and casual browser access from using the API; it is not a strong auth layer.
#
# IMPORTANT: /proxy is excluded from this gate. It's hit directly by the
# browser's <video>/hls.js for every segment and playlist load, and browsers
# frequently send no Referer/Origin at all for plain media requests (varies
# by Referrer-Policy, browser, extensions). Gating it here would break real
# playback even from the allowed site. It's already protected by the signed
# proxy token instead (see proxy_token.py) — only a request holding a valid,
# unexpired, server-issued token can pull the underlying stream.
@app.middleware("http")
async def origin_gate(request: Request, call_next):
    if request.url.path == "/proxy" or not request.url.path.startswith("/api"):
        return await call_next(request)
    if not _ALLOWED_HOSTNAMES:
        return await call_next(request)  # no allowlist configured → skip gate (dev mode)

    origin = request.headers.get("origin")
    referer = request.headers.get("referer")
    candidate = origin or referer

    if candidate and _is_allowed_origin(candidate):
        return await call_next(request)

    return JSONResponse(
        status_code=403,
        content={"error": "Forbidden: this API can only be used from the allowed site."},
    )


# ── Which URLs actually need proxying ───────────────────────────────────────
_DIRECT_FILE_EXTENSIONS = (".mkv", ".mp4", ".webm", ".ts", ".avi", ".mov")
_MANIFEST_LIKE_EXTENSIONS = (".m3u8", ".txt")


def _looks_like_manifest(url: str) -> bool:
    path = urlsplit(url).path.lower()
    if path.endswith(_DIRECT_FILE_EXTENSIONS):
        return False
    if path.endswith(_MANIFEST_LIKE_EXTENSIONS):
        return True
    # No recognizable extension (e.g. lizer123.site/getm3u8/WPNTDXR1) — can't
    # tell from the URL alone; the /proxy route sniffs real Content-Type and
    # falls back to binary passthrough if it's not actually a manifest.
    return True


def build_proxy_url(worker_base: str, raw_url: str, referer: str) -> str:
    token = create_proxy_token(raw_url, referer or None)
    return f"{worker_base.rstrip('/')}/proxy?token={quote(token, safe='')}"


def build_direct_proxy_url(worker_base: str, raw_url: str) -> str:
    """For direct file links (.mkv/.mp4/...) we don't need referer/origin
    spoofing (unlike HLS/DASH manifest segments, these direct links aren't
    typically referer-gated), so we don't store a `ref` in the token at
    all and the proxy sends no Referer/Origin upstream for these — just a
    plain byte-streaming proxy through /proxy with an encrypted, expiring
    token hiding the real URL."""
    token = create_proxy_token(raw_url, None)
    return f"{worker_base.rstrip('/')}/proxy?token={quote(token, safe='')}"


def attach_proxy_urls(payload: dict, worker_base: str) -> dict:
    url = payload.get("url")
    headers = payload.get("headers") or {}
    referer = headers.get("Referer", VIDNEST_REFERER)

    if not url:
        payload["proxy_url"] = None
    elif _looks_like_manifest(url):
        payload["proxy_url"] = build_proxy_url(worker_base, url, referer)
    else:
        # Direct file (.mkv/.mp4/.webm/...) — token-protected, but no
        # referer/origin needed for these.
        payload["proxy_url"] = build_direct_proxy_url(worker_base, url)


def _https_worker_base(request: Request) -> str:
    # Railway (and most PaaS hosts) terminate TLS at the edge and forward
    # to Uvicorn over plain HTTP, so request.base_url naturally comes back
    # as http:// unless proxy headers are correctly trusted. --proxy-headers
    # on the Uvicorn start command should already fix this, but we force
    # https here too as a defensive fallback — an http:// proxy_url served
    # into an https:// page gets blocked by the browser as mixed content.
    base = str(request.base_url).rstrip("/")
    if base.startswith("http://") and not base.startswith("http://localhost") and not base.startswith("http://127."):
        base = "https://" + base[len("http://"):]
    return base

    return payload


HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>VidNest Stream Extractor</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 700px; margin: 40px auto; padding: 0 16px; }
    code { background: #f0f0f0; padding: 2px 6px; border-radius: 4px; }
    h1 { font-size: 1.4rem; }
  </style>
</head>
<body>
  <h1>VidNest Stream Extractor</h1>
  <p>Endpoints:</p>
  <ul>
    <li><code>GET /api/stream/movie/{tmdb_id}</code></li>
    <li><code>GET /api/stream/tv/{tmdb_id}/season/{season}/episode/{episode}</code></li>
    <li><code>GET /api/stream/anime/{anilist_id}/{episode}/{sub_or_dub}</code></li>
    <li><code>GET /proxy?token=...</code> — manifest proxy (encrypted token, skipped for direct .mkv/.mp4 files)</li>
  </ul>
  <p>Example: <a href="/api/stream/movie/1433117">/api/stream/movie/1433117</a></p>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def root():
    return HTML_PAGE


@app.get("/api/stream/movie/{tmdb_id}")
def api_stream_movie(tmdb_id: str, request: Request):
    worker_base = _https_worker_base(request)
    try:
        result = extract_all(media_type="movie", tmdb_id=tmdb_id)
    except VidnestError as e:
        raise HTTPException(status_code=502, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")

    for r in result.get("results", []):
        if r.get("success") and r.get("url"):
            attach_proxy_urls(r, worker_base)
    for r in result.get("playable_urls", []):
        attach_proxy_urls(r, worker_base)

    return JSONResponse(result)


@app.get("/api/stream/tv/{tmdb_id}/season/{season}/episode/{episode}")
def api_stream_tv(tmdb_id: str, season: int, episode: int, request: Request):
    worker_base = _https_worker_base(request)
    try:
        result = extract_all(media_type="tv", tmdb_id=tmdb_id, season=season, episode=episode)
    except VidnestError as e:
        raise HTTPException(status_code=502, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")

    for r in result.get("results", []):
        if r.get("success") and r.get("url"):
            attach_proxy_urls(r, worker_base)
    for r in result.get("playable_urls", []):
        attach_proxy_urls(r, worker_base)

    return JSONResponse(result)


@app.get("/api/stream/anime/{anilist_id}/{episode}/{sub_or_dub}")
def api_stream_anime(anilist_id: str, episode: int, sub_or_dub: str, request: Request):
    if sub_or_dub not in ("sub", "dub"):
        raise HTTPException(status_code=422, detail="sub_or_dub must be 'sub' or 'dub'")

    worker_base = _https_worker_base(request)
    try:
        result = extract_anime(anilist_id=anilist_id, episode=episode, sub_or_dub=sub_or_dub)
    except VidnestError as e:
        raise HTTPException(status_code=502, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")

    for r in result.get("results", []):
        if r.get("success") and r.get("url"):
            attach_proxy_urls(r, worker_base)
    for r in result.get("playable_urls", []):
        attach_proxy_urls(r, worker_base)

    return JSONResponse(result)


# ── Manifest proxy ───────────────────────────────────────────────────────────

_PROXY_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _proxy_resolve_url(uri: str, base: str) -> str:
    if uri.startswith("http://") or uri.startswith("https://"):
        return uri
    if uri.startswith("//"):
        return "https:" + uri
    return urljoin(base, uri)


def _proxy_upstream_headers(referer: str, range_header: Optional[str]) -> dict:
    headers = {
        "User-Agent": _PROXY_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    # referer is only ever non-empty here for manifest/segment URLs, whose
    # tokens carry a `ref`. Direct-file tokens (see build_direct_proxy_url)
    # never store a ref, so this naturally stays empty for them — no
    # Referer/Origin sent upstream, since those links aren't referer-gated.
    if referer:
        headers["Referer"] = referer
        try:
            parsed = urlsplit(referer)
            headers["Origin"] = f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            pass
    if range_header:
        headers["Range"] = range_header
    return headers


def _rewrite_hls(text: str, worker_base: str, target_url: str, referer: str) -> str:
    def _sub_uri_attr(m):
        uri = m.group(1)
        try:
            abs_url = _proxy_resolve_url(uri, target_url)
            return f'URI="{build_proxy_url(worker_base, abs_url, referer)}"'
        except Exception:
            return m.group(0)

    out_lines = []
    for line in text.split("\n"):
        if "URI=" in line:
            line = re.sub(r'URI="([^"]+)"', _sub_uri_attr, line)

        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            out_lines.append(line)
            continue

        try:
            resolved = _proxy_resolve_url(stripped, target_url)
            out_lines.append(build_proxy_url(worker_base, resolved, referer))
        except Exception:
            out_lines.append(line)

    return "\n".join(out_lines)


def _proxy_content_kind(target_url: str, content_type: str) -> str:
    ct = (content_type or "").lower()
    path = target_url.split("?")[0].lower()
    if path.endswith((".m3u8", ".txt")) or "mpegurl" in ct:
        return "hls"
    if path.endswith(".mpd") or "dash+xml" in ct or "application/dash" in ct:
        return "dash"
    if path.endswith((".vtt", ".srt", ".ass")) or "vtt" in ct:
        return "subtitle"
    return "binary"


_DASH_TEMPLATE_RE = re.compile(r"\$(Number|Time|Bandwidth|RepresentationID)\$")
_DASH_BASEURL_RE = re.compile(r"<BaseURL>([^<]+)</BaseURL>")
_DASH_ATTR_RES = [re.compile(rf'{attr}="([^"]+)"') for attr in ("media", "initialization", "sourceURL", "index")]


def _rewrite_dash(text: str, worker_base: str, target_url: str, referer: str):
    """Rewrites DASH (.mpd) manifests the same way _rewrite_hls handles HLS —
    BaseURL elements and the media/initialization/sourceURL/index attributes
    each get pointed at our own /proxy?token=... instead of the raw upstream
    URL. Template placeholders like $Number$/$Time$ are left untouched (they
    aren't real URLs yet, the player expands them itself) and are excluded
    from the prefetch list. Mirrors the Node proxy's rewriteDash()."""
    segment_abs_urls: list[str] = []

    def _sub_baseurl(m: "re.Match[str]") -> str:
        raw = m.group(1).strip()
        try:
            abs_url = _proxy_resolve_url(raw, target_url)
        except Exception:
            return m.group(0)
        if not _DASH_TEMPLATE_RE.search(abs_url):
            segment_abs_urls.append(abs_url)
        return f"<BaseURL>{build_proxy_url(worker_base, abs_url, referer)}</BaseURL>"

    rewritten = _DASH_BASEURL_RE.sub(_sub_baseurl, text)

    for attr_re, attr_name in zip(_DASH_ATTR_RES, ("media", "initialization", "sourceURL", "index")):
        def _sub_attr(m: "re.Match[str]", _attr=attr_name) -> str:
            raw = m.group(1)
            try:
                abs_url = _proxy_resolve_url(raw, target_url)
            except Exception:
                return m.group(0)
            if not _DASH_TEMPLATE_RE.search(abs_url):
                segment_abs_urls.append(abs_url)
            return f'{_attr}="{build_proxy_url(worker_base, abs_url, referer)}"'

        rewritten = attr_re.sub(_sub_attr, rewritten)

    is_live = bool(re.search(r'type="dynamic"', text, re.IGNORECASE))
    return rewritten, segment_abs_urls, is_live


# ── In-memory segment/manifest cache ────────────────────────────────────────
# Same purpose as universal-proxy's memCache: bounds total memory via a hard
# byte cap (not just TTL) with oldest-inserted-first eviction, so prefetching
# ahead of playback can't OOM the process. Per-process only — fine for a
# single Railway instance, not shared across replicas.
_MAX_CACHE_BYTES = int(os.environ.get("MAX_CACHE_BYTES", str(150 * 1024 * 1024)))  # 150MB default
_SEGMENT_CACHE_TTL = int(os.environ.get("SEGMENT_CACHE_TTL", "21600"))  # 6h
_MANIFEST_CACHE_TTL = int(os.environ.get("MANIFEST_CACHE_TTL", "300"))  # 5min
_ENABLE_PREFETCH = os.environ.get("ENABLE_PREFETCH", "true").lower() != "false"
_PREFETCH_COUNT = int(os.environ.get("PREFETCH_COUNT", "64"))
_PREFETCH_CONCURRENCY = int(os.environ.get("PREFETCH_CONCURRENCY", "8"))

_cache: "OrderedDict[str, dict]" = OrderedDict()  # key -> {body, headers, expires_at, size}
_cache_bytes = 0
_cache_lock = threading.Lock()


def _cache_evict_until_under_cap(needed_bytes: int) -> None:
    global _cache_bytes
    while _cache and _cache_bytes + needed_bytes > _MAX_CACHE_BYTES:
        _, entry = _cache.popitem(last=False)  # oldest-inserted first (insertion order)
        _cache_bytes -= entry["size"]


def _cache_get(key: str) -> Optional[dict]:
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        if entry["expires_at"] < time.time():
            del _cache[key]
            global _cache_bytes
            _cache_bytes -= entry["size"]
            return None
        _cache.move_to_end(key)  # mark as most-recently-used
        return entry


def _cache_set(key: str, body: bytes, headers: dict, ttl_seconds: int) -> None:
    global _cache_bytes
    if ttl_seconds <= 0:
        return  # no-store
    size = len(body)
    if size > _MAX_CACHE_BYTES:
        return  # single entry too big to ever fit
    with _cache_lock:
        existing = _cache.pop(key, None)
        if existing is not None:
            _cache_bytes -= existing["size"]
        _cache_evict_until_under_cap(size)
        _cache[key] = {"body": body, "headers": headers, "expires_at": time.time() + ttl_seconds, "size": size}
        _cache_bytes += size


def _prefetch_ahead(worker_base: str, segment_urls: list[str], referer: str) -> None:
    """Best-effort background fetch of the next N segments so the player
    doesn't wait on them individually. Runs in a daemon thread pool so it
    never blocks or fails the manifest response that triggered it. Mirrors
    universal-proxy's prefetchAhead(), batched at PREFETCH_CONCURRENCY."""
    if not _ENABLE_PREFETCH or not segment_urls:
        return

    targets = segment_urls[:_PREFETCH_COUNT]

    def _fetch_one(abs_url: str) -> None:
        proxied = build_proxy_url(worker_base, abs_url, referer)
        if _cache_get(proxied) is not None:
            return
        try:
            headers = _proxy_upstream_headers(referer, None)
            upstream = requests.get(abs_url, headers=headers, timeout=20, allow_redirects=True)
            if upstream.status_code >= 400:
                return
            content_type = upstream.headers.get("content-type", "")
            is_real_media = any(
                s in content_type.lower() for s in ("video", "audio", "octet-stream", "mp4")
            )
            resp_headers = {
                "Content-Type": content_type if is_real_media else "application/octet-stream",
                "Cache-Control": f"public, max-age={_SEGMENT_CACHE_TTL}",
                "Accept-Ranges": "bytes",
            }
            cl = upstream.headers.get("content-length")
            if cl:
                resp_headers["Content-Length"] = cl
            _cache_set(proxied, upstream.content, resp_headers, _SEGMENT_CACHE_TTL)
        except Exception:
            pass  # best-effort — never surfaces to the caller

    def _run() -> None:
        with ThreadPoolExecutor(max_workers=_PREFETCH_CONCURRENCY) as pool:
            pool.map(_fetch_one, targets)

    threading.Thread(target=_run, daemon=True).start()


@app.get("/proxy")
def proxy(request: Request, token: str = Query(...)):
    """Manifest-aware proxy. Takes an opaque encrypted `token` (see
    proxy_token.py) rather than a plaintext url/referer — decrypts it to
    recover the real upstream URL, then falls back to binary passthrough
    for anything that turns out not to actually be an HLS/DASH manifest on
    arrival. Also does everything universal-proxy's Railway service does:
    DASH (.mpd) rewriting, a byte-capped in-memory segment/manifest cache,
    and background prefetching of upcoming segments — all folded into
    VidNest's own inbuilt proxy instead of a separate deployment.

    For direct files (.mkv/.mp4/...) the token carries no `ref` (see
    build_direct_proxy_url), so no Referer/Origin gets sent upstream —
    those links don't need it. Manifest/segment tokens do carry a `ref`
    and get it forwarded as before."""
    decoded = verify_proxy_token(token)
    if decoded is None:
        raise HTTPException(status_code=403, detail="Invalid or expired token")

    url = decoded["url"]
    referer = decoded.get("ref") or ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Invalid upstream url in token")

    worker_base = _https_worker_base(request)
    range_header = request.headers.get("range")

    # Cache lookup — keyed by the raw token itself (stable per upstream
    # url+referer+exp), same as universal-proxy keying by the full proxied
    # URL. Skipped for ranged requests since a cached full-body entry
    # wouldn't satisfy a partial-content request correctly.
    guessed_kind = _proxy_content_kind(url, "")
    is_manifest_guess = guessed_kind in ("hls", "dash")
    is_segment_guess = guessed_kind == "binary"
    cache_key = token

    if not range_header and (is_manifest_guess or is_segment_guess):
        cached = _cache_get(cache_key)
        if cached is not None:
            headers = {**cached["headers"], "Access-Control-Allow-Origin": "*", "X-Edge-Cache": "HIT"}
            headers.setdefault("Accept-Ranges", "bytes")
            return Response(content=cached["body"], status_code=200, headers=headers)

    upstream_headers = _proxy_upstream_headers(referer, range_header)

    try:
        upstream = requests.get(url, headers=upstream_headers, stream=True, timeout=30, allow_redirects=True)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Failed to reach upstream: {e}")

    if upstream.status_code >= 400:
        raise HTTPException(status_code=upstream.status_code, detail=f"Upstream returned HTTP {upstream.status_code}")

    content_type = upstream.headers.get("content-type", "")
    kind = _proxy_content_kind(url, content_type)

    if kind == "subtitle":
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            media_type=content_type if "vtt" in content_type.lower() else "text/vtt; charset=utf-8",
            headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=3600"},
        )

    if kind == "hls":
        text = upstream.text
        rewritten = _rewrite_hls(text, worker_base, url, referer)
        is_live = "#EXT-X-PLAYLIST-TYPE:VOD" not in text.upper() and "#EXT-X-ENDLIST" not in text.upper()

        # Reuse _rewrite_hls's own line-walk to also collect absolute segment
        # URLs for prefetching — cheap re-derivation rather than threading a
        # second return value through the existing helper.
        segment_urls = []
        base = url
        for line in text.split("\n"):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            try:
                segment_urls.append(_proxy_resolve_url(stripped, base))
            except Exception:
                pass
        if segment_urls:
            _prefetch_ahead(worker_base, segment_urls, referer)

        manifest_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store" if is_live else f"public, max-age={_MANIFEST_CACHE_TTL}",
        }
        if not is_live:
            _cache_set(cache_key, rewritten.encode("utf-8"), {**manifest_headers, "Content-Type": "application/vnd.apple.mpegurl"}, _MANIFEST_CACHE_TTL)
        return Response(content=rewritten, status_code=upstream.status_code, media_type="application/vnd.apple.mpegurl", headers=manifest_headers)

    if kind == "dash":
        text = upstream.text
        rewritten, segment_urls, is_live = _rewrite_dash(text, worker_base, url, referer)
        if segment_urls:
            _prefetch_ahead(worker_base, segment_urls, referer)

        manifest_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store" if is_live else f"public, max-age={_MANIFEST_CACHE_TTL}",
        }
        if not is_live:
            _cache_set(cache_key, rewritten.encode("utf-8"), {**manifest_headers, "Content-Type": "application/dash+xml"}, _MANIFEST_CACHE_TTL)
        return Response(content=rewritten, status_code=upstream.status_code, media_type="application/dash+xml", headers=manifest_headers)

    # Binary passthrough — reached only if a URL that looked manifest-like
    # by extension turned out to actually be a video file/segment.
    res_headers = {
        "Content-Type": content_type or "application/octet-stream",
        "Cache-Control": f"public, max-age={_SEGMENT_CACHE_TTL}",
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
    }
    if "content-range" in upstream.headers:
        res_headers["Content-Range"] = upstream.headers["content-range"]
    if "content-length" in upstream.headers:
        res_headers["Content-Length"] = upstream.headers["content-length"]

    status_code = upstream.status_code if upstream.status_code in (200, 206) else 200

    # Only cacheable when it's a plain (non-ranged) 200 — a 206 partial
    # response body isn't the full segment, so caching it under the same key
    # would corrupt later full-body reads. Buffer into memory here (instead
    # of streaming) specifically so we CAN cache it; segments are small
    # enough (~1-5MB) that this is fine.
    if is_segment_guess and status_code == 200 and not range_header:
        body = upstream.content
        _cache_set(cache_key, body, res_headers, _SEGMENT_CACHE_TTL)
        return Response(content=body, status_code=status_code, headers=res_headers)

    def _stream_body():
        try:
            for chunk in upstream.iter_content(chunk_size=65536):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    return StreamingResponse(_stream_body(), status_code=status_code, headers=res_headers)
