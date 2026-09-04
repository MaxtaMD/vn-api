#!/usr/bin/env python3
"""
VidNest Resolver - Standalone Version
Returns JSON with all backend results and headers separated
Compatible with AndroResolveURL
"""

import re
import json
import time
import threading
from queue import Queue
import urllib.request
import urllib.error
from urllib.parse import urlparse
import ssl

# Custom alphabet used by VidNest for encoding
VIDNEST_ALPHABET = "RB0fpH8ZEyVLkv7c2i6MAJ5u3IKFDxlS1NTsnGaqmXYdUrtzjwObCgQP94hoeW+/="

# Backend configurations
# One canonical entry per upstream backend path.
# Keep this list unique by `path` so aliases/casing variants cannot cause
# duplicate upstream requests or duplicate result rows.
BACKENDS = [
    {'name': 'MoviesAPI', 'path': 'moviesapi'},
    {'name': 'HollyMovieHD', 'path': 'hollymoviehd'},
    {'name': 'AllMovies', 'path': 'allmovies'},
    {'name': 'VidLink', 'path': 'vidlink'},
    {'name': 'KlikXXI', 'path': 'klikxxi'},
    {'name': 'MovieBox', 'path': 'moviebox'},
    {'name': 'Videasy', 'path': 'videasy'},
    {'name': 'NextgenCloudFabric', 'path': 'nextgencloudfabric'},
    {'name': 'Vidxyz', 'path': 'vidxyz'},
    {'name': 'Vidzee', 'path': 'vidzee'},
    {'name': 'Buzz', 'path': 'buzz'},
    {'name': 'Rogflix', 'path': 'rogflix'},
]

USER_AGENTS = {
    'default': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'api': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'mobile': 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36'
}


class VidNestResolver:
    def __init__(self, debug=False):
        self.debug = debug
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

    def log(self, message, level="INFO"):
        if self.debug or level == "ERROR":
            print(f"[{level}] {message}")

    def _fetch_url(self, url, headers=None, timeout=15):
        if headers is None:
            headers = {
                'User-Agent': USER_AGENTS['default'],
                'Accept': 'application/json, */*',
            }
        try:
            req = urllib.request.Request(url, headers=headers)
            response = urllib.request.urlopen(req, timeout=timeout, context=self.ssl_context)
            content = response.read().decode('utf-8', errors='ignore')
            return True, content, None
        except urllib.error.HTTPError as e:
            return False, None, f"HTTP Error {e.code}: {e.reason}"
        except urllib.error.URLError as e:
            return False, None, f"URL Error: {str(e)}"
        except Exception as e:
            return False, None, f"Error: {str(e)}"

    def resolve_dict(self, url_or_id, media_type='movie', season=None, episode=None):
        """Same as resolve() but returns the dict directly (no json.dumps round trip)."""
        self.log("=" * 80)
        self.log(f"VidNest Resolver Started - {media_type}")

        if url_or_id.startswith('http'):
            match = re.search(r'/(?:movie|tv)/(\d+)', url_or_id)
            if not match:
                return {'status': 'error', 'message': 'Could not extract media ID from URL'}
            media_id = match.group(1)
            if '/tv/' in url_or_id:
                media_type = 'tv'
                se_match = re.search(r'/tv/\d+/(\d+)/(\d+)', url_or_id)
                if se_match:
                    season = int(se_match.group(1))
                    episode = int(se_match.group(2))
        else:
            media_id = url_or_id

        self.log(f"Media ID: {media_id}")
        self.log(f"Content Type: {'TV Show' if media_type == 'tv' else 'Movie'}")
        if media_type == 'tv':
            self.log(f"Season: {season}, Episode: {episode}")

        tmdb_id = media_id
        results = self._try_all_backends(tmdb_id, media_type, season, episode)
        json_response = self._build_json_response(results)
        self._log_results_summary(results)

        self.log("=" * 80)
        return json_response

    def resolve_anime_dict(self, anilist_id, episode=1, sub_or_dub='sub'):
        """
        Anime resolution via the hianime backend. Different shape from
        movie/tv: keyed by AniList ID + episode + sub/dub instead of a
        TMDB ID + season, so it doesn't fit _try_all_backends/BACKENDS —
        it's a single dedicated backend, not a fan-out across many.
        Reuses the same decrypt/parse/header pipeline as every other
        backend so any future backend-agnostic fixes (like the 'file' vs
        'url' key fix) apply here too.
        """
        self.log("=" * 80)
        self.log(f"VidNest Anime Resolver Started - anilist:{anilist_id} ep:{episode} ({sub_or_dub})")

        result_queue = Queue()
        thread = threading.Thread(
            target=self._try_hianime_thread,
            args=(anilist_id, episode, sub_or_dub, result_queue)
        )
        thread.start()
        thread.join(timeout=15)

        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        json_response = self._build_json_response(results)
        self._log_results_summary(results)
        self.log("=" * 80)
        return json_response

    def _try_hianime_thread(self, anilist_id, episode, sub_or_dub, result_queue):
        result = {
            'backend': 'HiAnime',
            'path': 'hianime',
            'success': False,
            'url': None,
            'headers': None,
            'error': None,
            'response_time': 0,
            'raw_data': None,
            'subtitles': [],
            'intro': None,
            'outro': None
        }

        start_time = time.time()

        try:
            api_url = f'https://new.vidnest.fun/hianime/anime/{anilist_id}/{episode}/{sub_or_dub}'
            headers = {
                'User-Agent': USER_AGENTS['api'],
                'Accept': 'application/json, */*',
                'Origin': 'https://vidnest.fun',
                'Referer': 'https://vidnest.fun/',
            }

            success, content, error = self._fetch_url(api_url, headers)

            if not success:
                result['error'] = error
                result['response_time'] = time.time() - start_time
                result_queue.put(result)
                return

            response_data = json.loads(content)
            result['raw_data'] = response_data

            if response_data.get('encrypted', False):
                encrypted_data = response_data.get('data', '')
                decrypted_data = self._decrypt_vidnest(encrypted_data)
                if decrypted_data:
                    stream_url = self._parse_stream_data(decrypted_data)
                    if stream_url:
                        result['url'] = stream_url
                        result['success'] = True
                        result['headers'] = self._get_headers_for_url(stream_url, 'hianime')
                        self._attach_anime_extras(result, decrypted_data)
            else:
                stream_url = self._parse_stream_data(response_data)
                if stream_url:
                    result['url'] = stream_url
                    result['success'] = True
                    result['headers'] = self._get_headers_for_url(stream_url, 'hianime')
                    self._attach_anime_extras(result, response_data)

        except Exception as e:
            result['error'] = str(e)
            result['success'] = False

        result['response_time'] = time.time() - start_time
        result_queue.put(result)

    def _attach_anime_extras(self, result, data):
        """Pull subtitle tracks and intro/outro skip timestamps out of the
        hianime payload. These live alongside 'sources' in the same decrypted
        object that _parse_stream_data reads the video URL from."""
        if not isinstance(data, dict):
            return

        tracks = data.get('tracks')
        if isinstance(tracks, list):
            subtitles = []
            for track in tracks:
                if not isinstance(track, dict):
                    continue
                file_url = track.get('file')
                if not file_url:
                    continue
                if file_url.startswith('//'):
                    file_url = 'https:' + file_url
                subtitles.append({
                    'url': file_url,
                    'label': track.get('label', ''),
                    'kind': track.get('kind', 'captions'),
                    'default': bool(track.get('default', False)),
                })
            result['subtitles'] = subtitles

        intro = data.get('intro')
        if isinstance(intro, dict):
            result['intro'] = {'start': intro.get('start'), 'end': intro.get('end')}

        outro = data.get('outro')
        if isinstance(outro, dict):
            result['outro'] = {'start': outro.get('start'), 'end': outro.get('end')}


    def resolve(self, url_or_id, media_type='movie', season=None, episode=None):
        self.log("=" * 80)
        self.log(f"VidNest Resolver Started - {media_type}")

        if url_or_id.startswith('http'):
            match = re.search(r'/(?:movie|tv)/(\d+)', url_or_id)
            if not match:
                return json.dumps({
                    'status': 'error',
                    'message': 'Could not extract media ID from URL'
                })
            media_id = match.group(1)
            if '/tv/' in url_or_id:
                media_type = 'tv'
                se_match = re.search(r'/tv/\d+/(\d+)/(\d+)', url_or_id)
                if se_match:
                    season = int(se_match.group(1))
                    episode = int(se_match.group(2))
        else:
            media_id = url_or_id

        self.log(f"Media ID: {media_id}")
        self.log(f"Content Type: {'TV Show' if media_type == 'tv' else 'Movie'}")
        if media_type == 'tv':
            self.log(f"Season: {season}, Episode: {episode}")

        tmdb_id = media_id
        self.log(f"TMDB ID: {tmdb_id}")

        self.log(f"Starting parallel backend resolution...")
        self.log(f"Total backends to try: {len(BACKENDS)}")

        results = self._try_all_backends(tmdb_id, media_type, season, episode)
        json_response = self._build_json_response(results)
        self._log_results_summary(results)

        self.log("=" * 80)
        return json.dumps(json_response, indent=2)

    def _try_all_backends(self, tmdb_id, media_type='movie', season=1, episode=1):
        results = []
        threads = []
        result_queue = Queue()

        # Defensive config de-duplication. Even if a future edit accidentally
        # adds another alias such as MoviesApi/moviesapi or Vidlink/vidlink,
        # only one upstream request is fired for that canonical path.
        unique_backends = []
        seen_paths = set()
        for backend in BACKENDS:
            canonical_path = str(backend.get('path', '')).strip().lower()
            if not canonical_path or canonical_path in seen_paths:
                continue
            seen_paths.add(canonical_path)
            unique_backends.append(backend)

        for backend in unique_backends:
            thread = threading.Thread(
                target=self._try_backend_thread,
                args=(tmdb_id, backend, media_type, season, episode, result_queue)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join(timeout=15)

        while not result_queue.empty():
            results.append(result_queue.get())

        # Defensive result de-duplication by backend path.
        # Prefer a successful result; otherwise keep the faster attempt.
        deduped = {}
        order = []
        for result in results:
            key = str(result.get('path') or result.get('backend') or '').strip().lower()
            if not key:
                key = f"__row_{len(order)}"

            if key not in deduped:
                deduped[key] = result
                order.append(key)
                continue

            current = deduped[key]
            replace = False
            if result.get('success') and not current.get('success'):
                replace = True
            elif bool(result.get('success')) == bool(current.get('success')):
                if float(result.get('response_time') or 999999) < float(current.get('response_time') or 999999):
                    replace = True

            if replace:
                deduped[key] = result

        return [deduped[key] for key in order]

    def _try_backend_thread(self, tmdb_id, backend, media_type, season, episode, result_queue):
        result = {
            'backend': backend['name'],
            'path': backend['path'],
            'success': False,
            'url': None,
            'headers': None,
            'error': None,
            'response_time': 0,
            'raw_data': None,
            'subtitles': []
        }

        start_time = time.time()

        try:
            self.log(f"Thread started for backend: {backend['name']}", "DEBUG")

            if media_type == 'tv':
                # Use season and episode from parameters
                api_url = f'https://new.vidnest.fun/{backend["path"]}/tv/{tmdb_id}/{season}/{episode}'
            else:
                api_url = f'https://new.vidnest.fun/{backend["path"]}/movie/{tmdb_id}'

            headers = {
                'User-Agent': USER_AGENTS['api'],
                'Accept': 'application/json, */*',
                'Origin': 'https://vidnest.fun',
                'Referer': 'https://vidnest.fun/',
            }

            success, content, error = self._fetch_url(api_url, headers)

            if not success:
                result['error'] = error
                result['response_time'] = time.time() - start_time
                result_queue.put(result)
                return

            response_data = json.loads(content)
            result['raw_data'] = response_data

            if response_data.get('encrypted', False):
                encrypted_data = response_data.get('data', '')
                decrypted_data = self._decrypt_vidnest(encrypted_data)
                if decrypted_data:
                    stream_url = self._parse_stream_data(decrypted_data)
                    if stream_url:
                        result['url'] = stream_url
                        result['success'] = True
                        result['headers'] = self._get_headers_for_url(stream_url, backend['path'])
            else:
                stream_url = self._parse_stream_data(response_data)
                if stream_url:
                    result['url'] = stream_url
                    result['success'] = True
                    result['headers'] = self._get_headers_for_url(stream_url, backend['path'])

            # Subtitle tracks for movie/TV don't come from the backend's own
            # JSON — confirmed empty on every decrypted movie/TV payload
            # inspected (MoviesAPI, NextgenCloudFabric: no tracks/subtitles/
            # captions key of any kind). VidNest's own player instead pulls
            # them from a separate, backend-agnostic subtitle CDN keyed
            # purely by TMDB id (see _fetch_vdrk_subtitles below) — so this
            # only needs the tmdb_id/media_type/season/episode already in
            # scope here, independent of which backend resolved the video.
            if result['success']:
                result['subtitles'] = self._fetch_vdrk_subtitles(tmdb_id, media_type, season, episode)

        except Exception as e:
            result['error'] = str(e)
            result['success'] = False

        result['response_time'] = time.time() - start_time
        result_queue.put(result)

    # Languages to probe on the vdrk subtitle CDN. There's no discovery/list
    # endpoint for this service (none found), so this is a fixed allowlist
    # tried via HEAD request per language — only languages that actually
    # exist (200 OK) end up in the result. Extend this list if you confirm
    # more language names the CDN serves.
    _VDRK_SUBTITLE_LANGUAGES = [
        'English', 'Spanish', 'French', 'German', 'Italian', 'Portuguese',
        'Arabic', 'Hindi', 'Tamil', 'Telugu', 'Russian', 'Japanese',
        'Korean', 'Chinese', 'Turkish', 'Indonesian', 'Vietnamese', 'Thai',
    ]

    def _fetch_vdrk_subtitles(self, tmdb_id, media_type, season=None, episode=None):
        """Probes cache.vdrk.site for WEBVTT subtitle tracks, keyed by TMDB
        id alone — confirmed working directly against the CDN for both shapes:
          movie: /v2/movie/{tmdb_id}/{Language}.vtt
          tv:    /v2/tv/{tmdb_id}/{season}/{episode}/{Language}.vtt
        Both return a real WEBVTT file directly, no encryption/backend
        involved. Runs one HEAD request per candidate language in parallel;
        languages that 404 are silently dropped. Best-effort: any failure
        here just means an empty subtitles list, never breaks stream
        resolution.
        """
        subtitles = []
        result_queue = Queue()
        threads = []

        def check_language(language):
            if media_type == 'tv' and season is not None and episode is not None:
                url = f'https://cache.vdrk.site/v2/tv/{tmdb_id}/{season}/{episode}/{language}.vtt'
            else:
                url = f'https://cache.vdrk.site/v2/movie/{tmdb_id}/{language}.vtt'
            try:
                req = urllib.request.Request(url, method='HEAD', headers={'User-Agent': USER_AGENTS['default']})
                resp = urllib.request.urlopen(req, timeout=6, context=self.ssl_context)
                if resp.status == 200:
                    result_queue.put({'label': language, 'lang': language, 'url': url})
            except Exception:
                pass  # 404 / timeout / anything else -> this language isn't available, skip it

        for lang in self._VDRK_SUBTITLE_LANGUAGES:
            t = threading.Thread(target=check_language, args=(lang,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=8)

        while not result_queue.empty():
            subtitles.append(result_queue.get())

        return subtitles



    # Backends whose CDN validates Referer/Origin against a *different*
    # domain than the stream URL itself — auto-deriving from the stream
    # URL's own domain would be wrong for these, so they need an explicit
    # override. Keyed by backend path (not URL substring) since some CDN
    # hostnames rotate/are randomized per-request and can't be reliably
    # pattern-matched. Add entries here only when a backend is confirmed
    # to need a domain other than its own stream host.
    _HEADER_OVERRIDES = {
        'vidzee': {'Referer': 'https://core.vidzee.wtf/', 'Origin': 'https://core.vidzee.wtf'},
        'nextgencloudfabric': {'Referer': 'https://nextgencloudfabric.com/', 'Origin': 'https://nextgencloudfabric.com'},
        'hianime': {'Referer': 'https://megaplay.buzz/', 'Origin': 'https://megaplay.buzz'},
        'klikxxi': {'Referer': 'https://vidnest.fun/', 'Origin': 'https://vidnest.fun'},
    }

    def _get_headers_for_url(self, url, backend_path=None):
        # Stream URLs differ per backend and even per request (rotating
        # CDN hosts, tokens, etc.), so headers can't be hardcoded to one
        # fixed domain. Instead, derive Origin/Referer from the stream
        # URL's own host — that's what most CDNs actually validate
        # against — and only fall back to a hardcoded override for
        # backends confirmed to check a different domain.
        parsed = urlparse(url)
        host = parsed.netloc

        headers = {
            'User-Agent': USER_AGENTS['default'],
            'Accept': '*/*',
        }

        override = self._HEADER_OVERRIDES.get(backend_path) if backend_path else None

        if override:
            headers['Referer'] = override['Referer']
            headers['Origin'] = override['Origin']
        elif host:
            headers['Referer'] = f'{parsed.scheme}://{host}/'
            headers['Origin'] = f'{parsed.scheme}://{host}'
        else:
            # Malformed/relative URL — fall back to the vidnest.fun
            # referer/origin rather than sending an empty header.
            headers['Referer'] = 'https://vidnest.fun/'
            headers['Origin'] = 'https://vidnest.fun'

        return headers

    def _decrypt_vidnest(self, data):
        if not data:
            return None

        try:
            lookup = {char: idx for idx, char in enumerate(VIDNEST_ALPHABET)}
            result = bytearray()
            i = 0

            while i < len(data):
                chunk = data[i:i+4]
                while len(chunk) < 4:
                    chunk += '='

                vals = []
                for char in chunk:
                    if char in lookup:
                        vals.append(lookup[char])
                    else:
                        vals.append(64)

                if len(vals) >= 4:
                    result.append((vals[0] << 2) | (vals[1] >> 4))
                    if vals[2] != 64:
                        result.append(((vals[1] & 15) << 4) | (vals[2] >> 2))
                    if vals[3] != 64:
                        result.append(((vals[2] & 3) << 6) | vals[3])

                i += 4

            try:
                decoded = result.decode('utf-8')
                return json.loads(decoded)
            except:
                result_str = result.decode('utf-8', errors='ignore')
                json_match = re.search(r'\{.*\}', result_str, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group(0))
                    except:
                        pass
                return result_str

        except Exception as e:
            self.log(f"Decryption error: {str(e)}", "ERROR")
            return None

    def _parse_stream_data(self, data):
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except:
                json_match = re.search(r'\{.*\}', data, re.DOTALL)
                if json_match:
                    try:
                        data = json.loads(json_match.group(0))
                    except:
                        return None
                else:
                    return None

        if not isinstance(data, dict):
            return None

        if 'sources' in data and data['sources']:
            sources = data['sources']
            if isinstance(sources, list) and sources:
                for source in sources:
                    if isinstance(source, dict):
                        # Different backends use different key names for the
                        # same field — vidnest's movie/tv backends use 'url',
                        # hianime uses 'file'.
                        url = source.get('url') or source.get('file')
                        if url:
                            if url.startswith('//'):
                                url = 'https:' + url
                            if self._is_valid_stream_url(url):
                                return url

        if 'streams' in data and data['streams']:
            streams = data['streams']
            if isinstance(streams, list) and streams:
                for stream in streams:
                    if isinstance(stream, dict) and stream.get('url'):
                        url = stream['url']
                        if url.startswith('//'):
                            url = 'https:' + url
                        if self._is_valid_stream_url(url):
                            return url

        if 'data' in data and isinstance(data['data'], dict):
            if 'downloads' in data['data']:
                downloads = data['data']['downloads']
                if isinstance(downloads, list) and downloads:
                    best = None
                    best_res = 0
                    for dl in downloads:
                        if isinstance(dl, dict) and dl.get('url'):
                            res = dl.get('resolution', 0)
                            if res > best_res:
                                best_res = res
                                best = dl
                    if best and best.get('url'):
                        url = best['url']
                        if url.startswith('//'):
                            url = 'https:' + url
                        if self._is_valid_stream_url(url):
                            return url

        if 'url' in data and data['url']:
            url = data['url']
            if url.startswith('//'):
                url = 'https:' + url
            if self._is_valid_stream_url(url):
                return url

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str) and (value.startswith('http') or value.startswith('//')):
                    if any(ext in value.lower() for ext in ['.m3u8', '.mp4', '.ts', '.webm']):
                        if value.startswith('//'):
                            value = 'https:' + value
                        return value

        return None

    def _is_valid_stream_url(self, url):
        if not url:
            return False
        # .mkv is deliberately excluded: MKV containers don't support HTTP
        # range/progressive playback in browsers or most players, so a
        # player trying to "stream" one ends up attempting a full-file
        # download and times out instead of playing. Only formats that
        # actually support seekable HTTP streaming are accepted.
        streamable_extensions = ['.m3u8', '.mp4', '.ts', '.webm']
        if any(ext in url.lower() for ext in streamable_extensions):
            return True
        if any(domain in url.lower() for domain in ['video', 'stream', 'cdn']):
            return True
        if url.startswith(('http://', 'https://')):
            return True
        return False

    def _build_json_response(self, results):
        # Final safety net: collapse duplicate backend paths before counts and
        # response serialization. This protects callers even if results come
        # from a custom/legacy resolver path rather than _try_all_backends().
        unique_results = []
        by_path = {}
        order = []

        for result in results:
            key = str(result.get('path') or result.get('backend') or '').strip().lower()
            if not key:
                key = f"__row_{len(order)}"

            if key not in by_path:
                by_path[key] = result
                order.append(key)
                continue

            current = by_path[key]
            replace = False
            if result.get('success') and not current.get('success'):
                replace = True
            elif bool(result.get('success')) == bool(current.get('success')):
                if float(result.get('response_time') or 999999) < float(current.get('response_time') or 999999):
                    replace = True

            if replace:
                by_path[key] = result

        unique_results = [by_path[key] for key in order]

        successful = [r for r in unique_results if r['success']]
        failed = [r for r in unique_results if not r['success']]

        response = {
            'status': 'success' if successful else 'error',
            'total_backends': len(unique_results),
            'successful_backends': len(successful),
            'failed_backends': len(failed),
            'results': [],
            'playable_urls': []
        }

        seen_playable_urls = set()

        for result in unique_results:
            result_data = {
                'backend': result['backend'],
                'path': result['path'],
                'success': result['success'],
                'response_time': round(result['response_time'], 3),
                'error': result.get('error')
            }

            if result['success'] and result['url']:
                headers = result.get('headers', {})
                result_data['url'] = result['url']
                result_data['headers'] = headers

                # De-duplicate subtitle tracks inside an individual backend
                # result by normalized URL (falling back to language/label).
                subtitles = result.get('subtitles') or []
                if subtitles:
                    unique_subtitles = []
                    seen_subtitles = set()
                    for sub in subtitles:
                        if not isinstance(sub, dict):
                            continue
                        key = str(sub.get('url') or '').strip()
                        if not key:
                            key = (
                                str(sub.get('lang') or sub.get('label') or '').strip().lower()
                                + '|'
                                + str(sub.get('kind') or '').strip().lower()
                            )
                        if not key or key in seen_subtitles:
                            continue
                        seen_subtitles.add(key)
                        unique_subtitles.append(sub)
                    subtitles = unique_subtitles

                playable_entry = {
                    'backend': result['backend'],
                    'server': result['backend'],
                    'url': result['url'],
                    'headers': headers
                }

                if subtitles:
                    result_data['subtitles'] = subtitles
                    playable_entry['subtitles'] = subtitles
                if result.get('intro'):
                    result_data['intro'] = result['intro']
                    playable_entry['intro'] = result['intro']
                if result.get('outro'):
                    result_data['outro'] = result['outro']
                    playable_entry['outro'] = result['outro']

                # Same final stream URL from multiple aliases/backends should
                # only appear once in playable_urls. The first canonical
                # backend wins.
                stream_key = str(result['url']).strip()
                if stream_key and stream_key not in seen_playable_urls:
                    seen_playable_urls.add(stream_key)
                    response['playable_urls'].append(playable_entry)

            response['results'].append(result_data)

        def url_priority(item):
            url = item['url'].lower()
            if '.mp4' in url:
                return 3
            elif '.m3u8' in url:
                return 2
            else:
                return 1

        response['playable_urls'].sort(key=url_priority, reverse=True)

        return response

    def _log_results_summary(self, results):
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]

        self.log("=" * 80)
        self.log("BACKEND RESULTS SUMMARY:")
        self.log(f"Successful backends: {len(successful)}")
        for r in successful:
            url_preview = r['url'][:100] + '...' if len(r['url']) > 100 else r['url']
            self.log(f"  ✓ {r['backend']}: {url_preview} ({r['response_time']:.2f}s)")

        self.log(f"Failed backends: {len(failed)}")
        for r in failed:
            self.log(f"  ✗ {r['backend']}: {r.get('error', 'Unknown error')} ({r['response_time']:.2f}s)")
        self.log("=" * 80)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='VidNest Resolver')
    parser.add_argument('url_or_id', help='VidNest URL or media ID')
    parser.add_argument('--type', choices=['movie', 'tv'], default='movie', help='Media type (default: movie)')
    parser.add_argument('--season', type=int, default=1, help='Season number (for TV, default: 1)')
    parser.add_argument('--episode', type=int, default=1, help='Episode number (for TV, default: 1)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--pretty', action='store_true', help='Pretty print JSON output')

    args = parser.parse_args()

    resolver = VidNestResolver(debug=args.debug)
    result_json = resolver.resolve(
        args.url_or_id,
        media_type=args.type,
        season=args.season,
        episode=args.episode
    )

    if args.pretty:
        try:
            data = json.loads(result_json)
            print(json.dumps(data, indent=2))
        except:
            print(result_json)
    else:
        print(result_json)


if __name__ == "__main__":
    main()