"""
vidnest.fun extractor core — thin wrapper around VidNestResolver.

Mirrors vidcore_core.py's interface shape (extract_all returning a dict with
servers/playable_urls) so index.py can follow the same pattern used
elsewhere in this project.
"""

from vidnest_resolver import VidNestResolver

VIDNEST_REFERER = "https://vidnest.fun/"


class VidnestError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def extract_all(media_type: str, tmdb_id: str, season=None, episode=None) -> dict:
    """
    Runs every backend in parallel (VidNestResolver's own behavior) and
    returns the raw result dict — same shape the standalone script prints:
      { status, total_backends, successful_backends, failed_backends,
        results: [...], playable_urls: [...] }
    """
    resolver = VidNestResolver(debug=False)
    result = resolver.resolve_dict(tmdb_id, media_type=media_type, season=season, episode=episode)
    if result.get("status") != "success":
        raise VidnestError(result.get("message", "Resolution failed"))
    return result


def extract_anime(anilist_id: str, episode: int = 1, sub_or_dub: str = "sub") -> dict:
    """
    Anime resolution via the hianime backend, keyed by AniList ID + episode
    + sub/dub rather than a TMDB ID. Same result shape as extract_all.
    """
    resolver = VidNestResolver(debug=False)
    result = resolver.resolve_anime_dict(anilist_id, episode=episode, sub_or_dub=sub_or_dub)
    if result.get("status") != "success":
        raise VidnestError(result.get("message", "Resolution failed"))
    return result
