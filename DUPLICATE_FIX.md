# Duplicate fix

This build removes duplicate backend aliases and adds defensive response de-duplication.

Changes:
- Removed duplicate `MoviesApi -> moviesapi` entry.
- Removed duplicate `Vidlink -> vidlink` entry.
- Backend fan-out de-duplicates by canonical lowercase path.
- Final `results` also de-duplicates by backend path.
- If duplicate path results exist, a successful result wins; otherwise the faster result wins.
- `playable_urls` de-duplicates by exact final stream URL.
- Subtitle tracks inside each backend result de-duplicate by subtitle URL.

Canonical backend count is now 10.
