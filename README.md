# VidNest API — Vercel deploy

Same FastAPI app, same endpoints, same proxy-token protection, same
subtitle logic — just wired for Vercel via `vercel.json` in addition to
the existing `railway.json`. Both deploy targets work off the same
`api/index.py`; nothing in the app code changed for this.

## Deploy

```
npm i -g vercel
vercel
```

Or import the repo in the Vercel dashboard. Set the environment variables
below in the Vercel project settings (Settings → Environment Variables) —
same variables as `env.example` / Railway, nothing Vercel-specific to add.

- `ALLOWED_ORIGINS` — set this in production; unset means the origin gate
  runs open (dev mode).
- `PROXY_SIGNING_SECRET` — **set a real value in production.** Unset falls
  back to a hardcoded dev secret baked into the source, which means anyone
  with the source can forge/decrypt proxy tokens.
- `PROXY_TOKEN_TTL_SECONDS`, `MAX_CACHE_BYTES`, `SEGMENT_CACHE_TTL`,
  `MANIFEST_CACHE_TTL`, `ENABLE_PREFETCH`, `PREFETCH_COUNT`,
  `PREFETCH_CONCURRENCY` — optional, same defaults as Railway.

## Serverless caveats specific to Vercel (not an issue on Railway)

The `/proxy` route's in-memory cache (`_cache` in `api/index.py`) and its
background-thread segment prefetch (`_prefetch_ahead`) were written for a
long-running process — that's what Railway gives it. Vercel functions are
different in two ways that affect both:

1. **The in-memory cache doesn't reliably persist across requests.** Vercel
   may reuse a warm instance for consecutive requests (in which case the
   cache behaves normally for that stretch) or spin up a fresh one per
   request/after idle — there's no guarantee the same process handles the
   next request the way there is on Railway. Expect a lower cache hit rate
   than on Railway, not a broken cache.
2. **Background prefetch threads aren't guaranteed to finish.** `_run()` is
   kicked off in a daemon thread just before the manifest response returns;
   Vercel can freeze/tear down the function shortly after the response is
   sent, which can cut prefetch work short mid-flight.

Neither of these breaks functionality — every prefetch/cache path is
already best-effort (wrapped in `try/except`, a cache miss just means a
normal on-demand fetch happens instead) — but the prefetch optimization
will likely do less on Vercel than it does on Railway. If prefetching
segments ahead of playback matters for your use case, Railway (or another
always-on host) is the better fit for `/proxy`; Vercel is fine for the
`/api/stream/*` resolution endpoints either way.
