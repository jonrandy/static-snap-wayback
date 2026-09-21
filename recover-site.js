#!/usr/bin/env node
/**
 * recover-site.js
 * -----------------------------------------------------------------------
 * Rebuilds a static snapshot of a lost site (default: foolsgold.co.uk)
 * from the Internet Archive's Wayback Machine.
 *
 * What it does:
 *   1. Queries the CDX API LIVE (not a fixed dump) for every capture
 *      across the whole domain (both www and bare hostname), so galleries
 *      that are missing from any one CDX export still get picked up if
 *      they exist under a different snapshot/host.
 *   2. Groups all captures by the LOCAL FILE they'll resolve to (not by
 *      the raw archived URL string) — because the exact scheme/host
 *      Wayback recorded for the "same" page drifts over a site's life
 *      (http -> https, bare domain -> www, explicit :80, etc.), and
 *      grouping by raw URL would otherwise produce several different
 *      captures all racing to write the same output file concurrently.
 *      Within each group, candidates are sorted newest-first.
 *   3. For each destination file, tries the MOST RECENT capture first;
 *      if that specific capture fails to fetch, falls back to the next
 *      older capture of that same resource, and so on, until one
 *      succeeds or the group is exhausted. This is deliberate: it means
 *      every page/image is the newest version Wayback actually has a
 *      working copy of, not just whichever snapshot happened to exist
 *      when a fixed CDX export was taken.
 *   4. Downloads the raw, un-rewritten bytes of the winning capture
 *      (using the Wayback "id_" modifier, which returns the original
 *      response body with no Wayback banner/link-rewriting injected).
 *   5. Writes everything to disk in a folder structure that mirrors the
 *      original site (query strings are slugified into filenames).
 *   6. Does a best-effort rewrite of links inside downloaded HTML pages
 *      so they point at the local copies instead of the live/Wayback
 *      URLs, so the rebuilt site is browsable offline.
 *   7. Writes a manifest.json + report.json at the end summarising what
 *      succeeded/failed/fell back, so you can re-run just the failures
 *      later, or audit which pages didn't get the very newest snapshot.
 *
 * What it CANNOT do:
 *   - Recover server-side code (the site ran on a Perl CGI script;
 *     Wayback only ever captured that script's *output*, never its
 *     source). The rebuilt "index.cgi" pages are frozen HTML snapshots,
 *     not a working CGI.
 *   - Guarantee every historical asset exists — if Wayback never
 *     crawled it, it's gone for good.
 *
 * Requirements: Node.js 18+ (uses global fetch). No npm deps required.
 *
 * Usage:
 *   node recover-site.js                 # full run
 *   node recover-site.js --domain=example.com --out=./rebuilt
 *   node recover-site.js --retry-failed  # re-run only failed items from
 *                                        # a previous report.json
 * -----------------------------------------------------------------------
 */

'use strict';

const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const { setTimeout: sleep } = require('timers/promises');

// ------------------------------------------------------------------ CONFIG

const args = parseArgs(process.argv.slice(2));

const CONFIG = {
  // The domain to recover. matchType=domain in the CDX query will also
  // pick up www.<domain> and any other subdomains that were crawled.
  domain: args.domain || 'foolsgold.co.uk',

  // Where to write the rebuilt site.
  outDir: path.resolve(args.out || './rebuilt-site'),

  // How many captures to download at once. Archive.org will start
  // throttling/blocking if you go too high — keep this modest.
  concurrency: Number(args.concurrency || 4),

  // Minimum delay (ms) between the *start* of requests, per worker.
  // Combined with concurrency this sets your overall request rate.
  delayMs: Number(args.delay || 300),

  // How many times to retry a single failed download before giving up.
  maxRetries: Number(args.maxRetries || 3),

  // Base backoff (ms) for retries; doubles each attempt.
  retryBackoffMs: Number(args.retryBackoff || 1000),

  // How to order candidates within each destination file's group before
  // walking through them (first one that downloads successfully wins):
  //   'latest'  - try the most recent 200 OK capture first, falling back
  //               to progressively older ones if it fails to fetch
  //   'earliest'- try the oldest 200 OK capture first instead
  pickStrategy: args.pick || 'latest',

  // If true, only re-attempts URLs marked failed in an existing
  // report.json in outDir, instead of doing a fresh CDX query.
  retryFailedOnly: !!args['retry-failed'],
};

const CDX_ENDPOINT = 'https://web.archive.org/cdx/search/cdx';
const REPORT_PATH = path.join(CONFIG.outDir, 'report.json');
const MANIFEST_PATH = path.join(CONFIG.outDir, 'manifest.json');

// -------------------------------------------------------------- CDX QUERY

/**
 * Pull the full CDX listing for the domain. We ask for exactly the
 * fields we need, in JSON form (first row is the header).
 */
async function fetchCdx(domain) {
  const fields = ['urlkey', 'timestamp', 'original', 'mimetype', 'statuscode', 'digest'];
  const url =
    `${CDX_ENDPOINT}?url=${encodeURIComponent(domain)}` +
    `&matchType=domain` +
    `&output=json` +
    `&fl=${fields.join(',')}` +
    `&collapse=digest`; // drop consecutive identical-content captures

  console.log(`[cdx] querying: ${url}`);
  const res = await fetchWithRetry(url, {}, CONFIG.maxRetries);
  const rows = await res.json();

  if (!Array.isArray(rows) || rows.length < 2) {
    console.warn('[cdx] no rows returned — nothing to recover.');
    return [];
  }

  const header = rows[0];
  const idx = Object.fromEntries(header.map((h, i) => [h, i]));

  return rows.slice(1).map((r) => ({
    urlkey: r[idx.urlkey],
    timestamp: r[idx.timestamp],
    original: r[idx.original],
    mimetype: r[idx.mimetype],
    statuscode: r[idx.statuscode],
    digest: r[idx.digest],
  }));
}

/**
 * Group all 200-OK CDX rows by the LOCAL FILE they'll resolve to (via
 * urlToLocalPath), not by the raw "original" URL string. This is the key
 * fix that makes "most recent, fall back over time if missing" actually
 * work: over a site's life the exact scheme/host Wayback recorded for
 * the same page drifts (http -> https, bare domain -> www, explicit
 * :80, etc.), so grouping by raw URL would treat those as different
 * resources — leaving several candidates that all want to write the
 * same destination file, racing each other with no defined winner.
 * Grouping by the resolved destination guarantees exactly one group per
 * output file, and within each group we sort candidates so the newest
 * is tried first.
 */
function groupByLocalPath(rows) {
  const groups = new Map(); // localPath -> candidate rows, unsorted for now

  for (const row of rows) {
    if (row.statuscode !== '200') continue;

    let localPath;
    try {
      localPath = urlToLocalPath(row.original, row.mimetype);
    } catch {
      continue; // malformed "original" URL — skip, nothing sane to do
    }

    if (!groups.has(localPath)) groups.set(localPath, []);
    groups.get(localPath).push(row);
  }

  for (const candidates of groups.values()) {
    candidates.sort((a, b) =>
      CONFIG.pickStrategy === 'earliest'
        ? a.timestamp.localeCompare(b.timestamp)
        : b.timestamp.localeCompare(a.timestamp)
    );
  }

  return groups;
}

// ------------------------------------------------------------- FILESYSTEM

// Mimetypes that should always be saved with a .html extension,
// regardless of what the URL or query string happens to look like.
// (index.cgi?047.jpg is HTML output — the CGI script just took
// "047.jpg" as a parameter — not an actual image.)
const HTML_MIMETYPES = new Set(['text/html', 'application/xhtml+xml']);

/**
 * Map an original URL (e.g. http://foolsgold.co.uk/photos14/index.cgi?047.jpg)
 * to a safe local file path under outDir. Query strings are folded into
 * the filename since the filesystem can't represent them natively.
 * `mimetype` (from the CDX record) decides the final extension so HTML
 * pages never get saved under a misleading image/other extension just
 * because their query string looks like a filename.
 */
function urlToLocalPath(originalUrl, mimetype) {
  const u = new URL(originalUrl);
  let pathname = decodeURIComponent(u.pathname);

  // Directory listing pages ("/", "/photos14/") become index.html
  if (pathname.endsWith('/')) pathname += 'index.html';

  let filePath = pathname.replace(/^\/+/, '');

  if (u.search) {
    const safeQuery = u.search
      .replace(/^\?/, '')
      .replace(/[^a-zA-Z0-9._-]/g, '_');
    // e.g. photos14/index.cgi + ?047.jpg -> photos14/index.cgi__047.jpg
    filePath += `__${safeQuery}`;
  }

  // We only ever APPEND an extension, never strip one — stripping risks
  // silently discarding meaningful bits of the query string (e.g. two
  // different queries "047.jpg" and "047.png" could otherwise collapse
  // onto the same filename), and it can also make an index.cgi (script,
  // no query) collide on disk with its own directory's "/" listing,
  // since both would naively want to become "index.html". Appending
  // instead — index.cgi -> index.cgi.html — mirrors what `wget
  // --adjust-extension` does and keeps every URL's mapping unique.
  const isHtml = HTML_MIMETYPES.has((mimetype || '').toLowerCase());
  const alreadyHtml = /\.html?$/i.test(filePath);

  if (isHtml && !alreadyHtml) {
    filePath += '.html';
  } else if (!isHtml && !path.extname(filePath)) {
    // Non-HTML with no extension at all (rare) — fall back to .bin so
    // it's at least clearly not text.
    filePath += '.bin';
  }

  return path.join(CONFIG.outDir, filePath);
}

// ----------------------------------------------------------------- FETCH

async function fetchWithRetry(url, options, retries) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);
      if (res.ok) return res;
      lastErr = new Error(`HTTP ${res.status} for ${url}`);
    } catch (err) {
      lastErr = err;
    }
    if (attempt < retries) {
      const backoff = CONFIG.retryBackoffMs * 2 ** attempt;
      await sleep(backoff);
    }
  }
  throw lastErr;
}

/**
 * Download one capture's raw bytes. The "id_" modifier after the
 * timestamp tells Wayback to serve the exact original response body,
 * with no HTML rewriting or banner injection.
 */
async function downloadCapture(capture) {
  const waybackUrl = `https://web.archive.org/web/${capture.timestamp}id_/${capture.original}`;
  const res = await fetchWithRetry(waybackUrl, {}, CONFIG.maxRetries);
  const buffer = Buffer.from(await res.arrayBuffer());

  const localPath = urlToLocalPath(capture.original, capture.mimetype);
  await fsp.mkdir(path.dirname(localPath), { recursive: true });
  await fsp.writeFile(localPath, buffer);

  return { localPath, size: buffer.length };
}

/**
 * Try to fill one destination file from a list of candidate captures,
 * newest first. If the newest fails to actually fetch (as opposed to
 * just being an older snapshot — CDX metadata can be stale, or Wayback
 * can flake on a specific capture), fall back to the next older one,
 * and so on, until something succeeds or the candidates run out.
 */
async function downloadGroupWithFallback(candidates) {
  let lastErr;

  for (let i = 0; i < candidates.length; i++) {
    const capture = candidates[i];
    try {
      const { localPath, size } = await downloadCapture(capture);
      await rewriteHtmlLinks(localPath, CONFIG.domain);
      return {
        localPath,
        size,
        usedTimestamp: capture.timestamp,
        usedOriginal: capture.original,
        skippedNewerCaptures: candidates.slice(0, i).map((c) => c.timestamp),
      };
    } catch (err) {
      lastErr = err;
      if (i < candidates.length - 1) {
        console.warn(
          `  [fallback] ${capture.original} @ ${capture.timestamp} failed (${lastErr.message}); trying older capture...`
        );
      }
    }
  }

  throw lastErr || new Error('no candidates in group');
}

// -------------------------------------------------------- LINK REWRITING

/**
 * Best-effort rewrite of links inside a downloaded HTML file so they
 * point at local copies instead of the live domain. This is intentionally
 * simple (regex, not a real HTML parser) — good enough for browsing the
 * rebuilt site locally, not guaranteed pixel-perfect.
 */
async function rewriteHtmlLinks(localPath, domain) {
  if (path.extname(localPath) !== '.html') return;

  let html;
  try {
    html = await fsp.readFile(localPath, 'utf8');
  } catch {
    return; // not valid utf8 / not actually text — skip
  }

  const domainPattern = domain.replace(/\./g, '\\.');
  const hostRe = new RegExp(
    `https?://(?:www\\.)?${domainPattern}(:\\d+)?`,
    'gi'
  );

  const rewritten = html
    // absolute links back to the live site -> root-relative
    .replace(hostRe, '')
    // any stray Wayback rewrite artifacts, e.g. /web/20051016.../http://...
    .replace(/\/web\/\d{1,14}(?:id_|if_)?\//g, '/')
    // query-string links to CGI pages -> our slugified .html filenames.
    // Must match urlToLocalPath()'s scheme exactly: query is appended as
    // "<base>__<query>" and .html is appended only if not already present.
    .replace(/(index\.cgi)\?([^"'\s>]+)/gi, (_m, base, query) => {
      const safeQuery = query.replace(/[^a-zA-Z0-9._-]/g, '_');
      return `${base}__${safeQuery}.html`;
    });

  await fsp.writeFile(localPath, rewritten, 'utf8');
}

// -------------------------------------------------------- CONCURRENCY

/**
 * Tiny worker-pool runner: processes `items` with up to `concurrency`
 * workers, each pausing `delayMs` between tasks, calling `worker(item)`
 * for each one and collecting results/errors.
 */
async function runPool(items, worker, { concurrency, delayMs }) {
  const results = [];
  let cursor = 0;
  let done = 0;

  async function work() {
    while (cursor < items.length) {
      const index = cursor++;
      const item = items[index];
      try {
        const value = await worker(item, index);
        results[index] = { ok: true, item, value };
      } catch (err) {
        results[index] = { ok: false, item, error: err.message || String(err) };
      }
      done++;
      if (done % 25 === 0 || done === items.length) {
        console.log(`[progress] ${done}/${items.length}`);
      }
      if (delayMs) await sleep(delayMs);
    }
  }

  const workers = Array.from({ length: concurrency }, () => work());
  await Promise.all(workers);
  return results;
}

// --------------------------------------------------------------- REPORT

async function writeJson(filePath, data) {
  await fsp.mkdir(path.dirname(filePath), { recursive: true });
  await fsp.writeFile(filePath, JSON.stringify(data, null, 2));
}

async function loadJsonIfExists(filePath) {
  try {
    return JSON.parse(await fsp.readFile(filePath, 'utf8'));
  } catch {
    return null;
  }
}

// ----------------------------------------------------------------- MAIN

async function main() {
  await fsp.mkdir(CONFIG.outDir, { recursive: true });

  let groups; // array of candidate arrays, newest-first within each

  if (CONFIG.retryFailedOnly) {
    const previous = await loadJsonIfExists(REPORT_PATH);
    if (!previous || !previous.failed?.length) {
      console.log('No previous report.json with failures found — nothing to retry.');
      return;
    }
    groups = previous.failed.map((f) => f.candidates);
    console.log(`[retry] re-attempting ${groups.length} previously failed destination files`);
  } else {
    const rows = await fetchCdx(CONFIG.domain);
    console.log(`[cdx] ${rows.length} raw capture rows returned`);
    const groupMap = groupByLocalPath(rows);
    groups = [...groupMap.values()];
    console.log(`[cdx] ${groups.length} unique destination files to recover`);
  }

  const results = await runPool(
    groups,
    (candidates) => downloadGroupWithFallback(candidates),
    { concurrency: CONFIG.concurrency, delayMs: CONFIG.delayMs }
  );

  const succeeded = [];
  const failed = [];
  const fellBack = [];

  for (const r of results) {
    if (r.ok) {
      const entry = {
        original: r.value.usedOriginal,
        timestamp: r.value.usedTimestamp,
        localPath: path.relative(CONFIG.outDir, r.value.localPath),
        size: r.value.size,
      };
      succeeded.push(entry);
      if (r.value.skippedNewerCaptures.length) {
        fellBack.push({ ...entry, skippedNewerCaptures: r.value.skippedNewerCaptures });
      }
    } else {
      // Keep the whole candidate list so a retry can still walk through
      // whichever older captures weren't tried yet.
      failed.push({ candidates: r.item, error: r.error });
    }
  }

  await writeJson(MANIFEST_PATH, {
    domain: CONFIG.domain,
    generatedAt: new Date().toISOString(),
    totalFiles: succeeded.length,
    files: succeeded,
  });

  await writeJson(REPORT_PATH, {
    domain: CONFIG.domain,
    generatedAt: new Date().toISOString(),
    succeeded: succeeded.length,
    fellBackToOlderCapture: fellBack.length,
    failed,
  });

  console.log('\n----------------------------------------');
  console.log(`Done. ${succeeded.length} files recovered, ${failed.length} failed.`);
  if (fellBack.length) {
    console.log(
      `${fellBack.length} of those had to fall back to an older capture (their newest snapshot failed to fetch) — see "fellBackToOlderCapture" details in report.json.`
    );
  }
  console.log(`Site written to: ${CONFIG.outDir}`);
  console.log(`Manifest: ${MANIFEST_PATH}`);
  if (failed.length) {
    console.log(`Report (with failures): ${REPORT_PATH}`);
    console.log(`Re-run failures only with: node recover-site.js --retry-failed --out=${args.out || './rebuilt-site'}`);
  }
  console.log('----------------------------------------');
}

// --------------------------------------------------------------- HELPERS

function parseArgs(argv) {
  const out = {};
  for (const raw of argv) {
    const m = raw.match(/^--([^=]+)(?:=(.*))?$/);
    if (m) out[m[1]] = m[2] === undefined ? true : m[2];
  }
  return out;
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exitCode = 1;
});
