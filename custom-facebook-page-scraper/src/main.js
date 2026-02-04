// FILE: src/main.js
// Apify SDK v3.4.5 / Crawlee v3.14.1 / Node v22.x

import { Actor, log } from 'apify';
import { CheerioCrawler, RequestList } from 'crawlee';

/** ------------------------------
 *  URL canonicalization helpers
 *  ------------------------------ */
function canonicalizeFacebookUrl(u) {
  try {
    const url = new URL(u);
    if (url.hostname.endsWith('facebook.com')) url.hostname = 'www.facebook.com';

    // Strip noisy params that do not change identity
    const strip = [
      'fbclid', 'ref', 'refid', 'refsrc',
      'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'utm_id',
      '__tn__', 'locale', '_rdr', 'sk',
    ];
    strip.forEach((p) => url.searchParams.delete(p));

    // Remove trailing slash (but keep root '/')
    if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, '');

    return url.toString();
  } catch {
    return u;
  }
}

function parseLikesFromText(text) {
  if (!text) return null;
  const m = text.match(/(\d[\d,\.]*)\s+likes?/i) || text.match(/(\d[\d,\.]*)\s+followers?/i);
  if (!m) return null;
  const n = Number(m[1].replace(/[.,]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function extractFacebookId($) {
  const al =
    $('meta[property="al:android:url"]').attr('content') ||
    $('meta[property="al:ios:url"]').attr('content') ||
    null;
  if (al) {
    const m = al.match(/fb:\/\/(?:page|profile)\/(\d+)/i);
    if (m) return m[1];
  }
  const ogUrl = $('meta[property="og:url"]').attr('content') || null;
  if (ogUrl) {
    let m = ogUrl.match(/[?&]id=(\d+)/); // profile.php?id=###
    if (m) return m[1];
    m = ogUrl.match(/\/(\d{8,})\/?$/);   // numeric suffix at end
    if (m) return m[1];
  }
  return null;
}

/** ------------------------------
 *  Main
 *  ------------------------------ */
async function main() {
  await Actor.init();

  try {
    const input = (await Actor.getInput()) ?? {};
    const {
      requestQueueRef,
      maxRequestsPerCrawl,
      proxyGroups,
      proxyCountryCode = 'US',
    } = input;

    if (!requestQueueRef) throw new Error('requestQueueRef is required.');

    // Open queue by ID or by Name (resolve ID -> name when possible)
    // Open queue by ID or Name (SDK handles it)
    log.info(`Opening queue by ref '${requestQueueRef}'`);
    const requestQueue = await Actor.openRequestQueue(requestQueueRef);

    const rqInfoBefore = await requestQueue.getInfo();
    const queuedBefore = {
      total: rqInfoBefore?.totalRequestCount ?? 0,
      handled: rqInfoBefore?.handledRequestCount ?? 0,
    };
    log.info(
      `Queue opened: name='${rqInfoBefore?.name}', id='${rqInfoBefore?.id}', total=${queuedBefore.total}, handled=${queuedBefore.handled}`,
    );

    // -------- Manual seeding from persistent Request Queue --------
    const seededQueueReqs = [];
    const startRequests = [];
    const maxN = Number.isFinite(maxRequestsPerCrawl)
      ? Math.max(0, Number(maxRequestsPerCrawl))
      : Number.POSITIVE_INFINITY;

    while (seededQueueReqs.length < maxN) {
      const qreq = await requestQueue.fetchNextRequest();
      if (!qreq) break; // nothing more pending
      const canonical = canonicalizeFacebookUrl(qreq.url);

      seededQueueReqs.push(qreq);
      startRequests.push({
        url: canonical,
        userData: {
          qid: qreq.id,                 // original queue request id (so we can mark handled/failed)
          rawUrl: qreq.url,
          canonicalUrl: canonical,
          searchTerm: qreq.userData?.searchTerm ?? null,
          location: qreq.userData?.location ?? null,
        },
      });
    }

    const seeded = seededQueueReqs.length;
    log.info(`Seeded ${seeded} requests from the queue.`);

    // If nothing to do, write a small summary and exit gracefully
    if (seeded === 0) {
      const rqInfoAfter0 = await requestQueue.getInfo();
      await Actor.setValue('SCRAPER_SUMMARY', {
        queuedBefore,
        seeded: 0,
        queuedAfter: {
          total: rqInfoAfter0?.totalRequestCount ?? 0,
          handled: rqInfoAfter0?.handledRequestCount ?? 0,
        },
        deltaHandled: (rqInfoAfter0?.handledRequestCount ?? 0) - queuedBefore.handled,
        runId: Actor.getEnv().actorRunId,
        actorBuildId: Actor.getEnv().actorBuildId,
      });
      log.info('Nothing to do (no pending requests). Exiting.');
      return;
    }

    // Map queue request id -> original object for markRequestHandled/Failed
    const qreqById = new Map(seededQueueReqs.map((r) => [r.id, r]));

    // Use Apify proxies if configured
    const proxyConfiguration = await Actor.createProxyConfiguration({
      groups: proxyGroups,
      countryCode: proxyCountryCode,
    });

    // IMPORTANT: run crawler off a RequestList (do NOT pass requestQueue here)
    const requestList = await RequestList.open(`seeded-${Date.now()}`, startRequests);

    const crawler = new CheerioCrawler({
      requestList,
      maxRequestsPerCrawl,
      proxyConfiguration,
      minConcurrency: 1,
      maxConcurrency: 4,

      // light header to look more like a real browser
      preNavigationHooks: [
        async ({ request, session, proxyInfo }, gotoOptions) => {
          gotoOptions.headers = {
            ...(gotoOptions.headers || {}),
            'Accept-Language': 'en-US,en;q=0.9',
          };
        },
      ],

      requestHandler: async ({ request, $, log: crawlLog }) => {
        const now = new Date().toISOString();
        const { qid, rawUrl, canonicalUrl, searchTerm, location } = request.userData ?? {};

        // Read from <head> meta tags (works logged out)
        const ogTitle =
          $('meta[property="og:title"]').attr('content') ||
          $('meta[name="twitter:title"]').attr('content') ||
          null;

        const ogDesc =
          $('meta[property="og:description"]').attr('content') ||
          $('meta[name="description"]').attr('content') ||
          null;

        // Page name cleanup
        let pageName = ogTitle?.trim() ?? null;
        if (pageName) {
          pageName = pageName
            .replace(/\s*[|•·-]\s*Facebook.*$/i, '')
            .replace(/\s*·\s*\d[\d,\.]*\s+(?:likes?|followers?)$/i, '')
            .trim();
        }

        const likes =
          parseLikesFromText(ogTitle ?? '') ?? parseLikesFromText(ogDesc ?? '');

        const introduction = ogDesc ?? null;
        const facebookId = extractFacebookId($);

        const item = {
          pageId: facebookId ?? null,
          pageUrl: canonicalUrl ?? request.url,
          url: rawUrl ?? canonicalUrl ?? request.url,
          pageName: pageName ?? null,
          likes: likes ?? null,
          intro: introduction ?? null,
          scrapedAt: now,
          sourceRunId: Actor.getEnv().actorRunId,
          searchTerm: searchTerm ?? null,
          location: location ?? null,
        };

        await Actor.pushData(item);

        // Mark the ORIGINAL queue request handled
        if (qid && qreqById.has(qid)) {
          await requestQueue.markRequestHandled(qreqById.get(qid));
        } else {
          crawlLog.warning(`Could not mark handled (missing qid) for ${request.url}`);
        }

        const aboutLen = item.intro ? String(item.intro).length : 0;
        crawlLog.info(
          `OK: ${item.profileUrl} (name="${item.pageName ?? ''}", likes=${item.likes ?? 'null'}, aboutLen=${aboutLen})`,
        );
      },

      failedRequestHandler: async ({ request, error, log: crawlLog }) => {
        const { qid } = request.userData ?? {};
        if (qid && qreqById.has(qid)) {
          try {
            await requestQueue.markRequestFailed(qreqById.get(qid));
          } catch (e) {
            crawlLog.warning(`Failed to markRequestFailed: ${e?.message ?? e}`);
          }
        }
        crawlLog.warning(`FAILED: ${request.url} :: ${error?.message ?? error}`);
        await Actor.pushData({
          url: request.url,
          error: String(error?.message ?? error),
          scrapedAt: new Date().toISOString(),
          sourceRunId: Actor.getEnv().actorRunId,
        });
      },
    });

    log.info('CheerioCrawler: Starting the crawler.');
    await crawler.run();

    // After-run summary
    const rqInfoAfter = await requestQueue.getInfo();
    const queuedAfter = {
      total: rqInfoAfter?.totalRequestCount ?? 0,
      handled: rqInfoAfter?.handledRequestCount ?? 0,
    };
    const deltaHandled = queuedAfter.handled - queuedBefore.handled;

    await Actor.setValue('SCRAPER_SUMMARY', {
      queuedBefore,
      seeded,
      queuedAfter,
      deltaHandled,
      runId: Actor.getEnv().actorRunId,
      actorBuildId: Actor.getEnv().actorBuildId,
    });

    log.info(`Queue handled: before=${queuedBefore.handled}, after=${queuedAfter.handled}, delta=${deltaHandled}`);
    log.info('Finished scraper run.');
  } catch (err) {
    log.exception(err, 'RUN FAILED');
    throw err;
  } finally {
    await Actor.exit();
  }
}

await main();