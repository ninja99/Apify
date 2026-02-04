// src/main.js
import { Actor, log } from 'apify';

function isNullishUrl(u) {
  if (u == null) return true;
  const s = String(u).trim().toLowerCase();
  return s === '' || s === 'null' || s === 'undefined';
}

await Actor.init();

try {
  const input = (await Actor.getInput()) ?? {};
  const {
    queueRef,
    pageSize = 500,
    maxScan,
    dryRun = true,
  } = input;

  if (!queueRef) throw new Error('queueRef is required.');

  const client = Actor.apifyClient;

  // Robust resolver: try as ID via API; on 404, open by NAME via SDK and read ID from getInfo()
  async function resolveQueue(ref) {
    // Try as ID
    try {
      const q = await client.requestQueue(ref).get();
      if (q?.id) {
        const rqClient = client.requestQueue(q.id);
        const rqSdk = await Actor.openRequestQueue(q.name ?? q.id);
        return { queueId: q.id, queueName: q.name ?? q.id, rqClient, rqSdk };
      }
    } catch (e) {
      if (!(e?.statusCode === 404)) throw e;
    }
    // Fallback: treat ref as NAME
    const rqSdk = await Actor.openRequestQueue(ref);
    const info = await rqSdk.getInfo();
    const qId = info?.id;
    if (!qId) throw new Error(`Could not resolve queue ID for '${ref}'`);
    const rqClient = client.requestQueue(qId);
    return { queueId: qId, queueName: info?.name ?? ref, rqClient, rqSdk };
  }

  const { queueId, queueName, rqClient, rqSdk } = await resolveQueue(queueRef);
  const qInfo = await rqSdk.getInfo();
  log.info(`Queue resolved: name='${queueName}', id='${queueId}', total=${qInfo?.totalRequestCount}`);

  let scanned = 0;
  let invalidFound = 0;
  let deleted = 0;

  // Full pagination through all requests
  async function listRequestsPage(exclusiveStartId) {
    const params = { limit: pageSize };
    if (exclusiveStartId) params.exclusiveStartId = exclusiveStartId;
    return rqClient.listRequests(params);
  }

  let lastId;
  let usingHeadFallback = false;

  try {
    let page = await listRequestsPage(undefined);
    let items = page?.items ?? [];

    while (items.length) {
      for (const r of items) {
        if (maxScan && scanned >= maxScan) break;
        scanned += 1;

        const u = (r?.url ?? '').trim();
        const bad = isNullishUrl(u);

        if (bad) {
          invalidFound += 1;
          if (dryRun) {
            log.warning(`WOULD delete: id=${r.id} url=${JSON.stringify(u)}`);
          } else {
            try {
              await rqClient.deleteRequest(r.id);
              deleted += 1;
              log.info(`Deleted bad request id=${r.id}`);
            } catch (e) {
              log.warning(`Delete failed for ${r.id}: ${e?.message ?? e}`);
            }
          }
        }
        lastId = r.id;
        if (maxScan && scanned >= maxScan) break;
      }

      if (maxScan && scanned >= maxScan) break;

      page = await listRequestsPage(lastId);
      items = page?.items ?? [];
    }
  } catch (e) {
    if (`${e?.message}`.includes('Invalid URL')) {
      usingHeadFallback = true;
      log.warning('listRequests() failed with "Invalid URL". Falling back to head batches.');
      let cycle = 0;
      while (true) {
        cycle += 1;
        const head = await rqClient.listHead({ limit: Math.min(pageSize, 1000) });
        const items = head?.items ?? [];
        if (!items.length) break;

        let cycleInvalid = 0;
        for (const r of items) {
          if (maxScan && scanned >= maxScan) break;
          scanned += 1;

          const u = (r?.url ?? '').trim();
          const bad = isNullishUrl(u);
          if (bad) {
            cycleInvalid += 1;
            invalidFound += 1;
            if (dryRun) {
              log.warning(`[cycle ${cycle}] WOULD delete: id=${r.id} url=${JSON.stringify(u)}`);
            } else {
              try {
                await rqClient.deleteRequest(r.id);
                deleted += 1;
                log.info(`[cycle ${cycle}] Deleted bad request id=${r.id}`);
              } catch (err) {
                log.warning(`[cycle ${cycle}] Delete failed for ${r.id}: ${err?.message ?? err}`);
              }
            }
          }
          if (maxScan && scanned >= maxScan) break;
        }

        if (maxScan && scanned >= maxScan) break;
        if (cycleInvalid === 0) break; // no more invalids at head
      }
    } else {
      throw e;
    }
  }

  log.info(`Done. scanned=${scanned}, invalid=${invalidFound}, deleted=${deleted}, dryRun=${dryRun}, headFallback=${usingHeadFallback}`);

  await Actor.setValue('RUN_SUMMARY', {
    queueId,
    queueName,
    scanned,
    invalidFound,
    deleted,
    dryRun,
    headFallback: usingHeadFallback,
    at: new Date().toISOString(),
  });
} catch (err) {
  log.exception(err, 'RUN FAILED');
  throw err;
} finally {
  await Actor.exit();
}