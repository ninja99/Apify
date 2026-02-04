import { Actor } from 'apify';

function canonicalizeUrl(u) {
  try {
    const url = new URL(u);
    const strip = [
      'fbclid',
      'ref',
      'refid',
      'refsrc',
      'utm_source',
      'utm_medium',
      'utm_campaign',
      'utm_term',
      'utm_content',
      'utm_id',
      '__tn__',
      'locale'
    ];
    strip.forEach((p) => url.searchParams.delete(p));
    url.pathname = url.pathname.replace(/\/+$/, '');
    return url.toString();
  } catch {
    return u;
  }
}

await Actor.init();

try {
  const input = (await Actor.getInput()) ?? {};

  const {
    sourceDatasetId,
    targetRequestQueueRef, // name or ID (we'll pass the name: "fb-url-profiles")
    urlFieldName = 'url',
    statusDatasetName = 'bridge-status',
    pushRunSummaryToDataset = true,
    normalizeUrls = true,
    maxItems
  } = input;

  if (!sourceDatasetId || !targetRequestQueueRef) {
    throw new Error(
      'sourceDatasetId and targetRequestQueueRef are required.'
    );
  }

  const client = Actor.apifyClient;

  // Resolve queue ID -> name if ID was provided, otherwise open by name.
  async function openQueueByRef(ref) {
    try {
      const q = await client.requestQueue(ref).get();
      if (q?.name) return Actor.openRequestQueue(q.name);
    } catch (err) {
      if (err?.statusCode && err.statusCode !== 404) throw err;
    }
    return Actor.openRequestQueue(ref);
  }

  console.log(`Opening target request queue '${targetRequestQueueRef}'...`);
  const requestQueue = await openQueueByRef(targetRequestQueueRef);
  const rqInfo = await requestQueue.getInfo();

  console.log(`Opening status dataset '${statusDatasetName}'...`);
  const statusDs = await Actor.openDataset(statusDatasetName);

  console.log(`Reading source dataset '${sourceDatasetId}' via pagination...`);

  let processed = 0;
  let newCount = 0;
  let dupCount = 0;
  let handledDup = 0;
  let missingUrl = 0;

  const startedAt = new Date().toISOString();
  const runId = Actor.getEnv().actorRunId;

  const dsClient = client.dataset(sourceDatasetId);
  const pageSize = 500; // safe page size
  let offset = 0;
  let index = 0;
  let stop = false;

  while (!stop) {
    const res = await dsClient.listItems({
      clean: true,
      limit: pageSize,
      offset
    });

    const items = res?.items ?? [];
    if (!items.length) break;

    for (const item of items) {
      if (maxItems && processed >= maxItems) {
        stop = true;
        break;
      }

      const raw = item?.[urlFieldName];
      const normalized = normalizeUrls && raw ? canonicalizeUrl(raw) : raw;

      if (!normalized) {
        missingUrl += 1;
        await statusDs.pushData({
          type: 'per_url_status',
          status: 'missing_url',
          index,
          urlFieldName,
          sourceDatasetId,
          targetQueueName: rqInfo?.name ?? targetRequestQueueRef,
          runId,
          createdAt: new Date().toISOString()
        });
        index += 1;
        continue;
      }

      const info = await requestQueue.addRequest({
        url: normalized,
        uniqueKey: normalized
      });

      processed += 1;

      const status = info.wasAlreadyHandled
        ? 'alreadyHandled'
        : info.wasAlreadyPresent
        ? 'duplicate'
        : 'new';

      if (status === 'alreadyHandled') handledDup += 1;
      else if (status === 'duplicate') dupCount += 1;
      else newCount += 1;

      await statusDs.pushData({
        type: 'per_url_status',
        status,
        url: normalized,
        uniqueKey: normalized,
        requestId: info.requestId,
        wasAlreadyPresent: info.wasAlreadyPresent ?? false,
        wasAlreadyHandled: info.wasAlreadyHandled ?? false,
        index,
        sourceDatasetId,
        targetQueueName: rqInfo?.name ?? targetRequestQueueRef,
        runId,
        createdAt: new Date().toISOString()
      });

      if (processed % 100 === 0) {
        console.log(`Progress: ${processed} items processed...`);
        await Actor.setValue('PROGRESS', {
          processed,
          newCount,
          dupCount,
          handledDup,
          missingUrl,
          at: new Date().toISOString()
        });
      }

      index += 1;
    }

    offset += items.length;
  }

  const finishedAt = new Date().toISOString();
  const summary = {
    type: 'run_summary',
    sourceDatasetId,
    targetQueueName: rqInfo?.name ?? targetRequestQueueRef,
    processed,
    newCount,
    dupCount,
    alreadyHandled: handledDup,
    missingUrl,
    duplicateRatio:
      processed > 0
        ? Number(((dupCount + handledDup) / processed).toFixed(3))
        : 0,
    startedAt,
    finishedAt,
    runId,
    actorBuildId: Actor.getEnv().actorBuildId
  };

  console.log(
    `Done. processed=${processed}, new=${newCount}, duplicates=${dupCount}, alreadyHandled=${handledDup}, missingUrl=${missingUrl}`
  );

  await Actor.setValue('RUN_SUMMARY', summary);
  if (pushRunSummaryToDataset) await statusDs.pushData(summary);
} catch (err) {
  console.error('RUN FAILED:', err);
  throw err;
} finally {
  await Actor.exit();
}