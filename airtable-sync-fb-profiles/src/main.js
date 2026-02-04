import { Actor } from 'apify';

const AIRTABLE_API_BASE = 'https://api.airtable.com/v0';

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function escFormula(v) {
  return String(v ?? '').replace(/'/g, "''");
}

// Throttled + 429-resilient fetch for Airtable
// Airtable limits: 5 req/s per base; 429 → wait ~30s before retry.
async function airtableFetchJson(url, opts, throttleMs = 220) {
  await sleep(throttleMs);
  const res = await fetch(url, opts);
  if (res.status === 429) {
    await sleep(30000);
    const retry = await fetch(url, opts);
    if (!retry.ok) {
      const t = await retry.text();
      throw new Error(`Airtable 429 retry failed: ${retry.status} ${t}`);
    }
    return retry.json();
  }
  if (!res.ok) {
    const t = await res.text();
    throw new Error(
      `Airtable error: ${res.status} ${res.statusText} - ${t || 'no body'}`
    );
  }
  return res.json();
}

function headers(token) {
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json'
  };
}

// Prioritize first key that exists/non-empty
function first(item, keys) {
  for (const k of Array.isArray(keys) ? keys : [keys]) {
    const v = item?.[k];
    if (v !== undefined && v !== null && v !== '') return v;
  }
  return undefined;
}

// Airtable Rating (0..5 integer)
function coerceRating(v) {
  const n = Number(v);
  if (Number.isNaN(n)) return undefined;
  return Math.max(0, Math.min(5, Math.round(n)));
}

// Build OR({Field}='a', {Field}='b', ...)
function buildOrFormula(fieldName, values) {
  const parts = values.map((v) => `({${fieldName}}='${escFormula(v)}')`);
  return parts.length === 1 ? parts[0] : `OR(${parts.join(',')})`;
}

// Merge / dedupe an array of record-link IDs
function mergeLinkIds(prev = [], add = []) {
  const set = new Set(
    [
      ...prev.map((x) => (typeof x === 'string' ? x : x?.id)),
      ...add.map((x) => (typeof x === 'string' ? x : x?.id))
    ].filter(Boolean)
  );
  return Array.from(set).map((id) => ({ id }));
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------
await Actor.init();

try {
  const input = (await Actor.getInput()) ?? {};
  const { sourceDatasetId, airtable, runInfo, datasetFields = {}, maxItems } =
    input;

  if (!sourceDatasetId) throw new Error('sourceDatasetId is required.');
  if (!airtable?.baseId) throw new Error('airtable.baseId is required.');
  if (!runInfo?.runId) throw new Error('runInfo.runId is required.');

  const token =
    process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY || '';
  if (!token) {
    throw new Error(
      'Missing Airtable token. Set AIRTABLE_PAT (or AIRTABLE_API_KEY).'
    );
  }

  // Airtable config (defaults applied at runtime)
  const runTable = airtable.runTable ?? 'Run Log';
  const profileTable = airtable.profileTable ?? 'Master Profile List';
  const keyField = airtable.profileKeyFieldName ?? 'Facebook ID';
  const linkField = airtable.linkFieldName ?? 'Run Log';
  const statusField = airtable.statusFieldName ?? 'Status';
  const statusComplete = airtable.statusComplete ?? 'Scrape Complete';

  // Dataset field keys (runtime defaults; you can override via datasetFields)
  const df = {
    facebookId: 'facebookId',
    profileUrl: ['profileUrl', 'url', 'fbProfileUrl', 'profile_url'],
    name: ['name', 'businessName', 'pageName'],
    email: 'email',
    phone: 'phone',
    rating: 'rating',
    reviewCount: 'reviewCount',
    profileImageUrl: ['profileImageUrl', 'imageUrl', 'avatarUrl'],
    coverPhotoUrl: 'coverPhotoUrl',
    address: 'address',
    serviceArea: 'serviceArea',
    location: 'location',
    likes: 'likes',
    isVerified: ['isVerified', 'verified'],
    introduction: 'introduction',
    businessHours: 'businessHours',
    linkedinUrl: 'linkedinUrl',
    instagramUrl: 'instagramUrl',
    instagramHandle: 'instagramHandle',
    tiktokUrl: 'tiktokUrl',
    xUrl: 'xUrl',
    whatsapp: ['whatsapp', 'whatsappPhone'],
    mapUrl: 'mapUrl',
    priceRange: 'priceRange',
    profileType: 'profileType',
    trade: ['trade', 'category'],
    primaryWebsite: ['primaryWebsite', 'website'],
    additionalWebsites: ['additionalWebsites', 'otherWebsites'],
    notes: 'notes',
    ...datasetFields
  };

  const client = Actor.apifyClient;

  // Ensure the Run Log row exists
  const findRunById = async (runId) => {
    const formula = `({Run ID}='${escFormula(runId)}')`;
    const url =
      `${AIRTABLE_API_BASE}/${airtable.baseId}/${encodeURIComponent(
        runTable
      )}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;
    const data = await airtableFetchJson(url, { headers: headers(token) });
    return data.records?.[0] ?? null;
  };

  const runRecord = await findRunById(runInfo.runId);
  if (!runRecord) {
    throw new Error(
      `Run Log record not found for Run ID='${runInfo.runId}'. ` +
        `Ensure your Run Log integration created the row before sync.`
    );
  }

  // Iterate dataset; batch writes with true upsert (performUpsert)
  let processed = 0;
  let created = 0;
  let updated = 0;
  let skipped = 0;

  const pageLimit = 500;
  let offset = 0;

  while (true) {
    const res = await client.dataset(sourceDatasetId).listItems({
      clean: true,
      limit: pageLimit,
      offset
    });
    const items = res?.items ?? [];
    if (!items.length) break;

    // Map into normalized rows
    const rows = [];
    for (const it of items) {
      if (maxItems && processed >= maxItems) break;

      const fid = first(it, df.facebookId);
      if (!fid) {
        skipped += 1;
        processed += 1;
        continue;
      }

      const row = {
        facebookId: String(fid),
        fbProfileUrl: first(it, df.profileUrl),
        businessName: first(it, df.name),
        email: first(it, df.email),
        phone: first(it, df.phone),
        rating: coerceRating(first(it, df.rating)),
        reviewCount: first(it, df.reviewCount),
        profileImageUrl: first(it, df.profileImageUrl),
        coverPhotoUrl: first(it, df.coverPhotoUrl),
        address: first(it, df.address),
        serviceArea: first(it, df.serviceArea),
        location: first(it, df.location),
        likes: first(it, df.likes),
        isVerified: first(it, df.isVerified),
        introduction: first(it, df.introduction),
        businessHours: first(it, df.businessHours),
        linkedinUrl: first(it, df.linkedinUrl),
        instagramUrl: first(it, df.instagramUrl),
        instagramHandle: first(it, df.instagramHandle),
        tiktokUrl: first(it, df.tiktokUrl),
        xUrl: first(it, df.xUrl),
        whatsapp: first(it, df.whatsapp),
        mapUrl: first(it, df.mapUrl),
        priceRange: first(it, df.priceRange),
        profileType: first(it, df.profileType),
        trade: first(it, df.trade),
        primaryWebsite: first(it, df.primaryWebsite),
        additionalWebsites: Array.isArray(first(it, df.additionalWebsites))
          ? first(it, df.additionalWebsites).join('\n')
          : first(it, df.additionalWebsites),
        notes: first(it, df.notes)
      };

      rows.push(row);
      processed += 1;
    }

    // Write in chunks of 10; de‑dupe within the slice; upsert by Facebook ID
    for (let i = 0; i < rows.length; i += 10) {
      // 1) De‑dupe this slice by facebookId
      const sliceRaw = rows.slice(i, i + 10);
      const seen = new Set();
      const slice = [];
      for (const r of sliceRaw) {
        if (seen.has(r.facebookId)) continue;
        seen.add(r.facebookId);
        slice.push(r);
      }
      if (!slice.length) continue;

      const tableUrl =
        `${AIRTABLE_API_BASE}/${airtable.baseId}/` +
        `${encodeURIComponent(profileTable)}`;

      // 2) Pre‑check which records exist (for accurate created/updated counters)
      const keys = slice.map((r) => r.facebookId);
      const formula = buildOrFormula(keyField, keys);
      const findUrl =
        `${AIRTABLE_API_BASE}/${airtable.baseId}/${encodeURIComponent(
          profileTable
        )}?filterByFormula=${encodeURIComponent(formula)}`;

      const foundBefore = await airtableFetchJson(findUrl, {
        headers: headers(token)
      });
      const existedMap = new Map();
      for (const rec of foundBefore.records ?? []) {
        const val = rec.fields?.[keyField];
        if (val) existedMap.set(String(val), rec);
      }

      // 3) Upsert by Facebook ID (performUpsert). Also set Status in the same call.
      const upsertRecords = slice.map((r) => ({
        fields: {
          [keyField]: r.facebookId,
          'FB Profile URL': r.fbProfileUrl ?? undefined,
          'Business Name': r.businessName ?? undefined,
          Email: r.email ?? undefined,
          Phone: r.phone ?? undefined,
          'Public Rating': r.rating ?? undefined,
          'Review Count': r.reviewCount ?? undefined,
          'Profile image URL': r.profileImageUrl ?? undefined,
          Address: r.address ?? undefined,
          'Service Area': r.serviceArea ?? undefined,
          Location: r.location ?? undefined,
          Likes: r.likes ?? undefined,
          'Is Verified': r.isVerified ?? undefined,
          Introduction: r.introduction ?? undefined,
          'Business Hours': r.businessHours ?? undefined,
          'LinkedIn URL': r.linkedinUrl ?? undefined,
          'Instagram URL': r.instagramUrl ?? undefined,
          'Instagram Handle': r.instagramHandle ?? undefined,
          'TikTok URL': r.tiktokUrl ?? undefined,
          'X URL': r.xUrl ?? undefined,
          WhatsApp: r.whatsapp ?? undefined,
          'Map URL': r.mapUrl ?? undefined,
          'Price Range': r.priceRange ?? undefined,
          'Profile Type': r.profileType ?? undefined,
          Trade: r.trade ?? undefined,
          'Primary Website': r.primaryWebsite ?? undefined,
          'Additional Websites': r.additionalWebsites ?? undefined,
          Notes: r.notes ?? undefined,
          [statusField]: statusComplete
          // Run Log link is appended in step 5 (to preserve prior links)
        }
      }));

      const upsertBody = {
        records: upsertRecords,
        typecast: true,
        performUpsert: {
          fieldsToMergeOn: [keyField]
        }
      };

      // Perform upsert (PATCH)
      await airtableFetchJson(tableUrl, {
        method: 'PATCH',
        headers: headers(token),
        body: JSON.stringify(upsertBody)
      });

      // 4) Update counters accurately from our pre‑check
      const existedCount = existedMap.size;
      const createdCount = slice.length - existedCount;
      created += createdCount;
      updated += existedCount;

      // 5) Fetch the just‑upserted records and append the Run Log link (union)
      const foundAfter = await airtableFetchJson(findUrl, {
        headers: headers(token)
      });
      const byKey = new Map();
      for (const rec of foundAfter.records ?? []) {
        const val = rec.fields?.[keyField];
        if (val) byKey.set(String(val), rec);
      }

      const linkPatches = [];
      for (const r of slice) {
        const existing = byKey.get(r.facebookId);
        if (!existing) continue;

        const prevLinks = Array.isArray(existing.fields?.[linkField])
          ? existing.fields[linkField]
          : [];
        const newLinks = mergeLinkIds(prevLinks, [runRecord.id]);
        linkPatches.push({
          id: existing.id,
          fields: { [linkField]: newLinks }
        });
      }

      if (linkPatches.length) {
        const patchBody = { records: linkPatches, typecast: true };
        await airtableFetchJson(tableUrl, {
          method: 'PATCH',
          headers: headers(token),
          body: JSON.stringify(patchBody)
        });
      }
    }

    if (maxItems && processed >= maxItems) break;
    offset += items.length;
  }

  const summary = {
    runId: runInfo.runId,
    processed,
    created,
    updated,
    skipped,
    profileTable,
    runTable,
    finishedAt: new Date().toISOString()
  };

  console.log(
    `Airtable Sync FB Profiles summary: processed=${processed}, ` +
      `created=${created}, updated=${updated}, skipped=${skipped}`
  );

  await Actor.setValue('SYNC_SUMMARY', summary);
} catch (err) {
  console.error('RUN FAILED:', err);
  throw err;
} finally {
  await Actor.exit();
}