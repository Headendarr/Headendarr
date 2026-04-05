export function normalisePreviewCandidates(payload) {
  const rawCandidates = Array.isArray(payload?.candidates) ?
    payload.candidates :
    [];
  const candidates = rawCandidates.map((candidate, index) => ({
    url: String(candidate?.url || '').trim(),
    streamType: candidate?.stream_type || candidate?.streamType ||
      payload?.stream_type || 'auto',
    sourceId: candidate?.source_id ?? candidate?.sourceId ?? null,
    priority: Number.isFinite(Number(candidate?.priority)) ?
      Number(candidate.priority) :
      index,
    sourceResolution: candidate?.source_resolution ||
      candidate?.sourceResolution ||
      payload?.source_resolution || null,
    durationSeconds: typeof candidate?.duration_seconds === 'number' ?
      candidate.duration_seconds :
      (typeof candidate?.durationSeconds === 'number' ?
        candidate.durationSeconds :
        payload?.duration_seconds ?? null),
  })).filter((candidate) => candidate.url);

  if (candidates.length) {
    return candidates;
  }

  const previewUrl = String(payload?.preview_url || payload?.url || '').trim();
  if (!previewUrl) {
    return [];
  }

  return [
    {
      url: previewUrl,
      streamType: payload?.stream_type || 'auto',
      sourceId: payload?.source_id ?? null,
      priority: Number.isFinite(Number(payload?.priority)) ?
        Number(payload.priority) :
        0,
      sourceResolution: payload?.source_resolution || null,
      durationSeconds: payload?.duration_seconds ?? null,
    },
  ];
}

export function primaryPreviewCandidate(payload) {
  return normalisePreviewCandidates(payload)[0] || null;
}
