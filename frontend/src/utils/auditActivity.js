export function extractDetailField(details, key) {
  const text = String(details || '');
  if (!text || !key) {
    return '';
  }
  const pattern = new RegExp(`(?:^|\\|\\s*)${key}=([^|]+)`);
  const match = text.match(pattern);
  return String(match?.[1] || '').trim();
}

export function getAuditActivityTitle(entry) {
  const eventType = String(entry?.event_type || '').trim().toLowerCase();
  const entryType = String(entry?.entry_type || '').trim().toLowerCase();
  const reason = extractDetailField(entry?.details, 'reason').toLowerCase();
  const prettifyEvent = (value) => String(value || '').
    split('_').
    filter(Boolean).
    map((part) => part.charAt(0).toUpperCase() + part.slice(1)).
    join(' ');

  if (entryType === 'cso_event_log') {
    if (eventType === 'switch_attempt') {
      return 'CSO Failover Attempt';
    }
    if (eventType === 'switch_success') {
      if (reason === 'initial_start') {
        return 'CSO Initial Source Selected';
      }
      return 'CSO Failover Success';
    }
    if (eventType === 'session_start') {
      return 'CSO Session Started';
    }
    if (eventType === 'session_end') {
      return 'CSO Session Ended';
    }
    if (eventType === 'playback_unavailable') {
      return 'CSO Playback Unavailable';
    }
    if (eventType === 'capacity_blocked') {
      return 'CSO Capacity Blocked';
    }
    if (eventType === 'health_recovered') {
      return 'CSO Health Recovered';
    }
    if (eventType === 'health_actioned') {
      return 'CSO Health Actioned';
    }
    if (eventType === 'scheduled_health_failed') {
      return 'Scheduled Health Check Failed';
    }
    if (eventType === 'scheduled_health_recovered') {
      return 'Scheduled Health Check Recovered';
    }
    if (eventType) {
      return `CSO ${prettifyEvent(eventType)}`;
    }
  }

  if (eventType === 'stream_start') {
    return 'Playback Session Started';
  }
  if (eventType === 'stream_stop') {
    return 'Playback Session Ended';
  }
  if (eventType === 'xc_stream') {
    return 'XC Stream Requested';
  }
  if (eventType === 'hls_stream_connect') {
    return 'HLS Stream Connected';
  }
  if (eventType === 'hls_stream_disconnect') {
    return 'HLS Stream Disconnected';
  }

  return String(entry?.activity_label || '').trim() || 'Other activity';
}
