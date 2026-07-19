const DEFAULT_BACKEND_ERROR_FIELDS = ['error', 'detail'];

export function extractApiError(error, fallbackMessage) {
  const payload = error?.response?.data;
  const errorCode = String(payload?.error_code || '').trim();
  const backendMessage = String(payload?.message || '').trim();
  if (errorCode || backendMessage) {
    return {
      errorCode,
      message: backendMessage || fallbackMessage,
      retryable: Boolean(payload?.retryable),
    };
  }
  for (const field of DEFAULT_BACKEND_ERROR_FIELDS) {
    const value = String(payload?.[field] || '').trim();
    if (value) {
      return {errorCode: '', message: value, retryable: false};
    }
  }
  return {
    errorCode: '',
    message: fallbackMessage || String(error?.message || '').trim() || 'Request failed',
    retryable: false,
  };
}

export function apiErrorNotification(error, fallbackMessage) {
  const extracted = extractApiError(error, fallbackMessage);
  return {
    color: extracted.errorCode === 'upstream_capacity_reached' ? 'warning' : 'negative',
    message: extracted.message,
  };
}
