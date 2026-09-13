/* Shared JSON client for refactored pages. */
const csrfToken = () => document.querySelector('meta[name="csrf-token"]')?.content ||
  document.querySelector('[data-csrf-token]')?.dataset.csrfToken || '';

export async function requestJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = options.timeout ?? 15000;
  const timer = setTimeout(() => controller.abort(), timeout);
  const headers = new Headers(options.headers || {});
  headers.set('Accept', 'application/json');
  if (!headers.has('X-Request-ID')) headers.set('X-Request-ID', crypto.randomUUID());
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes((options.method || 'GET').toUpperCase())) {
    headers.set('X-CSRF-Token', csrfToken());
  }
  try {
    const response = await fetch(url, { ...options, headers, signal: controller.signal });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false || payload.success === false) {
      const error = payload.error || { code: 'http_error', message: response.statusText };
      throw Object.assign(new Error(error.message || 'Request failed'), { code: error.code, details: error.details, status: response.status });
    }
    return payload.data ?? payload;
  } finally {
    clearTimeout(timer);
  }
}

export function idempotencyKey() {
  return crypto.randomUUID();
}
