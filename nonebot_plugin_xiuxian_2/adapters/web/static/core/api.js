const token = () => document.querySelector('meta[name="csrf-token"]')?.content || '';

export async function requestJson(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeout ?? 15000);
  const headers = new Headers(options.headers || {});
  headers.set('Accept', 'application/json');
  if (!headers.has('X-Request-ID')) headers.set('X-Request-ID', crypto.randomUUID());
  const method = (options.method || 'GET').toUpperCase();
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method) && !headers.has('X-CSRF-Token')) {
    headers.set('X-CSRF-Token', token());
  }
  try {
    const response = await fetch(url, { ...options, method, headers, signal: controller.signal });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false) {
      const error = payload.error || { code: 'http_error', message: response.statusText };
      throw Object.assign(new Error(error.message || 'Request failed'), { code: error.code, details: error.details, status: response.status });
    }
    return payload.data ?? payload;
  } finally {
    clearTimeout(timer);
  }
}

export const idempotencyKey = () => crypto.randomUUID();
