export const one = (selector, root = document) => root.querySelector(selector);
export const all = (selector, root = document) => [...root.querySelectorAll(selector)];

export function setBusy(button, busy) {
  if (!button) return;
  button.disabled = Boolean(busy);
  button.setAttribute('aria-busy', String(Boolean(busy)));
}

export function renderState(root, state) {
  if (!root) return;
  root.toggleAttribute('data-loading', state.status === 'loading');
  root.toggleAttribute('data-empty', state.status === 'empty');
  root.toggleAttribute('data-error', state.status === 'error');
  const error = one('[data-state-error]', root);
  if (error) error.textContent = state.error || '';
}
