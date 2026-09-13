export const one = (selector, root = document) => root.querySelector(selector);

export function setBusy(button, busy) {
  if (!button) return;
  button.disabled = Boolean(busy);
  button.setAttribute('aria-busy', String(Boolean(busy)));
}

export function renderState(root, state) {
  if (!root) return;
  root.dataset.state = state.status;
  const error = one('[data-state-error]', root);
  if (error) error.textContent = state.error || '';
}
