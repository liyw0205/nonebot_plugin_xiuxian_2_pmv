import { requestJson } from '../core/api.js';
import { createState } from '../core/state.js';
import { one, renderState, setBusy } from '../core/dom.js';

export function initAdminPage(root = document) {
  const panel = one('[data-admin-page]', root);
  const state = createState({ status: 'idle', data: null, error: '' });
  if (!panel) return state;
  const refresh = one('[data-action="refresh"]', panel);
  const output = one('[data-state-output]', panel);
  const load = async () => {
    state.update({ status: 'loading', error: '' });
    renderState(panel, state.get());
    setBusy(refresh, true);
    try {
      const data = await requestJson(panel.dataset.endpoint);
      state.update({ status: data && ((Array.isArray(data) && data.length) || Object.keys(data || {}).length) ? 'ready' : 'empty', data });
      if (output) output.textContent = JSON.stringify(data ?? null, null, 2);
    } catch (error) {
      state.update({ status: 'error', error: error.message || '请求失败' });
    } finally {
      setBusy(refresh, false);
      renderState(panel, state.get());
    }
  };
  refresh?.addEventListener('click', load);
  load();
  return state;
}

if (typeof document !== 'undefined') {
  document.addEventListener('DOMContentLoaded', () => initAdminPage());
}
