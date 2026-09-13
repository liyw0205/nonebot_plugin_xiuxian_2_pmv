import { requestJson } from '../core/api.js';
import { createState } from '../core/state.js';
import { one, renderState, setBusy } from '../core/dom.js';

export function initMessagesPage(root = document) {
  const panel = one('[data-messages-page]', root);
  const state = createState({ status: 'idle', rows: [], error: '' });
  if (!panel) return state;
  const refresh = one('[data-action="refresh"]', panel);
  const body = one('[data-message-rows]', panel);
  const load = async () => {
    state.update({ status: 'loading', error: '' });
    renderState(panel, state.get());
    setBusy(refresh, true);
    try {
      const rows = await requestJson(panel.dataset.endpoint || '/api/v1/messages');
      state.update({ status: rows.length ? 'ready' : 'empty', rows });
      body?.replaceChildren(...rows.map(row => {
        const item = document.createElement('li');
        item.textContent = row.content || row.message || '';
        return item;
      }));
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
  document.addEventListener('DOMContentLoaded', () => initMessagesPage());
}
