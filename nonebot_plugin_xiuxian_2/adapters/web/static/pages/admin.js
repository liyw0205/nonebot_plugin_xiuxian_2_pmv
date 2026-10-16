import { requestJson } from '../core/api.js';
import { createState } from '../core/state.js';
import { one, renderState, setBusy } from '../core/dom.js';

export function initAdminPage(root = document) {
  const panel = one('[data-admin-page]', root);
  const state = createState({ status: 'idle', data: null, error: '' });
  if (!panel) return state;
  const refresh = one('[data-action="refresh"]', panel);
  const output = one('[data-state-output]', panel);
  const activityForm = one('[data-activity-controls]', panel);
  const activityView = one('[data-activity-view]', panel);
  const activityUserId = one('[data-activity-user-id]', panel);
  const load = async () => {
    state.update({ status: 'loading', error: '' });
    if (activityForm && output) output.textContent = '';
    renderState(panel, state.get());
    setBusy(refresh, true);
    try {
      let endpoint = panel.dataset.endpoint;
      if (activityForm) {
        const url = new URL(endpoint, window.location.origin);
        url.searchParams.set('view', activityView?.value || 'center');
        const userId = activityUserId?.value.trim();
        if (userId) url.searchParams.set('user_id', userId);
        endpoint = url.toString();
      }
      const data = await requestJson(endpoint);
      state.update({ status: data && ((Array.isArray(data) && data.length) || Object.keys(data || {}).length) ? 'ready' : 'empty', data });
      if (output) {
        output.textContent = activityForm
          ? String(data?.text || '')
          : JSON.stringify(data ?? null, null, 2);
      }
    } catch (error) {
      state.update({ status: 'error', error: error.message || '请求失败' });
    } finally {
      setBusy(refresh, false);
      renderState(panel, state.get());
    }
  };
  if (activityForm) {
    activityForm.addEventListener('submit', (event) => {
      event.preventDefault();
      load();
    });
  } else {
    refresh?.addEventListener('click', load);
  }
  activityView?.addEventListener('change', load);
  load();
  return state;
}

if (typeof document !== 'undefined') {
  document.addEventListener('DOMContentLoaded', () => initAdminPage());
}
