import { idempotencyKey, requestJson } from '../core/api.js';
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
  const activitySign = one('[data-action="activity-sign"]', panel);
  const activityClaim = one('[data-action="activity-claim"]', panel);
  let pendingActivitySign = null;
  let pendingActivityClaim = null;
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
  const refreshActivityState = async (feedback) => {
    await load();
    if (!output || !feedback) return;
    const refreshed = output.textContent;
    output.textContent = refreshed ? `${feedback}\n\n${refreshed}` : feedback;
  };
  const setActivityControlsBusy = (busy) => {
    if (activityUserId) activityUserId.disabled = busy;
    if (activityView) activityView.disabled = busy;
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
  const updateActionAvailability = () => {
    const userSelected = Boolean(activityUserId?.value.trim());
    const mutationBusy = activitySign?.getAttribute('aria-busy') === 'true' ||
      activityClaim?.getAttribute('aria-busy') === 'true';
    if (activitySign) {
      activitySign.disabled = !userSelected || mutationBusy;
    }
    if (activityClaim) {
      activityClaim.disabled = !userSelected || mutationBusy;
    }
  };
  activityUserId?.addEventListener('input', () => {
    if (pendingActivitySign && pendingActivitySign.userId !== activityUserId.value.trim()) {
      pendingActivitySign = null;
    }
    if (pendingActivityClaim && pendingActivityClaim.userId !== activityUserId.value.trim()) {
      pendingActivityClaim = null;
    }
    updateActionAvailability();
  });
  activitySign?.addEventListener('click', async () => {
    const userId = activityUserId?.value.trim() || '';
    if (!userId) return;
    if (!pendingActivitySign || pendingActivitySign.userId !== userId) {
      pendingActivitySign = { userId, operationId: idempotencyKey() };
    }
    setBusy(activitySign, true);
    setActivityControlsBusy(true);
    updateActionAvailability();
    state.update({ status: 'submitting', error: '' });
    renderState(panel, state.get());
    try {
      const result = await requestJson('/api/v1/activity/sign-in', {
        method: 'POST',
        headers: { 'Idempotency-Key': pendingActivitySign.operationId },
        body: JSON.stringify({ user_id: userId }),
      });
      pendingActivitySign = null;
      if (output) output.textContent = String(result?.data?.message || result?.message || '签到操作已完成');
      await load();
    } catch (error) {
      const retryable = !error.status || error.status >= 500 || error.status === 403 ||
        ['conflict', 'operation_conflict'].includes(error.code);
      if (!retryable) pendingActivitySign = null;
      state.update({ status: 'error', error: error.message || '请求失败' });
      if (output) output.textContent = error.message || '请求失败';
      renderState(panel, state.get());
    } finally {
      setBusy(activitySign, false);
      setActivityControlsBusy(false);
      updateActionAvailability();
    }
  });
  activityClaim?.addEventListener('click', async () => {
    const userId = activityUserId?.value.trim() || '';
    if (!userId) return;
    if (!pendingActivityClaim || pendingActivityClaim.userId !== userId) {
      pendingActivityClaim = { userId, operationId: idempotencyKey() };
    }
    setBusy(activityClaim, true);
    setActivityControlsBusy(true);
    updateActionAvailability();
    state.update({ status: 'submitting', error: '' });
    renderState(panel, state.get());
    try {
      const result = await requestJson('/api/v1/activity/rewards/claim', {
        method: 'POST',
        headers: { 'Idempotency-Key': pendingActivityClaim.operationId },
        body: JSON.stringify({ user_id: userId }),
      });
      pendingActivityClaim = null;
      await refreshActivityState(result?.data?.text || result?.message || '活动奖励领取已完成');
    } catch (error) {
      const retryable = !error.status || error.status >= 500 || error.status === 403 ||
        error.code === 'retryable_failure' || error.code === 'conflict';
      if (!retryable) pendingActivityClaim = null;
      const message = error.message || '活动奖励领取未完成';
      if (error.status === 409) {
        await refreshActivityState(message);
      } else {
        state.update({ status: 'error', error: message });
        if (output) output.textContent = message;
        renderState(panel, state.get());
      }
    } finally {
      setBusy(activityClaim, false);
      setActivityControlsBusy(false);
      updateActionAvailability();
    }
  });
  updateActionAvailability();
  load();
  return state;
}

if (typeof document !== 'undefined') {
  document.addEventListener('DOMContentLoaded', () => initAdminPage());
}
