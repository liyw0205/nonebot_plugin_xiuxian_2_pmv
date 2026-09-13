export function createState(initial = {}) {
  let value = { ...initial };
  const listeners = new Set();
  return {
    get: () => value,
    update(patch) {
      value = { ...value, ...(typeof patch === 'function' ? patch(value) : patch) };
      listeners.forEach(listener => listener(value));
      return value;
    },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
  };
}
