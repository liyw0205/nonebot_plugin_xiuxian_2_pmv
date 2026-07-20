/**
 * Shared frontend UI components for Xiuxian Web console.
 * - No new/changed backend APIs; only progressive enhancement of existing DOM.
 * - Hooks: DOMContentLoaded + custom event `xiuxian:page-load` (SPA navigations).
 */
(function () {
  "use strict";

  const BP = { sm: 640, md: 768, lg: 900, xl: 1280 };

  function mq(max) {
    return window.matchMedia(`(max-width: ${max}px)`).matches;
  }

  function isMobile() {
    return mq(BP.md);
  }

  function isDrawerMode() {
    return mq(BP.lg);
  }

  /* ---------- Sidebar helpers (works with base.html classes) ---------- */
  function getShellEls() {
    return {
      nav: document.getElementById("mainNav"),
      backdrop: document.getElementById("sidebarBackdrop"),
      toggle: document.getElementById("menuToggle"),
    };
  }

  function setSidebarOpen(open) {
    const { nav, backdrop, toggle } = getShellEls();
    if (!nav) return;
    nav.classList.toggle("active", !!open);
    document.body.classList.toggle("sidebar-open", !!open);
    if (backdrop) backdrop.hidden = !open;
    if (toggle) toggle.setAttribute("aria-expanded", open ? "true" : "false");
  }

  function closeSidebar() {
    setSidebarOpen(false);
  }

  function openSidebar() {
    setSidebarOpen(true);
  }

  /* ---------- Responsive tables: table → cards on mobile ---------- */
  function textOf(el) {
    return (el && (el.textContent || "").trim()) || "";
  }

  function enhanceTable(table) {
    if (!table || table.dataset.uiEnhanced === "1") return;
    const scroll = table.closest(".table-scroll") || table.parentElement;
    if (!scroll) return;

    const headCells = Array.from(table.querySelectorAll("thead th")).map(textOf);
    if (!headCells.length) return;

    // Remove previous cards if re-run
    scroll.querySelectorAll(".ui-table-cards").forEach((n) => n.remove());

    const cards = document.createElement("div");
    cards.className = "ui-table-cards";
    cards.setAttribute("aria-label", "移动端卡片视图");

    const rows = Array.from(table.querySelectorAll("tbody tr"));
    let built = 0;
    rows.forEach((tr) => {
      const cells = Array.from(tr.children);
      if (!cells.length) return;
      // empty-state row with colspan
      if (cells.length === 1 && cells[0].hasAttribute("colspan")) {
        const empty = document.createElement("div");
        empty.className = "ui-table-card";
        empty.innerHTML = `<div class="ui-table-card-title">${escapeHtml(textOf(cells[0]))}</div>`;
        cards.appendChild(empty);
        built += 1;
        return;
      }

      const card = document.createElement("div");
      card.className = "ui-table-card";

      // Prefer first cell with a link/button as actions
      let actionCell = cells.find((td) => td.querySelector("a,button,.btn"));
      const titleIdx = actionCell === cells[0] ? 1 : 0;
      const titleCell = cells[titleIdx] || cells[0];

      const head = document.createElement("div");
      head.className = "ui-table-card-head";
      const title = document.createElement("div");
      title.className = "ui-table-card-title";
      title.textContent = textOf(titleCell) || headCells[titleIdx] || "记录";
      head.appendChild(title);

      if (actionCell) {
        const actions = document.createElement("div");
        actions.className = "ui-table-card-actions";
        Array.from(actionCell.querySelectorAll("a,button,.btn")).forEach((node) => {
          actions.appendChild(node.cloneNode(true));
        });
        if (actions.childNodes.length) head.appendChild(actions);
      }
      card.appendChild(head);

      const body = document.createElement("div");
      body.className = "ui-table-card-rows";
      cells.forEach((td, i) => {
        if (td === actionCell) return;
        if (i === titleIdx) return;
        const key = headCells[i] || `列${i + 1}`;
        const val = textOf(td);
        if (!val || val === "NULL") {
          // still show short NULLs sparingly
        }
        const row = document.createElement("div");
        row.className = "ui-table-card-row";
        row.innerHTML =
          `<span class="ui-table-card-key">${escapeHtml(key)}</span>` +
          `<span class="ui-table-card-val">${escapeHtml(val || "—")}</span>`;
        body.appendChild(row);
      });
      card.appendChild(body);
      cards.appendChild(card);
      built += 1;
    });

    if (!built) return;
    scroll.appendChild(cards);
    scroll.classList.add("ui-has-cards");
    table.dataset.uiEnhanced = "1";
  }

  function enhanceAllTables(root) {
    const scope = root || document;
    scope.querySelectorAll(".table-scroll table, table.js-responsive-table").forEach(enhanceTable);
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /* ---------- Collapsible filters ---------- */
  function enhanceFilters(root) {
    const scope = root || document;
    scope.querySelectorAll(".ui-filter-bar[data-collapsible='true']").forEach((bar) => {
      if (bar.dataset.uiFilterReady === "1") return;
      let fields = bar.querySelector(".ui-filter-fields");
      if (!fields) {
        // wrap existing children except toggle
        fields = document.createElement("div");
        fields.className = "ui-filter-fields";
        Array.from(bar.children).forEach((ch) => {
          if (!ch.classList.contains("ui-filter-toggle")) fields.appendChild(ch);
        });
        bar.appendChild(fields);
      }
      let toggle = bar.querySelector(".ui-filter-toggle");
      if (!toggle) {
        toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "btn btn-secondary ui-filter-toggle";
        toggle.innerHTML = '<i class="fas fa-filter"></i> <span>筛选</span>';
        bar.insertBefore(toggle, bar.firstChild);
      }
      const label = toggle.querySelector("span") || toggle;
      const sync = () => {
        const open = bar.classList.contains("is-open");
        if (label.tagName === "SPAN") label.textContent = open ? "收起筛选" : "筛选";
        toggle.setAttribute("aria-expanded", open ? "true" : "false");
      };
      toggle.addEventListener("click", () => {
        bar.classList.toggle("is-open");
        sync();
      });
      // auto-open if any field has non-empty value
      const hasValue = Array.from(fields.querySelectorAll("input,select,textarea")).some((el) => {
        if (el.type === "checkbox" || el.type === "radio") return el.checked;
        return String(el.value || "").trim() !== "";
      });
      if (hasValue) bar.classList.add("is-open");
      sync();
      bar.dataset.uiFilterReady = "1";
    });

    // Auto-mark search-form as collapsible filter on mobile density pages
    scope.querySelectorAll("form.search-form").forEach((form) => {
      form.classList.add("ui-filter-bar");
      // don't force collapsible wrapper if already many controls — layout CSS handles stack
    });
  }

  /* ---------- Messages: list/chat pane class for mobile ---------- */
  function enhanceMessages() {
    if (!document.body.classList.contains("messages-layout")) {
      document.body.classList.remove("ui-msg-chat-open");
      return;
    }
    const shell = document.getElementById("qqShell") || document.querySelector(".qq-shell");
    if (!shell) return;

    const syncChatOpen = () => {
      // Prefer existing messages.html class: mobile-chat-open
      let open = false;
      if (isMobile()) {
        if (
          shell.classList.contains("mobile-chat-open") ||
          shell.classList.contains("chat-open") ||
          shell.classList.contains("show-chat") ||
          document.body.classList.contains("chat-open")
        ) {
          open = true;
        }
      }
      document.body.classList.toggle("ui-msg-chat-open", open);
    };

    if (shell.dataset.uiMsgReady === "1") {
      syncChatOpen();
      return;
    }
    shell.dataset.uiMsgReady = "1";

    // Observe session list clicks to mark chat open on mobile
    const sessionList = document.getElementById("sessionList");
    if (sessionList) {
      sessionList.addEventListener(
        "click",
        (e) => {
          if (!isMobile()) return;
          const item = e.target.closest(
            "[data-target-id], .qq-session-item, .session-item, .qq-session-card"
          );
          if (!item) return;
          // messages.html may set mobile-chat-open itself; also set our flag
          shell.dataset.chatOpen = "1";
          shell.classList.add("chat-open");
          document.body.classList.add("ui-msg-chat-open");
        },
        true
      );
    }

    // Back affordance: look for existing back buttons
    document.addEventListener(
      "click",
      (e) => {
        const back = e.target.closest(
          "[data-ui-msg-back], .qq-chat-back, #chatBackBtn, .mobile-back, button.mobile-back"
        );
        if (!back) return;
        shell.dataset.chatOpen = "0";
        shell.classList.remove("chat-open", "show-chat");
        document.body.classList.remove("ui-msg-chat-open");
      },
      true
    );

    const mo = new MutationObserver(() => syncChatOpen());
    mo.observe(shell, {
      attributes: true,
      attributeFilter: ["class", "data-chat-open"],
    });
    syncChatOpen();
  }

  /* ---------- Action sheet API ---------- */
  let sheetEl = null;
  function ensureSheet() {
    if (sheetEl && document.body.contains(sheetEl)) return sheetEl;
    sheetEl = document.createElement("div");
    sheetEl.className = "ui-action-sheet";
    sheetEl.innerHTML =
      '<div class="ui-action-sheet-panel" role="dialog" aria-modal="true">' +
      '<div class="ui-action-sheet-title"></div>' +
      '<div class="ui-action-sheet-actions"></div>' +
      "</div>";
    sheetEl.addEventListener("click", (e) => {
      if (e.target === sheetEl) hideActionSheet();
    });
    document.body.appendChild(sheetEl);
    return sheetEl;
  }

  function showActionSheet({ title = "操作", actions = [] } = {}) {
    const el = ensureSheet();
    el.querySelector(".ui-action-sheet-title").textContent = title;
    const box = el.querySelector(".ui-action-sheet-actions");
    box.innerHTML = "";
    actions.forEach((act) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ui-action-sheet-btn" + (act.danger ? " is-danger" : "") + (act.cancel ? " is-cancel" : "");
      btn.textContent = act.label || "操作";
      btn.addEventListener("click", async () => {
        hideActionSheet();
        if (typeof act.onClick === "function") await act.onClick();
      });
      box.appendChild(btn);
    });
    // default cancel
    if (!actions.some((a) => a.cancel)) {
      const cancel = document.createElement("button");
      cancel.type = "button";
      cancel.className = "ui-action-sheet-btn is-cancel";
      cancel.textContent = "取消";
      cancel.addEventListener("click", hideActionSheet);
      box.appendChild(cancel);
    }
    el.classList.add("is-open");
  }

  function hideActionSheet() {
    if (sheetEl) sheetEl.classList.remove("is-open");
  }

  /* ---------- Page init ---------- */
  function initPage(root) {
    enhanceAllTables(root || document);
    enhanceFilters(root || document);
    enhanceMessages();
    // keep drawer closed after SPA page swap on small screens
    if (isDrawerMode()) closeSidebar();
  }

  function boot() {
    // Expose on XiuxianWeb without breaking existing API surface
    const api = {
      breakpoints: BP,
      isMobile,
      isDrawerMode,
      openSidebar,
      closeSidebar,
      setSidebarOpen,
      enhanceAllTables,
      enhanceFilters,
      showActionSheet,
      hideActionSheet,
      initPage,
    };
    window.XiuxianUI = api;
    if (window.XiuxianWeb) {
      Object.assign(window.XiuxianWeb, {
        ui: api,
        // non-breaking aliases
        closeSidebar: closeSidebar,
      });
    }

    // Patch closeSidebar used by base if present after load
    document.addEventListener("xiuxian:page-load", () => {
      const main = document.querySelector("main.container");
      initPage(main || document);
    });

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", () => initPage(document));
    } else {
      initPage(document);
    }

    // Resize: re-sync messages class; tables already dual-rendered
    let resizeTimer = null;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        if (!isDrawerMode()) closeSidebar();
        enhanceMessages();
      }, 120);
    });
  }

  boot();
})();
