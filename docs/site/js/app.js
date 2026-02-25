/* ===== Office Prospector — Dashboard App ===== */

(function () {
  "use strict";

  // ===== STATE =====
  let allFirms = [];
  let filteredFirms = [];
  let currentPage = 1;
  let pageSize = 100;
  let sortCol = "latestReturns";
  let sortDir = "desc";
  let selectedEfins = new Set();
  let expandedEfin = null;
  let notes = {};  // { efin: { status: "", notes: "" } }
  let hideChains = false;
  let hideEnriched = false;
  let noWebsiteOnly = false;
  let gasUrl = "";  // Google Apps Script URL

  // ===== DOM REFS =====
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  const searchInput = $("#search-input");
  const filterState = $("#filter-state");
  const filterCity = $("#filter-city");
  const filterMinReturns = $("#filter-min-returns");
  const filterMaxReturns = $("#filter-max-returns");
  const sortBySelect = $("#sort-by");
  const sortDirSelect = $("#sort-dir");
  const firmsBody = $("#firms-body");
  const selectAllCb = $("#select-all");
  const bulkBar = $("#bulk-bar");
  const bulkCount = $("#bulk-count");
  const pageSizeSelect = $("#page-size");

  // ===== INIT =====
  async function init() {
    loadSettings();
    loadLocalNotes();
    await loadData();
    bindEvents();
    applyFilter();
  }

  // Data URL: try GitHub Release asset first, fall back to local file
  const DATA_URLS = [
    "https://github.com/aabed-ghub/OfficeProspector/releases/latest/download/firms.json",
    "firms.json"
  ];

  async function loadData() {
    try {
      let resp;
      for (const url of DATA_URLS) {
        try {
          resp = await fetch(url);
          if (resp.ok) break;
        } catch (_) { /* try next */ }
      }
      if (!resp || !resp.ok) throw new Error("Failed to load firms.json");
      const data = await resp.json();
      allFirms = data.firms || [];
      updateStats(data);
      populateStateFilter(data.stateSummary || {});
      renderStateChart(data.stateSummary || {});
    } catch (err) {
      firmsBody.innerHTML = `<tr><td colspan="19" style="text-align:center;padding:40px;color:#6B7280;">
        Could not load data. Run the pipeline first: <code>python -m src.cli run</code>
        <br><small>${err.message}</small>
      </td></tr>`;
    }
  }

  // ===== STATS =====
  function updateStats(data) {
    $("#stat-total").textContent = (data.totalFirms || 0).toLocaleString();
    const avg = allFirms.length > 0
      ? Math.round(allFirms.reduce((s, f) => s + (f.latestReturns || 0), 0) / allFirms.length)
      : 0;
    $("#stat-avg-returns").textContent = avg.toLocaleString();
    $("#stat-enriched").textContent = (data.totalEnriched || 0).toLocaleString();
    const states = new Set(allFirms.map((f) => f.state)).size;
    $("#stat-states").textContent = states;
  }

  function populateStateFilter(stateSummary) {
    const states = Object.keys(stateSummary).sort();
    states.forEach((st) => {
      const opt = document.createElement("option");
      opt.value = st;
      opt.textContent = `${st} (${stateSummary[st]})`;
      filterState.appendChild(opt);
    });
  }

  function renderStateChart(stateSummary) {
    const chart = $("#state-chart");
    const entries = Object.entries(stateSummary).slice(0, 8);
    if (!entries.length) return;
    const max = entries[0][1];
    chart.innerHTML = entries
      .map(([st, count]) => {
        const pct = Math.max((count / max) * 100, 8);
        return `<div class="chart-bar-group">
          <div class="chart-bar" style="height:${pct}%" title="${st}: ${count}"></div>
          <span class="chart-label">${st}</span>
        </div>`;
      })
      .join("");
  }

  // ===== FILTERING =====
  function applyFilter() {
    const query = searchInput.value.toLowerCase().trim();
    const state = filterState.value;
    const city = filterCity.value;
    const minRet = parseInt(filterMinReturns.value) || 0;
    const maxRet = parseInt(filterMaxReturns.value) || Infinity;

    filteredFirms = allFirms.filter((f) => {
      if (hideChains && f.flaggedChain) return false;
      if (hideEnriched && f.isEnriched) return false;
      if (noWebsiteOnly && f.website) return false;
      if (query && !f.firmName.toLowerCase().includes(query) &&
          !(f.dba || "").toLowerCase().includes(query) &&
          !f.efin.includes(query)) return false;
      if (state && f.state !== state) return false;
      if (city && f.city !== city) return false;
      if (f.latestReturns < minRet) return false;
      if (f.latestReturns > maxRet) return false;
      return true;
    });

    sortFirms();
    currentPage = 1;
    renderTable();
    updatePagination();
    updateCityFilter();
  }

  function sortFirms() {
    const dir = sortDir === "asc" ? 1 : -1;
    filteredFirms.sort((a, b) => {
      let va = a[sortCol] ?? "";
      let vb = b[sortCol] ?? "";
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
      if (typeof va === "string") va = va.toLowerCase();
      if (typeof vb === "string") vb = vb.toLowerCase();
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return 0;
    });
  }

  function updateCityFilter() {
    const state = filterState.value;
    filterCity.innerHTML = "";
    if (!state) {
      filterCity.disabled = true;
      filterCity.innerHTML = '<option value="">Select a state first</option>';
      return;
    }
    filterCity.disabled = false;
    const cities = [...new Set(allFirms.filter((f) => f.state === state).map((f) => f.city))].sort();
    filterCity.innerHTML = '<option value="">All Cities</option>';
    cities.forEach((c) => {
      const opt = document.createElement("option");
      opt.value = c;
      opt.textContent = c;
      filterCity.appendChild(opt);
    });
  }

  // ===== TABLE RENDERING =====
  function renderTable() {
    const start = (currentPage - 1) * pageSize;
    const page = filteredFirms.slice(start, start + pageSize);

    if (page.length === 0) {
      firmsBody.innerHTML = `<tr><td colspan="19" style="text-align:center;padding:40px;color:#6B7280;">
        No firms match your filters.
      </td></tr>`;
      return;
    }

    firmsBody.innerHTML = page.map((f) => renderRow(f)).join("");

    // Restore expanded row
    if (expandedEfin) {
      const row = firmsBody.querySelector(`tr[data-efin="${expandedEfin}"]`);
      if (row) insertDetailRow(row, allFirms.find((ff) => ff.efin === expandedEfin));
    }
  }

  function renderRow(f) {
    const note = notes[f.efin] || {};
    const isSelected = selectedEfins.has(f.efin);
    const flaggedClass = f.flaggedChain ? " flagged" : "";
    const selectedClass = isSelected ? " selected" : "";

    const enrichedIcon = f.isEnriched
      ? '<span class="icon-enriched" title="Enriched">&#10003;</span>'
      : '<span class="icon-empty"></span>';

    const flaggedIcon = f.flaggedChain
      ? `<span class="icon-flagged" title="Flagged: ${esc(f.chainMatch)}">&#10007;</span>`
      : '<span class="icon-empty"></span>';

    const yoyClass = (f.yoyGrowth || 0) > 0 ? "growth-positive" : (f.yoyGrowth || 0) < 0 ? "growth-negative" : "";
    const yoy = f.yoyGrowth != null ? `<span class="${yoyClass}">${f.yoyGrowth > 0 ? "+" : ""}${f.yoyGrowth}%</span>` : "—";

    const website = f.website
      ? `<a class="website-link" href="${ensureUrl(f.website)}" target="_blank" title="${esc(f.website)}" onclick="event.stopPropagation()">${shortUrl(f.website)}</a>`
      : "";

    const sources = (f.enrichmentSources || [])
      .map((s) => `<span class="source-tag">${esc(s.replace("IRS ", ""))}</span>`)
      .join("");

    const statusClass = (note.status || "").toLowerCase().replace(/\s+/g, "");
    const statusBadge = note.status
      ? `<span class="status-badge status-${statusClass}">${esc(note.status)}</span>`
      : "";

    return `<tr data-efin="${f.efin}" class="${flaggedClass}${selectedClass}">
      <td class="col-checkbox"><input type="checkbox" class="row-cb" ${isSelected ? "checked" : ""} onclick="event.stopPropagation()"></td>
      <td class="col-firm" title="${esc(f.firmName)}">${esc(f.firmName)}</td>
      <td class="col-icon">${enrichedIcon}</td>
      <td class="col-icon">${flaggedIcon}</td>
      <td class="col-dba" title="${esc(f.dba || "")}">${esc(f.dba || "")}</td>
      <td class="col-efin">${f.efin}</td>
      <td class="col-addr" title="${esc(f.address || "")}">${esc(f.address || "")}</td>
      <td class="col-state">${f.state}</td>
      <td class="col-city" title="${esc(f.city || "")}">${esc(f.city || "")}</td>
      <td class="col-num">${(f.latestReturns || 0).toLocaleString()}</td>
      <td class="col-num">${yoy}</td>
      <td class="col-contact" title="${esc(f.keyContact || "")}">${esc(f.keyContact || "")}</td>
      <td class="col-contact" title="${esc(f.contactTitle || "")}">${esc(f.contactTitle || "")}</td>
      <td class="col-phone">${esc(f.phone || "")}</td>
      <td class="col-link">${website}</td>
      <td class="col-email" title="${esc(f.email || "")}">${esc(f.email || "")}</td>
      <td class="col-num">${f.preparerCount || "—"}</td>
      <td class="col-source">${sources}</td>
      <td class="col-status">${statusBadge}</td>
    </tr>`;
  }

  // ===== EXPANDED ROW =====
  function toggleRowExpand(efin) {
    const existing = firmsBody.querySelector(".expanded-row");
    if (existing) existing.remove();

    if (expandedEfin === efin) {
      expandedEfin = null;
      return;
    }

    expandedEfin = efin;
    const row = firmsBody.querySelector(`tr[data-efin="${efin}"]`);
    const firm = allFirms.find((f) => f.efin === efin);
    if (row && firm) insertDetailRow(row, firm);
  }

  function insertDetailRow(afterRow, firm) {
    const tpl = $("#row-detail-template").content.cloneNode(true);
    const note = notes[firm.efin] || {};

    // Firm details
    tpl.querySelector(".d-efin").textContent = firm.efin;
    tpl.querySelector(".d-name").textContent = firm.firmName;
    tpl.querySelector(".d-dba").textContent = firm.dba || "—";
    tpl.querySelector(".d-address").textContent =
      [firm.address, firm.city, firm.state, firm.zip].filter(Boolean).join(", ");
    tpl.querySelector(".d-phone").textContent = firm.phone || "—";

    const webEl = tpl.querySelector(".d-website");
    if (firm.website) {
      webEl.href = ensureUrl(firm.website);
      webEl.textContent = firm.website;
    } else {
      webEl.textContent = "—";
    }

    tpl.querySelector(".d-rating").textContent =
      firm.googleRating != null ? `${firm.googleRating} ★ (${firm.googleReviews || 0} reviews)` : "—";

    // Return history bars
    const retEl = tpl.querySelector(".d-returns");
    const vols = [
      { label: "Y-2", val: firm.returnsY2 },
      { label: "Y-1", val: firm.returnsY1 },
      { label: "Current", val: firm.latestReturns },
    ].filter((v) => v.val != null);
    const maxRet = Math.max(...vols.map((v) => v.val), 1);
    retEl.innerHTML = vols
      .map(
        (v) =>
          `<div class="return-bar-group">
            <div class="return-bar-value">${v.val.toLocaleString()}</div>
            <div class="return-bar" style="height:${Math.max((v.val / maxRet) * 50, 8)}px"></div>
            <div class="return-bar-label">${v.label}</div>
          </div>`
      )
      .join("");

    tpl.querySelector(".d-indiv-pct").textContent =
      firm.individualPct != null ? `${firm.individualPct}%` : "—";
    tpl.querySelector(".d-biz-pct").textContent =
      firm.businessPct != null ? `${firm.businessPct}%` : "—";

    // Contacts
    tpl.querySelector(".d-contact-count").textContent = (firm.contacts || []).length;
    const contactsBody = tpl.querySelector(".d-contacts");
    contactsBody.innerHTML = (firm.contacts || [])
      .map(
        (c) =>
          `<tr>
            <td>${esc(c.name)}</td>
            <td>${esc(c.title)}</td>
            <td class="${c.emailVerified ? "email-verified" : "email-unverified"}">${esc(c.email || "—")}${c.email && !c.emailVerified ? " (unverified)" : ""}</td>
            <td>${esc(c.phone || "—")}</td>
            <td><span class="source-tag">${esc(c.source || "")}</span></td>
          </tr>`
      )
      .join("");

    // Notes
    const statusSelect = tpl.querySelector(".d-status");
    statusSelect.value = note.status || "";
    const notesArea = tpl.querySelector(".d-notes");
    notesArea.value = note.notes || "";

    const saveBtn = tpl.querySelector(".d-save-note");
    saveBtn.addEventListener("click", () => {
      saveNote(firm.efin, statusSelect.value, notesArea.value);
    });

    afterRow.after(tpl);
  }

  // ===== NOTES (localStorage + Google Apps Script) =====
  function loadLocalNotes() {
    try {
      const stored = localStorage.getItem("op_notes");
      if (stored) notes = JSON.parse(stored);
    } catch (e) { /* ignore */ }
  }

  function saveLocalNotes() {
    localStorage.setItem("op_notes", JSON.stringify(notes));
  }

  function saveNote(efin, status, noteText) {
    notes[efin] = { status, notes: noteText, updatedAt: new Date().toISOString() };
    saveLocalNotes();

    // Sync to Google Apps Script if configured
    if (gasUrl) {
      syncNoteToGAS(efin, status, noteText);
    }

    // Re-render status badge in table
    const row = firmsBody.querySelector(`tr[data-efin="${efin}"]`);
    if (row) {
      const statusCell = row.querySelector(".col-status");
      if (statusCell) {
        const cls = status.toLowerCase().replace(/\s+/g, "");
        statusCell.innerHTML = status
          ? `<span class="status-badge status-${cls}">${esc(status)}</span>`
          : "";
      }
    }
  }

  async function syncNoteToGAS(efin, status, noteText) {
    try {
      await fetch(gasUrl, {
        method: "POST",
        mode: "no-cors",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ efin, status, notes: noteText, timestamp: new Date().toISOString() }),
      });
    } catch (e) { /* silent fail for no-cors */ }
  }

  async function loadNotesFromGAS() {
    if (!gasUrl) return;
    try {
      const resp = await fetch(gasUrl);
      if (resp.ok) {
        const data = await resp.json();
        if (data && typeof data === "object") {
          // Merge: GAS data wins over local if newer
          Object.entries(data).forEach(([efin, note]) => {
            const local = notes[efin];
            if (!local || new Date(note.updatedAt) > new Date(local.updatedAt || 0)) {
              notes[efin] = note;
            }
          });
          saveLocalNotes();
        }
      }
    } catch (e) { /* ignore */ }
  }

  // ===== PAGINATION =====
  function updatePagination() {
    const total = filteredFirms.length;
    const totalPages = Math.ceil(total / pageSize);
    const start = (currentPage - 1) * pageSize + 1;
    const end = Math.min(currentPage * pageSize, total);

    $("#page-showing").textContent = total > 0 ? `${start}-${end}` : "0";
    $("#page-total").textContent = total.toLocaleString();
    $("#btn-prev").disabled = currentPage <= 1;
    $("#btn-next").disabled = currentPage >= totalPages;

    // Page numbers
    const pageNums = $("#page-numbers");
    const pages = [];
    for (let i = Math.max(1, currentPage - 2); i <= Math.min(totalPages, currentPage + 2); i++) {
      pages.push(i);
    }
    pageNums.innerHTML = pages
      .map((p) => `<button class="page-num${p === currentPage ? " active" : ""}" data-page="${p}">${p}</button>`)
      .join("");
  }

  // ===== CSV EXPORT =====
  function exportCSV() {
    const rows = filteredFirms.filter((f) => selectedEfins.size === 0 || selectedEfins.has(f.efin));
    const headers = [
      "EFIN", "Firm Name", "DBA", "State", "City", "Address", "ZIP", "Phone", "Website", "Email",
      "Latest Returns", "Returns Y-1", "Returns Y-2", "YoY Growth %",
      "Key Contact", "Contact Title", "Contact Email", "Contact Phone",
      "Preparer Count", "Flagged Chain", "Chain Match", "Enriched", "Sources", "Status", "Notes",
    ];

    const csvRows = [headers.join(",")];
    rows.forEach((f) => {
      const note = notes[f.efin] || {};
      csvRows.push(
        [
          f.efin, q(f.firmName), q(f.dba), f.state, q(f.city), q(f.address), f.zip,
          q(f.phone), q(f.website), q(f.email),
          f.latestReturns, f.returnsY1 ?? "", f.returnsY2 ?? "", f.yoyGrowth ?? "",
          q(f.keyContact), q(f.contactTitle), q(f.contactEmail), q(f.contactPhone),
          f.preparerCount, f.flaggedChain ? "Yes" : "", q(f.chainMatch),
          f.isEnriched ? "Yes" : "", q((f.enrichmentSources || []).join("; ")),
          q(note.status || ""), q(note.notes || ""),
        ].join(",")
      );
    });

    const blob = new Blob([csvRows.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `office_prospector_export_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ===== SETTINGS =====
  function loadSettings() {
    gasUrl = localStorage.getItem("op_gas_url") || "";
    if (gasUrl) loadNotesFromGAS();
  }

  function saveSettings() {
    gasUrl = $("#setting-gas-url").value.trim();
    localStorage.setItem("op_gas_url", gasUrl);
    $("#settings-modal").classList.add("hidden");
    if (gasUrl) loadNotesFromGAS();
  }

  // ===== EVENTS =====
  function bindEvents() {
    // Search
    let searchTimer;
    searchInput.addEventListener("input", () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(applyFilter, 200);
    });

    // Ctrl+F shortcut
    document.addEventListener("keydown", (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        e.preventDefault();
        searchInput.focus();
        searchInput.select();
      }
    });

    // Filters
    filterState.addEventListener("change", applyFilter);
    filterCity.addEventListener("change", applyFilter);
    filterMinReturns.addEventListener("change", applyFilter);
    filterMaxReturns.addEventListener("change", applyFilter);
    sortBySelect.addEventListener("change", () => { sortCol = sortBySelect.value; applyFilter(); });
    sortDirSelect.addEventListener("change", () => { sortDir = sortDirSelect.value; applyFilter(); });

    // Toggle advanced filters
    $("#btn-toggle-filters").addEventListener("click", () => {
      $("#advanced-filters").classList.toggle("hidden");
    });

    // Quick filter pills
    $$(".quick-filters .pill").forEach((pill) => {
      pill.addEventListener("click", () => handleQuickFilter(pill));
    });

    // Column header sorting
    $$("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const col = th.dataset.col;
        if (sortCol === col) {
          sortDir = sortDir === "desc" ? "asc" : "desc";
        } else {
          sortCol = col;
          sortDir = "desc";
        }
        $$("th.sortable").forEach((t) => t.classList.remove("sort-asc", "sort-desc"));
        th.classList.add(sortDir === "asc" ? "sort-asc" : "sort-desc");
        sortBySelect.value = col;
        sortDirSelect.value = sortDir;
        applyFilter();
      });
    });

    // Row click → expand
    firmsBody.addEventListener("click", (e) => {
      const row = e.target.closest("tr[data-efin]");
      if (!row || e.target.closest("input, a, button, .expanded-row")) return;
      toggleRowExpand(row.dataset.efin);
    });

    // Row checkbox
    firmsBody.addEventListener("change", (e) => {
      if (!e.target.classList.contains("row-cb")) return;
      const row = e.target.closest("tr[data-efin]");
      if (!row) return;
      const efin = row.dataset.efin;
      if (e.target.checked) {
        selectedEfins.add(efin);
        row.classList.add("selected");
      } else {
        selectedEfins.delete(efin);
        row.classList.remove("selected");
      }
      updateBulkBar();
    });

    // Select all
    selectAllCb.addEventListener("change", () => {
      const start = (currentPage - 1) * pageSize;
      const page = filteredFirms.slice(start, start + pageSize);
      page.forEach((f) => {
        if (selectAllCb.checked) selectedEfins.add(f.efin);
        else selectedEfins.delete(f.efin);
      });
      renderTable();
      updateBulkBar();
    });

    // Bulk actions
    $("#btn-bulk-apply").addEventListener("click", () => {
      const status = $("#bulk-status").value;
      if (!status) return;
      selectedEfins.forEach((efin) => saveNote(efin, status, (notes[efin] || {}).notes || ""));
      renderTable();
    });

    $("#btn-bulk-clear").addEventListener("click", () => {
      selectedEfins.clear();
      selectAllCb.checked = false;
      renderTable();
      updateBulkBar();
    });

    // Pagination
    $("#btn-prev").addEventListener("click", () => { currentPage--; renderTable(); updatePagination(); });
    $("#btn-next").addEventListener("click", () => { currentPage++; renderTable(); updatePagination(); });
    $("#page-numbers").addEventListener("click", (e) => {
      if (e.target.classList.contains("page-num")) {
        currentPage = parseInt(e.target.dataset.page);
        renderTable();
        updatePagination();
      }
    });
    pageSizeSelect.addEventListener("change", () => {
      pageSize = parseInt(pageSizeSelect.value);
      currentPage = 1;
      renderTable();
      updatePagination();
    });

    // Export CSV
    $("#btn-export-csv").addEventListener("click", exportCSV);

    // Settings
    $("#btn-settings").addEventListener("click", () => {
      $("#setting-gas-url").value = gasUrl;
      $("#settings-modal").classList.remove("hidden");
    });
    $("#btn-close-settings").addEventListener("click", () => { $("#settings-modal").classList.add("hidden"); });
    $("#btn-save-settings").addEventListener("click", saveSettings);
    $("#settings-modal").addEventListener("click", (e) => {
      if (e.target === e.currentTarget) e.currentTarget.classList.add("hidden");
    });
  }

  function handleQuickFilter(pill) {
    const filter = pill.dataset.filter;

    // Toggle pills
    if (filter === "hideChains") {
      hideChains = !hideChains;
      pill.classList.toggle("active");
      applyFilter();
      return;
    }
    if (filter === "hideEnriched") {
      hideEnriched = !hideEnriched;
      pill.classList.toggle("active");
      applyFilter();
      return;
    }

    if (filter === "clear") {
      searchInput.value = "";
      filterState.value = "";
      filterCity.value = "";
      filterMinReturns.value = "";
      filterMaxReturns.value = "";
      sortCol = "latestReturns";
      sortDir = "desc";
      hideChains = false;
      hideEnriched = false;
      noWebsiteOnly = false;
      sortBySelect.value = sortCol;
      sortDirSelect.value = sortDir;
      $$(".pill.active").forEach((p) => p.classList.remove("active"));
      $$(".pill[data-filter='highest']").forEach((p) => p.classList.add("active"));
      applyFilter();
      return;
    }

    // Sort pills (mutually exclusive)
    noWebsiteOnly = false;  // Reset whenever any sort pill is clicked
    $$(".quick-filters .pill:not(.toggle-pill):not(.pill-clear)").forEach((p) => p.classList.remove("active"));
    pill.classList.add("active");

    switch (filter) {
      case "highest":
        sortCol = "latestReturns"; sortDir = "desc"; break;
      case "lowest":
        sortCol = "latestReturns"; sortDir = "asc"; break;
      case "az":
        sortCol = "firmName"; sortDir = "asc"; break;
      case "growth":
        sortCol = "yoyGrowth"; sortDir = "desc"; break;
      case "nowebsite":
        noWebsiteOnly = true;
        sortCol = "latestReturns"; sortDir = "desc"; break;
    }
    sortBySelect.value = sortCol;
    sortDirSelect.value = sortDir;
    applyFilter();
  }

  function updateBulkBar() {
    const count = selectedEfins.size;
    if (count > 0) {
      bulkBar.classList.remove("hidden");
      bulkCount.textContent = `${count} selected`;
    } else {
      bulkBar.classList.add("hidden");
    }
  }

  // ===== HELPERS =====
  function esc(str) {
    if (!str) return "";
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function q(str) {
    // CSV quote
    if (!str) return "";
    if (str.includes(",") || str.includes('"') || str.includes("\n")) {
      return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
  }

  function ensureUrl(url) {
    if (!url) return "";
    return url.startsWith("http") ? url : "https://" + url;
  }

  function shortUrl(url) {
    return url.replace(/^https?:\/\/(www\.)?/, "").replace(/\/$/, "").slice(0, 30);
  }

  // ===== LAUNCH =====
  init();
})();
