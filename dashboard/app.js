/* app.js — Data Contract Enforcer Dashboard */

// ── State ─────────────────────────────────────────────────────────────────
const state = {
  page: 'overview',
  contracts: [],
  reports: [],
  baselines: {},
  summary: {},
  loading: false,
};

// ── Router ────────────────────────────────────────────────────────────────
function navigate(page) {
  state.page = page;
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.page === page);
  });
  renderPage();
}

// ── API helpers ───────────────────────────────────────────────────────────
async function api(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function loadAll() {
  const [summary, contracts, reports, baselines] = await Promise.all([
    api('/api/summary'),
    api('/api/contracts'),
    api('/api/reports'),
    api('/api/baselines'),
  ]);
  state.summary   = summary;
  state.contracts = contracts;
  state.reports   = reports;
  state.baselines = baselines;
}

// ── Badge helpers ─────────────────────────────────────────────────────────
function statusBadge(status) {
  const cls = { PASS: 'pass', WARN: 'warn', FAIL: 'fail', CRITICAL: 'critical' }[status] || 'muted';
  return `<span class="badge badge-${cls}">${status}</span>`;
}

function checkBadge(check) {
  return `<span class="badge badge-info">${check}</span>`;
}

// ── Pages ─────────────────────────────────────────────────────────────────

function renderOverview() {
  const s = state.summary;
  const sc = s.status_counts || {};

  const passColor  = sc.PASS  ? 'var(--pass)'  : 'var(--muted)';
  const warnColor  = sc.WARN  ? 'var(--warn)'  : 'var(--muted)';
  const failColor  = sc.FAIL  ? 'var(--fail)'  : 'var(--muted)';

  return `
    <div class="phase-banner">
      <span class="phase-label">Phase 1</span>
      <span>ContractGenerator &amp; ValidationRunner — live results below. Dashboard grows with each phase.</span>
    </div>

    <div class="stat-grid">
      <div class="stat-card">
        <span class="label">Contracts</span>
        <span class="value" style="color:var(--accent)">${s.contracts ?? '—'}</span>
        <span class="sub">Generated YAML contracts</span>
      </div>
      <div class="stat-card">
        <span class="label">Validation Runs</span>
        <span class="value">${s.reports ?? '—'}</span>
        <span class="sub">Reports on disk</span>
      </div>
      <div class="stat-card">
        <span class="label">Pass</span>
        <span class="value" style="color:${passColor}">${sc.PASS ?? 0}</span>
        <span class="sub">Clean runs</span>
      </div>
      <div class="stat-card">
        <span class="label">Warn</span>
        <span class="value" style="color:${warnColor}">${sc.WARN ?? 0}</span>
        <span class="sub">Drift warnings</span>
      </div>
      <div class="stat-card">
        <span class="label">Fail</span>
        <span class="value" style="color:${failColor}">${sc.FAIL ?? 0}</span>
        <span class="sub">Contract violations</span>
      </div>
      <div class="stat-card">
        <span class="label">Total Violations</span>
        <span class="value" style="color:${s.total_violations ? 'var(--fail)' : 'var(--pass)'}">${s.total_violations ?? 0}</span>
        <span class="sub">Across all runs</span>
      </div>
      <div class="stat-card">
        <span class="label">Baselined Cols</span>
        <span class="value">${s.baselined_columns ?? 0}</span>
        <span class="sub">Statistical anchors</span>
      </div>
    </div>

    <div class="section">
      <h2>Recent Validation Runs</h2>
      ${renderReportsTable(state.reports.slice(0, 5), true)}
    </div>

    <div class="section">
      <h2>Contracts</h2>
      ${renderContractsTable(state.contracts, true)}
    </div>
  `;
}

function renderContractsPage() {
  return `
    <div class="page-header">
      <div><h1>Contracts</h1><p class="page-subtitle">Auto-generated Bitol YAML contracts from JSONL source data</p></div>
      <button class="btn" onclick="refresh()">Refresh</button>
    </div>
    ${renderContractsTable(state.contracts, false)}
  `;
}

function renderContractsTable(contracts, compact) {
  if (!contracts.length) return `<div class="empty">No contracts found in generated_contracts/</div>`;
  const rows = contracts.map(c => `
    <tr onclick="openContractPanel('${c.id}')">
      <td class="text-mono" style="color:var(--info)">${c.id}</td>
      <td>${c.record_count ?? '—'}</td>
      <td>${c.field_count ?? '—'}</td>
      <td>${c.quality_checks ?? '—'}</td>
      <td class="text-muted" style="font-size:12px">${c.generated_at ? c.generated_at.replace('T',' ').replace('Z','') : '—'}</td>
    </tr>
  `).join('');
  return `
    <div class="card table-wrap">
      <table>
        <thead><tr>
          <th>Contract ID</th><th>Records</th><th>Fields</th><th>Quality Checks</th><th>Generated At</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderReportsPage() {
  return `
    <div class="page-header">
      <div><h1>Validation Reports</h1><p class="page-subtitle">Each run of the ValidationRunner produces one report</p></div>
      <button class="btn" onclick="refresh()">Refresh</button>
    </div>
    ${renderReportsTable(state.reports, false)}
  `;
}

function renderReportsTable(reports, compact) {
  if (!reports.length) return `<div class="empty">No validation reports found in validation_reports/</div>`;
  const rows = reports.map(r => `
    <tr onclick="openReportPanel('${r.filename}')">
      <td>${statusBadge(r.overall_status)}</td>
      <td class="text-mono" style="color:var(--info);font-size:12px">${r.contract_id}</td>
      <td>${r.record_count ?? '—'}</td>
      <td style="color:${r.violation_count ? 'var(--fail)' : 'var(--pass)'}">${r.violation_count}</td>
      <td style="font-size:11px" class="text-muted">${r.filename}</td>
      <td class="text-muted" style="font-size:12px">${r.validated_at ? r.validated_at.replace('T',' ').replace('Z','') : '—'}</td>
    </tr>
  `).join('');
  return `
    <div class="card table-wrap">
      <table>
        <thead><tr>
          <th>Status</th><th>Contract</th><th>Records</th><th>Violations</th><th>File</th><th>Validated At</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderBaselinesPage() {
  const cols = state.baselines.columns || {};
  const keys = Object.keys(cols);
  return `
    <div class="page-header">
      <div><h1>Statistical Baselines</h1><p class="page-subtitle">Anchors used for drift detection — written on first ValidationRunner pass</p></div>
      <button class="btn" onclick="refresh()">Refresh</button>
    </div>
    <p class="text-muted mb-16" style="font-size:12px">Written at: ${state.baselines.written_at || '—'}</p>
    ${keys.length === 0 ? '<div class="empty">No baselines yet — run the ValidationRunner first.</div>' : `
    <div class="card table-wrap">
      <table>
        <thead><tr><th>Column</th><th>Baseline Mean</th><th>Baseline StdDev</th></tr></thead>
        <tbody>
          ${keys.map(k => `
            <tr>
              <td class="text-mono" style="color:var(--info)">${k}</td>
              <td>${cols[k].mean?.toFixed(4) ?? '—'}</td>
              <td>${cols[k].stddev?.toFixed(4) ?? '—'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>`}
  `;
}

function renderEnforcerReportPage() {
  return `
    <div class="page-header">
      <div><h1>Enforcer Report</h1><p class="page-subtitle">Health-scored pipeline report — available after Phase 3</p></div>
    </div>
    <div class="card" style="text-align:center; padding:60px 20px; color:var(--muted)">
      <div style="font-size:40px; margin-bottom:12px">🔒</div>
      <div style="font-size:15px; margin-bottom:8px">Available in Phase 3</div>
      <div style="font-size:13px">The Enforcer Report with health scores, blast radius, and attribution chains will appear here once <code>report_generator.py</code> is implemented.</div>
    </div>
  `;
}

// ── Detail Panels ─────────────────────────────────────────────────────────

async function openContractPanel(id) {
  const panel = document.getElementById('detail-panel');
  panel.innerHTML = `<button class="panel-close" onclick="closePanel()">✕ Close</button><div class="loading">Loading…</div>`;
  panel.classList.add('open');

  try {
    const c = await api(`/api/contracts/${id}`);
    const props = c.schema?.properties || {};
    const checks = c.quality?.checks || [];

    const fields = Object.entries(props).map(([name, clause]) => {
      const tags = [];
      if (clause.required) tags.push(`<span class="badge badge-info">required</span>`);
      if (clause.format)   tags.push(`<span class="badge badge-muted">${clause.format}</span>`);
      if (clause.minimum !== undefined) tags.push(`<span class="badge badge-muted">min ${clause.minimum}</span>`);
      if (clause.maximum !== undefined) tags.push(`<span class="badge badge-muted">max ${clause.maximum}</span>`);
      if (clause.enum)     tags.push(`<span class="badge badge-warn">enum(${clause.enum.length})</span>`);
      return `
        <div class="field-chip">
          <div class="fname">${name}</div>
          <div class="fmeta">${clause.type || '—'} &nbsp; ${tags.join(' ')}</div>
        </div>
      `;
    }).join('');

    const checksHtml = checks.length ? checks.map(ch => `
      <div style="padding:8px 12px; background:var(--surface2); border-radius:6px; margin-bottom:8px; font-size:12px">
        ${checkBadge(ch.type)} <span class="text-mono" style="color:var(--info); margin-left:6px">${ch.field}</span>
        ${ch.severity ? `<span class="badge badge-${ch.severity.toLowerCase() === 'critical' ? 'critical' : 'muted'}" style="margin-left:6px">${ch.severity}</span>` : ''}
        ${ch.z_score_warn ? `<span class="text-muted" style="margin-left:8px">warn@${ch.z_score_warn}σ fail@${ch.z_score_fail}σ</span>` : ''}
      </div>
    `).join('') : '<div class="empty">No quality checks defined.</div>';

    panel.innerHTML = `
      <button class="panel-close" onclick="closePanel()">✕ Close</button>
      <h2 style="margin-bottom:4px">${c.id}</h2>
      <p class="text-muted" style="font-size:12px; margin-bottom:20px">${c.description || ''}</p>

      <div class="flex gap-8 mb-16">
        <span class="badge badge-info">v${c.version}</span>
        <span class="badge badge-muted">${c.source?.record_count ?? '—'} records</span>
        <span class="badge badge-muted">${Object.keys(props).length} fields</span>
      </div>

      <h3>Schema Fields</h3>
      <div class="field-grid">${fields}</div>

      <h3 style="margin-top:20px">Quality Checks (${checks.length})</h3>
      ${checksHtml}

      <h3 style="margin-top:20px">Lineage</h3>
      <div style="font-size:12px; color:var(--muted)">
        Upstream: ${(c.lineage?.upstream || []).length} &nbsp;|&nbsp; Downstream: ${(c.lineage?.downstream || []).length}
      </div>
    `;
  } catch (e) {
    panel.innerHTML = `<button class="panel-close" onclick="closePanel()">✕ Close</button><div class="empty">Error: ${e.message}</div>`;
  }
}

async function openReportPanel(filename) {
  const panel = document.getElementById('detail-panel');
  panel.innerHTML = `<button class="panel-close" onclick="closePanel()">✕ Close</button><div class="loading">Loading…</div>`;
  panel.classList.add('open');

  try {
    const r = await api(`/api/reports/${filename}`);
    const violations = r.violations || [];

    const vHtml = violations.length ? violations.map(v => `
      <div class="violation-item">
        <div>${statusBadge(v.status)}</div>
        <div class="v-meta">
          ${checkBadge(v.check)}
          <span class="text-mono" style="color:var(--info);font-size:12px">${v.field}</span>
        </div>
        <div class="v-reason">${v.reason}</div>
        ${v.sample ? `<div class="v-sample">Sample: ${JSON.stringify(v.sample)}</div>` : ''}
      </div>
    `).join('') : `<div class="empty" style="color:var(--pass)">No violations — all checks passed.</div>`;

    panel.innerHTML = `
      <button class="panel-close" onclick="closePanel()">✕ Close</button>
      <div class="flex gap-8 mb-16" style="margin-top:4px">
        ${statusBadge(r.overall_status)}
        <span class="badge badge-info text-mono">${r.contract_id}</span>
      </div>
      <h2 style="margin-bottom:4px">${filename}</h2>
      <p class="text-muted" style="font-size:12px; margin-bottom:20px">${r.validated_at?.replace('T',' ').replace('Z','') || '—'}</p>

      <div class="stat-grid" style="grid-template-columns:repeat(4,1fr); margin-bottom:20px">
        <div class="stat-card" style="padding:14px">
          <span class="label">Records</span>
          <span class="value" style="font-size:22px">${r.record_count}</span>
        </div>
        <div class="stat-card" style="padding:14px">
          <span class="label">Critical</span>
          <span class="value" style="font-size:22px;color:var(--fail)">${r.summary?.CRITICAL ?? 0}</span>
        </div>
        <div class="stat-card" style="padding:14px">
          <span class="label">Fail</span>
          <span class="value" style="font-size:22px;color:var(--fail)">${r.summary?.FAIL ?? 0}</span>
        </div>
        <div class="stat-card" style="padding:14px">
          <span class="label">Warn</span>
          <span class="value" style="font-size:22px;color:var(--warn)">${r.summary?.WARN ?? 0}</span>
        </div>
      </div>

      <h3>Violations (${violations.length})</h3>
      <div style="margin-top:12px">${vHtml}</div>
    `;
  } catch (e) {
    panel.innerHTML = `<button class="panel-close" onclick="closePanel()">✕ Close</button><div class="empty">Error: ${e.message}</div>`;
  }
}

function closePanel() {
  document.getElementById('detail-panel').classList.remove('open');
}

// ── Render ────────────────────────────────────────────────────────────────

function renderPage() {
  const main = document.getElementById('main-content');
  const pages = {
    overview:  renderOverview,
    contracts: renderContractsPage,
    reports:   renderReportsPage,
    baselines: renderBaselinesPage,
    enforcer:  renderEnforcerReportPage,
  };
  main.innerHTML = (pages[state.page] || renderOverview)();
}

async function refresh() {
  const main = document.getElementById('main-content');
  main.innerHTML = `<div class="loading">Loading data…</div>`;
  try {
    await loadAll();
    renderPage();
  } catch (e) {
    main.innerHTML = `<div class="empty">Could not load data: ${e.message}</div>`;
  }
}

// ── Boot ──────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.nav-item').forEach(el => {
    el.addEventListener('click', () => navigate(el.dataset.page));
  });
  refresh();

  // Auto-refresh every 10s
  setInterval(() => {
    loadAll().then(() => renderPage()).catch(() => {});
  }, 10000);
});
