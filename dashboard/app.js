/* app.js — Data Contract Enforcer Dashboard */

// ── State ─────────────────────────────────────────────────────────────────
const state = {
  page: 'overview',
  contracts: [],
  reports: [],
  baselines: {},
  summary: {},
};

// ── Router ────────────────────────────────────────────────────────────────
function navigate(page) {
  state.page = page;
  document.querySelectorAll('.nav-item').forEach(el =>
    el.classList.toggle('active', el.dataset.page === page)
  );
  renderPage();
}

// ── API ───────────────────────────────────────────────────────────────────
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
  Object.assign(state, { summary, contracts, reports, baselines });
}

// ── Helpers ───────────────────────────────────────────────────────────────
function statusBadge(status) {
  const map = { PASS: 'pass', WARN: 'warn', FAIL: 'fail', CRITICAL: 'critical' };
  const cls = map[status] || 'neutral';
  const dot = `<span class="dot dot-${cls}"></span>`;
  return `<span class="badge badge-${cls}">${dot} ${status}</span>`;
}

function typeBadge(type) {
  return `<span class="badge badge-neutral">${type}</span>`;
}

function checkBadge(check) {
  return `<span class="badge badge-info">${check}</span>`;
}

function fmtDate(iso) {
  if (!iso) return '—';
  return iso.replace('T', ' ').replace('Z', '').slice(0, 16);
}

function violationIcon(status) {
  if (status === 'CRITICAL' || status === 'FAIL') return { icon: '✕', cls: 'v-icon-critical' };
  if (status === 'WARN') return { icon: '⚠', cls: 'v-icon-warn' };
  return { icon: '●', cls: '' };
}

// ── Pages ─────────────────────────────────────────────────────────────────

function renderOverview() {
  const s = state.summary;
  const sc = s.status_counts || {};
  const passRate = s.reports ? Math.round((sc.PASS || 0) / s.reports * 100) : 0;

  return `
    <div class="page-header">
      <div>
        <h1>Overview</h1>
        <div class="subtitle">Plan, enforce, and monitor your data contracts.</div>
      </div>
      <button class="btn" onclick="refresh()">↻ Refresh</button>
    </div>

    <!-- Stat cards -->
    <div class="stat-grid">
      <div class="stat-card accent">
        <div class="s-label">Total Contracts</div>
        <div class="s-value">${s.contracts ?? 0}</div>
        <div class="s-trend">Active YAML contracts</div>
        <div class="s-arrow">↗</div>
      </div>
      <div class="stat-card">
        <div class="s-label">Validation Runs</div>
        <div class="s-value">${s.reports ?? 0}</div>
        <div class="s-trend">Reports on disk</div>
        <div class="s-arrow">↗</div>
      </div>
      <div class="stat-card">
        <div class="s-label">Pass Rate</div>
        <div class="s-value" style="color:var(--pass)">${passRate}%</div>
        <div class="progress-bar"><div class="progress-fill" style="width:${passRate}%"></div></div>
        <div class="s-trend" style="margin-top:6px">${sc.PASS ?? 0} passing runs</div>
      </div>
      <div class="stat-card">
        <div class="s-label">Total Violations</div>
        <div class="s-value" style="color:${s.total_violations ? 'var(--fail)' : 'var(--pass)'}">${s.total_violations ?? 0}</div>
        <div class="s-trend">${sc.FAIL ?? 0} failing · ${sc.WARN ?? 0} warnings</div>
        <div class="s-arrow">↗</div>
      </div>
      <div class="stat-card">
        <div class="s-label">Baselined Columns</div>
        <div class="s-value">${s.baselined_columns ?? 0}</div>
        <div class="s-trend">Statistical anchors</div>
        <div class="s-arrow">↗</div>
      </div>
    </div>

    <!-- Two-column section -->
    <div class="grid-2">

      <!-- Recent validation runs -->
      <div class="card">
        <div class="card-header">
          <h2>Recent Validation Runs</h2>
          <span class="card-action" onclick="navigate('reports')">View all →</span>
        </div>
        <div class="table-wrap">
          ${renderReportRows(state.reports.slice(0, 5))}
        </div>
      </div>

      <!-- Contracts -->
      <div class="card">
        <div class="card-header">
          <h2>Contracts</h2>
          <span class="card-action" onclick="navigate('contracts')">View all →</span>
        </div>
        <div class="table-wrap">
          ${renderContractRows(state.contracts.slice(0, 5))}
        </div>
      </div>

    </div>
  `;
}

function renderContractsPage() {
  return `
    <div class="page-header">
      <div>
        <h1>Contracts</h1>
        <div class="subtitle">Auto-generated Bitol YAML contracts profiled from JSONL source data.</div>
      </div>
      <button class="btn" onclick="refresh()">↻ Refresh</button>
    </div>
    <div class="card">
      <div class="card-header">
        <h2>${state.contracts.length} Contract${state.contracts.length !== 1 ? 's' : ''}</h2>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Contract ID</th>
              <th>Records</th>
              <th>Fields</th>
              <th>Quality Checks</th>
              <th>Generated</th>
            </tr>
          </thead>
          <tbody>
            ${state.contracts.length ? state.contracts.map(c => `
              <tr onclick="openContractPanel('${c.id}')">
                <td>
                  <span style="font-weight:600">${c.id}</span>
                  <div class="text-muted text-small mt-4">${c.filename}</div>
                </td>
                <td>${c.record_count ?? '—'}</td>
                <td>${c.field_count ?? '—'}</td>
                <td>${c.quality_checks ?? '—'}</td>
                <td class="text-muted text-small">${fmtDate(c.generated_at)}</td>
              </tr>
            `).join('') : `<tr><td colspan="5" class="empty">No contracts found.</td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderReportsPage() {
  return `
    <div class="page-header">
      <div>
        <h1>Validation Reports</h1>
        <div class="subtitle">Each run of the ValidationRunner produces one timestamped report.</div>
      </div>
      <button class="btn" onclick="refresh()">↻ Refresh</button>
    </div>
    <div class="card">
      <div class="card-header">
        <h2>${state.reports.length} Report${state.reports.length !== 1 ? 's' : ''}</h2>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Contract</th>
              <th>Records</th>
              <th>Violations</th>
              <th>Validated</th>
            </tr>
          </thead>
          <tbody>
            ${state.reports.length ? state.reports.map(r => `
              <tr onclick="openReportPanel('${r.filename}')">
                <td>${statusBadge(r.overall_status)}</td>
                <td>
                  <span style="font-weight:500">${r.contract_id}</span>
                  <div class="text-muted text-small mt-4">${r.filename}</div>
                </td>
                <td>${r.record_count ?? '—'}</td>
                <td style="color:${r.violation_count ? 'var(--fail)' : 'var(--pass)'}; font-weight:600">
                  ${r.violation_count}
                </td>
                <td class="text-muted text-small">${fmtDate(r.validated_at)}</td>
              </tr>
            `).join('') : `<tr><td colspan="5" class="empty">No reports found.</td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderBaselinesPage() {
  const cols = state.baselines.columns || {};
  const keys = Object.keys(cols);
  return `
    <div class="page-header">
      <div>
        <h1>Statistical Baselines</h1>
        <div class="subtitle">Numeric column anchors used for drift detection. Written on first ValidationRunner pass.</div>
      </div>
      <button class="btn" onclick="refresh()">↻ Refresh</button>
    </div>
    <div class="card">
      <div class="card-header">
        <h2>${keys.length} Baselined Column${keys.length !== 1 ? 's' : ''}</h2>
        <span class="text-muted text-small">Written: ${state.baselines.written_at ? fmtDate(state.baselines.written_at) : '—'}</span>
      </div>
      ${keys.length === 0 ? `<div class="empty">No baselines yet — run the ValidationRunner first.</div>` : `
      <div class="table-wrap">
        <table>
          <thead><tr><th>Column</th><th>Baseline Mean</th><th>Baseline StdDev</th><th>Drift Trigger (WARN)</th><th>Drift Trigger (FAIL)</th></tr></thead>
          <tbody>
            ${keys.map(k => `
              <tr>
                <td class="text-mono" style="color:var(--info)">${k}</td>
                <td>${cols[k].mean?.toFixed(4) ?? '—'}</td>
                <td>${cols[k].stddev?.toFixed(4) ?? '—'}</td>
                <td class="text-muted">${cols[k].mean != null && cols[k].stddev != null ? (cols[k].mean + 2 * cols[k].stddev).toFixed(4) : '—'}</td>
                <td class="text-muted">${cols[k].mean != null && cols[k].stddev != null ? (cols[k].mean + 3 * cols[k].stddev).toFixed(4) : '—'}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>`}
    </div>
  `;
}

function renderEnforcerPage() {
  return `
    <div class="page-header">
      <div>
        <h1>Enforcer Report</h1>
        <div class="subtitle">Health-scored pipeline report with violation attribution and blast radius.</div>
      </div>
    </div>
    <div class="card">
      <div class="locked-state">
        <div class="lock-icon">🔒</div>
        <h3>Available in Phase 3</h3>
        <p>The Enforcer Report with health scores, attribution chains, and blast radius analysis will appear here once <code>report_generator.py</code> is implemented.</p>
      </div>
    </div>
  `;
}

// ── Shared table snippets ─────────────────────────────────────────────────

function renderReportRows(reports) {
  if (!reports.length) return `<div class="empty">No validation reports yet.</div>`;
  return `
    <table>
      <thead><tr><th>Status</th><th>Contract</th><th>Violations</th><th>Date</th></tr></thead>
      <tbody>
        ${reports.map(r => `
          <tr onclick="openReportPanel('${r.filename}')">
            <td>${statusBadge(r.overall_status)}</td>
            <td style="font-size:12px; font-weight:500">${r.contract_id}</td>
            <td style="color:${r.violation_count ? 'var(--fail)' : 'var(--pass)'}; font-weight:600">${r.violation_count}</td>
            <td class="text-muted text-small">${fmtDate(r.validated_at)}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
}

function renderContractRows(contracts) {
  if (!contracts.length) return `<div class="empty">No contracts generated yet.</div>`;
  return `
    <table>
      <thead><tr><th>Contract ID</th><th>Fields</th><th>Records</th></tr></thead>
      <tbody>
        ${contracts.map(c => `
          <tr onclick="openContractPanel('${c.id}')">
            <td style="font-weight:500; font-size:12px">${c.id}</td>
            <td>${c.field_count ?? '—'}</td>
            <td>${c.record_count ?? '—'}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
}

// ── Detail panels ─────────────────────────────────────────────────────────

async function openContractPanel(id) {
  const panel = document.getElementById('detail-panel');
  panel.innerHTML = `<button class="panel-close" onclick="closePanel()">✕ Close</button><div class="loading">Loading…</div>`;
  panel.classList.add('open');
  try {
    const c = await api(`/api/contracts/${id}`);
    const props = c.schema?.properties || {};
    const checks = c.quality?.checks || [];

    const fieldHtml = Object.entries(props).map(([name, clause]) => {
      const tags = [];
      if (clause.required) tags.push(`<span class="badge badge-green" style="font-size:10px">required</span>`);
      if (clause.format)   tags.push(`<span class="badge badge-neutral" style="font-size:10px">${clause.format}</span>`);
      if (clause.minimum !== undefined && clause.maximum !== undefined)
        tags.push(`<span class="badge badge-neutral" style="font-size:10px">${clause.minimum}–${clause.maximum}</span>`);
      if (clause.enum)     tags.push(`<span class="badge badge-warn" style="font-size:10px">enum(${clause.enum.length})</span>`);
      return `
        <div class="field-chip">
          <div class="f-name">${name}</div>
          <div class="f-tags">${typeBadge(clause.type || '?')} ${tags.join('')}</div>
        </div>`;
    }).join('');

    const checksHtml = checks.length ? checks.map(ch => `
      <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border-soft)">
        ${checkBadge(ch.type)}
        <span class="text-mono text-small" style="color:var(--info);flex:1">${ch.field}</span>
        ${ch.severity ? statusBadge(ch.severity) : ''}
        ${ch.z_score_warn ? `<span class="text-muted text-small">±${ch.z_score_warn}σ / ±${ch.z_score_fail}σ</span>` : ''}
      </div>
    `).join('') : `<div class="empty" style="padding:20px 0">No quality checks.</div>`;

    panel.innerHTML = `
      <button class="panel-close" onclick="closePanel()">✕ Close</button>
      <div style="margin-top:4px">
        <div class="flex gap-8 items-center" style="margin-bottom:12px">
          <span class="badge badge-green">v${c.version}</span>
          <span class="badge badge-neutral">${c.source?.record_count ?? '—'} records</span>
          <span class="badge badge-neutral">${Object.keys(props).length} fields</span>
        </div>
        <h2 style="font-size:17px;margin-bottom:4px">${c.id}</h2>
        <p class="text-muted text-small" style="margin-bottom:24px">${c.description || ''}</p>

        <h3 style="font-size:13px;margin-bottom:12px">Schema Fields</h3>
        <div class="field-grid">${fieldHtml || '<div class="text-muted text-small">No fields.</div>'}</div>

        <h3 style="font-size:13px;margin-top:24px;margin-bottom:8px">Quality Checks (${checks.length})</h3>
        ${checksHtml}

        <h3 style="font-size:13px;margin-top:20px;margin-bottom:8px">Lineage</h3>
        <div class="text-muted text-small">
          Upstream: ${(c.lineage?.upstream || []).length} source(s) &nbsp;·&nbsp;
          Downstream: ${(c.lineage?.downstream || []).length} consumer(s)
        </div>
      </div>`;
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

    const vHtml = violations.length ? violations.map(v => {
      const ic = violationIcon(v.status);
      return `
        <div class="violation-item">
          <div class="v-icon ${ic.cls}">${ic.icon}</div>
          <div class="v-body">
            <div class="v-title flex gap-8 items-center flex-wrap">
              ${statusBadge(v.status)} ${checkBadge(v.check)}
              <span class="text-mono text-small" style="color:var(--info)">${v.field}</span>
            </div>
            <div class="v-reason">${v.reason}</div>
            ${v.sample ? `<div class="v-sample">Sample: ${JSON.stringify(v.sample)}</div>` : ''}
          </div>
        </div>`;
    }).join('') : `<div style="text-align:center;padding:32px 0;color:var(--pass);font-weight:600">All checks passed — no violations.</div>`;

    panel.innerHTML = `
      <button class="panel-close" onclick="closePanel()">✕ Close</button>
      <div style="margin-top:4px">
        <div class="flex gap-8 items-center" style="margin-bottom:12px">
          ${statusBadge(r.overall_status)}
          <span class="badge badge-neutral text-mono">${r.contract_id}</span>
        </div>
        <h2 style="font-size:16px;margin-bottom:4px">${filename}</h2>
        <p class="text-muted text-small" style="margin-bottom:24px">Validated: ${fmtDate(r.validated_at)}</p>

        <div class="stat-grid" style="grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px">
          ${[
            ['Records', r.record_count, ''],
            ['Critical', r.summary?.CRITICAL ?? 0, r.summary?.CRITICAL ? 'var(--fail)' : ''],
            ['Fail', r.summary?.FAIL ?? 0, r.summary?.FAIL ? 'var(--fail)' : ''],
            ['Warn', r.summary?.WARN ?? 0, r.summary?.WARN ? 'var(--warn)' : ''],
          ].map(([label, val, color]) => `
            <div class="stat-card" style="padding:14px">
              <div class="s-label">${label}</div>
              <div class="s-value" style="font-size:22px;${color ? `color:${color}` : ''}">${val}</div>
            </div>`).join('')}
        </div>

        <h3 style="font-size:13px;margin-bottom:12px">Violations (${violations.length})</h3>
        <div class="violation-list">${vHtml}</div>
      </div>`;
  } catch (e) {
    panel.innerHTML = `<button class="panel-close" onclick="closePanel()">✕ Close</button><div class="empty">Error: ${e.message}</div>`;
  }
}

function closePanel() {
  document.getElementById('detail-panel').classList.remove('open');
}

// ── Render ────────────────────────────────────────────────────────────────
function renderPage() {
  const pages = {
    overview:  renderOverview,
    contracts: renderContractsPage,
    reports:   renderReportsPage,
    baselines: renderBaselinesPage,
    enforcer:  renderEnforcerPage,
  };
  document.getElementById('main-content').innerHTML = (pages[state.page] || renderOverview)();
}

async function refresh() {
  document.getElementById('main-content').innerHTML = `<div class="loading">Loading…</div>`;
  try {
    await loadAll();
    renderPage();
  } catch (e) {
    document.getElementById('main-content').innerHTML = `<div class="empty">Could not load data: ${e.message}</div>`;
  }
}

// ── Boot ──────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.nav-item').forEach(el =>
    el.addEventListener('click', () => navigate(el.dataset.page))
  );
  refresh();
  setInterval(() => loadAll().then(() => renderPage()).catch(() => {}), 10000);
});
