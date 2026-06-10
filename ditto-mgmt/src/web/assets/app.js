// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let autoRefresh = true;
let refreshTimer = null;
let selectedAddr = null;
let nodesData = [];
let doctorData = null;
let confirmationModal = null;
let pendingConfirmation = null;
// Reverse-proxy-aware base path. The console is served at its mount root —
// "/" standalone, or "/api/v1/admin/ditto-mgmt/" behind the TinyMe admin
// proxy — so location.pathname IS the prefix. We build API URLs relative to
// it (never a leading-"/" absolute path) so the same bundle works under any
// mount without the proxy rewriting request paths. A <base href> can't help
// (the console's own CSP sets base-uri 'none') and script-src 'self' forbids
// inline script, so this lives in the external bundle.
const uiBasePath = `${window.location.pathname.replace(/\/?$/, '/')}`;

// Resolve an app-relative path (leading slash optional) against the mount prefix.
function apiUrl(path) {
  return `${uiBasePath}${String(path).replace(/^\/+/, '')}`;
}

// ---------------------------------------------------------------------------
// Auto-refresh
// ---------------------------------------------------------------------------
function toggleAutoRefresh() {
  autoRefresh = !autoRefresh;
  const badge = document.getElementById('auto-refresh-badge');
  if (autoRefresh) {
    badge.className = 'badge bg-secondary refresh-badge';
    badge.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Auto-refresh ON';
    scheduleRefresh();
  } else {
    badge.className = 'badge bg-danger refresh-badge';
    badge.innerHTML = '<i class="bi bi-pause-fill me-1"></i>Auto-refresh OFF';
    clearTimeout(refreshTimer);
  }
}

function scheduleRefresh() {
  clearTimeout(refreshTimer);
  if (autoRefresh) refreshTimer = setTimeout(fetchNodes, 3000);
}

// ---------------------------------------------------------------------------
// Fetch nodes
// ---------------------------------------------------------------------------
async function fetchNodes() {
  try {
    const resp = await fetch(apiUrl('api/nodes'));
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    nodesData = data.nodes || [];
    renderTable(nodesData);
    renderSummary(nodesData);
    renderProductionOverview(nodesData);
    renderProductionTools(nodesData);
    fetchDoctor();
    document.getElementById('last-updated').textContent =
      'Last updated: ' + new Date().toLocaleTimeString();
    if (selectedAddr) refreshDetails(selectedAddr);
  } catch(e) {
    showToast('danger', 'Fetch error: ' + e.message);
  }
  scheduleRefresh();
}

async function fetchDoctor() {
  try {
    const resp = await fetch(apiUrl('api/doctor'));
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    doctorData = await resp.json();
    renderDoctorPanel(doctorData);
  } catch(e) {
    doctorData = null;
    document.getElementById('doctor-panel').innerHTML = `
      <div class="ops-card border-danger">
        <div class="ops-card-title">Doctor unavailable</div>
        <div class="text-danger fw-semibold">${escapeHtml(e.message)}</div>
      </div>`;
  }
}

// ---------------------------------------------------------------------------
// Production overview
// ---------------------------------------------------------------------------
function renderProductionOverview(nodes) {
  const overview = buildProductionOverview(nodes);
  const el = document.getElementById('production-overview');
  el.innerHTML = `
    ${renderOverviewCard('Readiness', overview.readiness.level, overview.readiness.value, overview.readiness.note, overview.readiness.findings)}
    ${renderOverviewCard('Security posture', overview.security.level, overview.security.value, overview.security.note, overview.security.findings)}
    ${renderOverviewCard('Quota pressure', overview.quota.level, overview.quota.value, overview.quota.note, overview.quota.findings)}
    ${renderOverviewCard('Latency/errors', overview.latency.level, overview.latency.value, overview.latency.note, overview.latency.findings)}
    ${renderOverviewCard('Persistence', overview.persistence.level, overview.persistence.value, overview.persistence.note, overview.persistence.findings)}
    ${renderOverviewCard('Operator actions', overview.actions.level, overview.actions.value, overview.actions.note, overview.actions.findings)}
  `;
}

function buildProductionOverview(nodes) {
  if (!nodes.length) {
    const empty = {
      level: 'warn',
      value: 'No nodes',
      note: 'No cluster data is currently available.',
      findings: ['Check management seed configuration and node reachability.'],
    };
    return {
      readiness: empty,
      security: empty,
      quota: empty,
      latency: empty,
      persistence: empty,
      actions: empty,
    };
  }

  const reachable = nodes.filter(n => n.reachable);
  const active = reachable.filter(n => n.status === 'Active');
  const inactive = reachable.filter(n => n.status === 'Inactive');
  const unreachable = nodes.filter(n => !n.reachable);
  const primaryCount = reachable.filter(n => n.is_primary).length;
  const committed = reachable.map(n => n.committed_index).filter(v => Number.isFinite(v));
  const committedSpread = committed.length ? Math.max(...committed) - Math.min(...committed) : 0;
  const quotaTop = nodes.flatMap(n => (n.namespace_quota_top_usage || []).map(q => ({ ...q, node_name: n.node_name || n.addr })));
  const quotaPeak = quotaTop.reduce((max, q) => Math.max(max, Number(q.usage_pct || 0)), 0);
  const quotaRejectRate = sum(nodes, 'namespace_quota_reject_rate_per_min');
  const risingQuota = nodes.some(n => ['rising', 'surging'].includes(String(n.namespace_quota_reject_trend || '').toLowerCase()));
  const p99 = max(nodes, 'client_latency_p99_estimate_ms');
  const clientErrors = sum(nodes, 'client_error_total');
  const clientRequests = sum(nodes, 'client_requests_total');
  const errorPct = clientRequests > 0 ? Math.round(clientErrors * 1000 / clientRequests) / 10 : 0;
  const persistenceEnabled = reachable.filter(n => n.persistence_enabled === true).length;
  const backupEnabled = reachable.filter(n => n.persistence_backup_enabled === true).length;
  const restoreFailures = sum(nodes, 'snapshot_restore_failure_total') + sum(nodes, 'snapshot_restore_policy_block_total');
  const degradedNodes = [...unreachable, ...inactive];

  const readinessFindings = [];
  if (unreachable.length) readinessFindings.push(`${unreachable.length} unreachable node(s)`);
  if (!active.length) readinessFindings.push('No active nodes');
  if (primaryCount !== 1) readinessFindings.push(`Primary count is ${primaryCount}`);
  if (committedSpread > 0) readinessFindings.push(`Committed index spread is ${committedSpread}`);
  const readinessLevel = readinessFindings.some(f => f.includes('unreachable') || f.includes('No active') || f.includes('Primary')) ? 'crit' : committedSpread > 0 ? 'warn' : 'ok';

  const securityFindings = [];
  const httpAuthKnown = reachable.length > 0;
  if (httpAuthKnown) securityFindings.push('Management and node HTTP auth are required for this view');
  if (degradedNodes.length) securityFindings.push('Degraded membership affects production readiness');

  const quotaFindings = [];
  if (quotaPeak >= 90) quotaFindings.push(`Top namespace usage is ${quotaPeak}%`);
  if (quotaRejectRate >= 10) quotaFindings.push(`${quotaRejectRate} quota rejects/min`);
  if (risingQuota) quotaFindings.push('Quota reject trend is rising');
  const quotaLevel = quotaPeak >= 90 || quotaRejectRate >= 10 || risingQuota ? 'warn' : 'ok';

  const latencyFindings = [];
  if (p99 >= 500) latencyFindings.push(`p99 latency estimate is ${p99}ms`);
  if (errorPct >= 1) latencyFindings.push(`Client error rate is ${errorPct}%`);
  const latencyLevel = p99 >= 500 || errorPct >= 1 ? 'warn' : 'ok';

  const persistenceFindings = [];
  if (persistenceEnabled !== reachable.length) persistenceFindings.push(`${reachable.length - persistenceEnabled} reachable node(s) have persistence disabled`);
  if (backupEnabled !== reachable.length) persistenceFindings.push(`${reachable.length - backupEnabled} reachable node(s) have backup disabled`);
  if (restoreFailures > 0) persistenceFindings.push(`${restoreFailures} restore failure/block event(s)`);
  const persistenceLevel = restoreFailures > 0 ? 'warn' : 'ok';

  const actions = [
    ...readinessFindings,
    ...quotaFindings,
    ...latencyFindings,
    ...persistenceFindings,
  ].slice(0, 4);

  return {
    readiness: {
      level: readinessLevel,
      value: readinessLevel === 'ok' ? 'Ready' : readinessLevel === 'crit' ? 'Critical' : 'Warning',
      note: `${active.length}/${nodes.length} active, ${primaryCount} primary, spread ${committedSpread}`,
      findings: readinessFindings.length ? readinessFindings : ['Cluster membership and primary election look healthy.'],
    },
    security: {
      level: degradedNodes.length ? 'warn' : 'ok',
      value: degradedNodes.length ? 'Review' : 'Guarded',
      note: 'Full strict-security verdict will move to the planned doctor API.',
      findings: securityFindings.length ? securityFindings : ['No degraded membership detected from node status.'],
    },
    quota: {
      level: quotaLevel,
      value: quotaLevel === 'ok' ? `${quotaPeak}% peak` : 'Pressure',
      note: `${quotaRejectRate} rejects/min across reachable nodes`,
      findings: quotaFindings.length ? quotaFindings : ['No namespace quota pressure detected.'],
    },
    latency: {
      level: latencyLevel,
      value: p99 ? `p99 ${p99}ms` : 'No samples',
      note: `${clientErrors}/${clientRequests} client errors (${errorPct}%)`,
      findings: latencyFindings.length ? latencyFindings : ['Client latency and error counters look normal.'],
    },
    persistence: {
      level: persistenceLevel,
      value: `${persistenceEnabled}/${reachable.length} enabled`,
      note: `${backupEnabled}/${reachable.length} backup-enabled nodes`,
      findings: persistenceFindings.length ? persistenceFindings : ['Persistence and restore counters look normal.'],
    },
    actions: {
      level: actions.length ? (readinessLevel === 'crit' ? 'crit' : 'warn') : 'ok',
      value: actions.length ? `${actions.length} item(s)` : 'None',
      note: 'Highest-priority operator follow-up from current telemetry.',
      findings: actions.length ? actions : ['No immediate operator action suggested.'],
    },
  };
}

function renderOverviewCard(label, level, value, note, findings) {
  const icon = level === 'crit' ? 'x-octagon-fill' : level === 'warn' ? 'exclamation-triangle-fill' : 'check-circle-fill';
  const textClass = level === 'crit' ? 'text-danger' : level === 'warn' ? 'text-warning' : 'text-success';
  return `
    <section class="overview-card ${level}">
      <div class="overview-label">${escapeHtml(label)}</div>
      <div class="overview-value ${textClass}"><i class="bi bi-${icon} me-2"></i>${escapeHtml(value)}</div>
      <div class="overview-note">${escapeHtml(note)}</div>
      <ul class="finding-list">${findings.map(item => `<li>${escapeHtml(item)}</li>`).join('')}</ul>
    </section>`;
}

// ---------------------------------------------------------------------------
// Production tools
// ---------------------------------------------------------------------------
function renderProductionTools(nodes) {
  renderCacheTargetOptions(nodes);
  renderNamespacePanel(nodes);
  renderBackupPanel(nodes);
  renderObservabilityPanel(nodes);
  renderTopologyPanel(nodes);
  if (doctorData) renderDoctorPanel(doctorData);
}

function renderDoctorPanel(data) {
  const level = severityToLevel(data.verdict);
  const summary = data.summary || {};
  const findings = data.findings || [];
  document.getElementById('doctor-panel').innerHTML = `
    <div class="ops-card ${level}">
      <div class="ops-card-title">Verdict</div>
      <div class="overview-value ${levelTextClass(level)}">${escapeHtml(String(data.verdict || 'unknown').toUpperCase())}</div>
      <div class="overview-note">${summary.active_nodes ?? 0}/${summary.total_nodes ?? 0} active, ${summary.primary_count ?? 0} primary, spread ${summary.committed_index_spread ?? 0}</div>
    </div>
    <div class="ops-card">
      <div class="ops-card-title">Pressure</div>
      <div class="detail-value">Quota peak ${summary.quota_peak_usage_pct ?? 0}%</div>
      <div class="overview-note">${summary.quota_reject_rate_per_min ?? 0} quota rejects/min, p99 ${summary.p99_latency_ms ?? 0}ms, errors ${summary.client_error_rate_pct ?? 0}%</div>
    </div>
    <div class="ops-card findings-card">
      <div class="ops-card-title">Findings</div>
      ${renderFindingsTable(findings)}
    </div>`;
}

function renderFindingsTable(findings) {
  if (!findings.length) return '<div class="text-secondary small">No findings.</div>';
  return `
    <div class="table-responsive findings-scroll">
      <table class="table table-borderless compact-table mb-0">
        <thead class="text-secondary"><tr><th>Severity</th><th>Scope</th><th>Finding</th><th>Action</th></tr></thead>
        <tbody>${findings.map(f => `
          <tr>
            <td>${severityBadge(f.severity)}</td>
            <td>${escapeHtml(f.scope)}</td>
            <td>${escapeHtml(f.message)}</td>
            <td>${escapeHtml(f.action)}</td>
          </tr>`).join('')}</tbody>
      </table>
    </div>`;
}

function renderNamespacePanel(nodes) {
  const rows = nodes.flatMap(n => (n.namespace_quota_top_usage || []).map(q => ({
    node: n.node_name || n.addr,
    namespace: q.namespace,
    key_count: q.key_count,
    quota_limit: q.quota_limit,
    usage_pct: q.usage_pct,
    remaining_keys: q.remaining_keys,
    trend: n.namespace_quota_reject_trend || 'steady',
    reject_rate: n.namespace_quota_reject_rate_per_min || 0,
  }))).sort((a, b) => b.usage_pct - a.usage_pct);

  document.getElementById('namespace-panel').innerHTML = `
    <div class="ops-grid mb-2">
      <div class="ops-card">
        <div class="ops-card-title">Quota pressure</div>
        <div class="detail-value">${rows[0]?.usage_pct ?? 0}% peak namespace usage</div>
        <div class="overview-note">${sum(nodes, 'namespace_quota_reject_rate_per_min')} rejects/min across nodes</div>
      </div>
      <div class="ops-card">
        <div class="ops-card-title">Tenancy</div>
        <div class="detail-value">${nodes.filter(n => n.tenancy_enabled).length}/${nodes.filter(n => n.reachable).length} reachable enabled</div>
        <div class="overview-note">Default namespaces: ${escapeHtml(unique(nodes.map(n => n.tenancy_default_namespace).filter(Boolean)).join(', ') || '-')}</div>
      </div>
    </div>
    ${rows.length ? `
      <div class="table-responsive">
        <table class="table table-borderless table-hover compact-table mb-0">
          <thead class="text-secondary"><tr><th>Node</th><th>Namespace</th><th>Keys</th><th>Quota</th><th>Usage</th><th>Remaining</th><th>Rejects/min</th><th>Trend</th></tr></thead>
          <tbody>${rows.map(r => `
            <tr>
              <td>${escapeHtml(r.node)}</td>
              <td>${escapeHtml(r.namespace)}</td>
              <td>${r.key_count}</td>
              <td>${r.quota_limit || 'unlimited'}</td>
              <td>${quotaBadge(r.usage_pct)}</td>
              <td>${r.remaining_keys}</td>
              <td>${r.reject_rate}</td>
              <td>${escapeHtml(r.trend)}</td>
            </tr>`).join('')}</tbody>
        </table>
      </div>` : '<div class="text-secondary small">No namespace quota usage reported.</div>'}`;
}

function renderCacheTargetOptions(nodes) {
  const select = document.getElementById('cache-target');
  const previous = select.value;
  const options = ['<option value="all">all nodes</option>']
    .concat(nodes.filter(n => n.reachable).map(n => {
      const value = n.node_name || n.addr;
      return `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`;
    }));
  select.innerHTML = options.join('');
  if ([...select.options].some(o => o.value === previous)) select.value = previous;
}

function renderBackupPanel(nodes) {
  const rows = nodes.filter(n => n.reachable);
  document.getElementById('backup-panel').innerHTML = `
    <div class="table-responsive">
      <table class="table table-borderless table-hover compact-table mb-0">
        <thead class="text-secondary"><tr><th>Node</th><th>Status</th><th>Backup</th><th>Storage</th><th>Last load</th><th>Restore</th><th>Actions</th></tr></thead>
        <tbody>${rows.map(n => {
          const target = escapeHtml(n.node_name || n.addr);
          const failures = Number(n.snapshot_restore_failure_total || 0) + Number(n.snapshot_restore_policy_block_total || 0);
          return `
            <tr>
              <td>${escapeHtml(n.node_name || n.addr)}</td>
              <td>${escapeHtml(n.status || '-')}</td>
              <td>${boolBadge(n.persistence_backup_enabled)}</td>
              <td>${fmtBytes(n.backup_dir_bytes)}</td>
              <td>${escapeHtml(n.snapshot_last_load_path || '-')}${n.snapshot_last_load_age_secs != null ? ` (${fmtUptime(n.snapshot_last_load_age_secs)} ago)` : ''}</td>
              <td>${n.snapshot_restore_success_total || 0} ok / ${failures} issue(s)</td>
              <td>
                <button class="btn btn-sm btn-outline-secondary me-1" data-action="backup" data-node-name="${target}"><i class="bi bi-archive"></i></button>
                <button class="btn btn-sm btn-outline-warning" data-node-op="restore" data-node-name="${target}"><i class="bi bi-arrow-counterclockwise"></i></button>
              </td>
            </tr>`;
        }).join('')}</tbody>
      </table>
    </div>`;
}

function renderObservabilityPanel(nodes) {
  document.getElementById('observability-panel').innerHTML = `
    <div class="ops-grid">
      ${nodes.filter(n => n.reachable).map(n => `
        <div class="ops-card">
          <div class="ops-card-title">${escapeHtml(n.node_name || n.addr)}</div>
          <div class="detail-value">p99 ${n.client_latency_p99_estimate_ms ?? 0}ms</div>
          <div class="overview-note">requests tcp:${n.client_requests_tcp_total ?? 0} http:${n.client_requests_http_total ?? 0} internal:${n.client_requests_internal_total ?? 0}</div>
          <div class="overview-note">errors auth:${n.client_error_auth_total ?? 0} throttle:${n.client_error_throttle_total ?? 0} availability:${n.client_error_availability_total ?? 0} validation:${n.client_error_validation_total ?? 0}</div>
          <div class="overview-note">hot-key coalesced:${n.hot_key_coalesced_hits_total ?? 0} fallback:${n.hot_key_fallback_exec_total ?? 0} timeout:${n.hot_key_wait_timeout_total ?? 0} stale:${n.hot_key_stale_served_total ?? 0}</div>
          <div class="overview-note">circuit ${escapeHtml(n.circuit_breaker_state || '-')} open:${n.circuit_breaker_open_total ?? 0} reject:${n.circuit_breaker_reject_total ?? 0}</div>
        </div>`).join('')}
    </div>`;
}

function renderTopologyPanel(nodes) {
  const committed = nodes.filter(n => n.committed_index != null).map(n => n.committed_index);
  const spread = committed.length ? Math.max(...committed) - Math.min(...committed) : 0;
  document.getElementById('topology-panel').innerHTML = `
    <div class="ops-grid">
      <div class="ops-card">
        <div class="ops-card-title">Cluster spread</div>
        <div class="detail-value">${spread}</div>
        <div class="overview-note">Committed index difference across reachable reported nodes.</div>
      </div>
      ${nodes.map(n => `
        <div class="ops-card ${!n.reachable ? 'crit' : n.status === 'Active' ? 'ok' : 'warn'}">
          <div class="ops-card-title">${escapeHtml(n.node_name || n.addr)}</div>
          <div class="detail-value">${n.is_primary ? '<i class="bi bi-star-fill text-primary me-1"></i>' : ''}${escapeHtml(n.status || 'Unreachable')}</div>
          <div class="overview-note">${escapeHtml(n.addr)} - committed ${n.committed_index ?? '-'}</div>
          <div class="overview-note">heartbeat ${n.heartbeat_ms ?? '-'}ms - uptime ${n.uptime_secs != null ? fmtUptime(n.uptime_secs) : '-'}</div>
        </div>`).join('')}
    </div>`;
}

// ---------------------------------------------------------------------------
// Render cluster summary pills
// ---------------------------------------------------------------------------
function renderSummary(nodes) {
  const total    = nodes.length;
  const active   = nodes.filter(n => n.reachable && n.status === 'Active').length;
  const inactive = nodes.filter(n => n.reachable && n.status === 'Inactive').length;
  const unreach  = nodes.filter(n => !n.reachable).length;
  const primary  = nodes.find(n => n.is_primary);

  const el = document.getElementById('cluster-summary');
  el.innerHTML = `
    <span class="badge bg-secondary"><i class="bi bi-server me-1"></i>${total} nodes</span>
    <span class="badge badge-success-custom"><i class="bi bi-check-circle me-1"></i>${active} active</span>
    ${inactive ? `<span class="badge badge-warning-custom"><i class="bi bi-pause-circle me-1"></i>${inactive} inactive</span>` : ''}
    ${unreach  ? `<span class="badge bg-danger"><i class="bi bi-exclamation-triangle me-1"></i>${unreach} unreachable</span>` : ''}
    ${primary  ? `<span class="badge badge-primary-gradient"><i class="bi bi-star-fill me-1"></i>Primary: ${escapeHtml(primary.node_name || primary.addr)}</span>` : ''}
  `;
}

// ---------------------------------------------------------------------------
// Render table
// ---------------------------------------------------------------------------
function renderTable(nodes) {
  const tbody = document.getElementById('nodes-tbody');
  if (!nodes.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="text-center text-secondary py-4">No nodes discovered.</td></tr>';
    return;
  }

  tbody.innerHTML = nodes.map(n => {
    const isSelected = n.addr === selectedAddr;

    // Status indicator
    let statusHtml;
    if (!n.reachable) {
      statusHtml = `<span class="status-unreachable"><i class="bi bi-exclamation-triangle-fill"></i></span><span class="text-danger small">Unreachable</span>`;
    } else if (n.status === 'Active') {
      statusHtml = `<span class="status-dot status-active"></span><span class="text-success small">Active</span>`;
    } else if (n.status === 'Inactive') {
      statusHtml = `<span class="status-dot status-inactive"></span><span class="text-warning small">Inactive</span>`;
    } else {
      statusHtml = `<span class="status-dot status-offline"></span><span class="text-secondary small">${escapeHtml(n.status || '?')}</span>`;
    }

    // Primary badge
    const primaryHtml = n.is_primary
      ? '<span class="badge badge-primary"><i class="bi bi-star-fill me-1"></i>PRIMARY</span>'
      : '<span class="text-muted">-</span>';

    // Memory bar
    let memHtml = '<span class="text-muted">-</span>';
    if (n.memory_used_bytes != null && n.memory_max_bytes != null && n.memory_max_bytes > 0) {
      const pct = Math.round(n.memory_used_bytes * 100 / n.memory_max_bytes);
      const cls = pct >= 90 ? 'crit' : pct >= 70 ? 'warn' : '';
      memHtml = `
        <span class="small">${fmtBytes(n.memory_used_bytes)} / ${fmtBytes(n.memory_max_bytes)}</span>
        <progress class="memory-progress ${cls}" value="${pct}" max="100" aria-label="Memory usage ${pct}%"></progress>`;
    }

    // Heartbeat
    let hbHtml = '<span class="text-muted">-</span>';
    if (n.heartbeat_ms != null) {
      const cls = n.heartbeat_ms < 10 ? 'heartbeat-good' : n.heartbeat_ms < 50 ? 'heartbeat-warn' : 'heartbeat-bad';
      hbHtml = `<span class="${cls} small fw-semibold">${n.heartbeat_ms}ms</span>`;
    }

    // Uptime
    const uptimeHtml = n.uptime_secs != null
      ? `<span class="uptime-col small">${fmtUptime(n.uptime_secs)}</span>`
      : '<span class="text-muted">-</span>';


    // Actions
    let actionsHtml = '';
    if (n.reachable) {
      const isActive = n.status === 'Active';
      const btnVariant = isActive ? 'outline-warning' : 'outline-success';
      const btnLabel   = isActive ? 'Deactivate' : 'Activate';
      actionsHtml = `
        <button class="btn ${btnVariant} action-btn me-1" data-action="set-active" data-node-name="${escapeHtml(n.node_name)}" data-active="${!isActive}">
          ${btnLabel}
        </button>
        <button class="btn btn-outline-secondary action-btn" data-action="backup" data-node-name="${escapeHtml(n.node_name)}">
          <i class="bi bi-archive me-1"></i>Backup
        </button>`;
    } else {
      actionsHtml = '<span class="text-muted small">(unavailable)</span>';
    }

    return `
      <tr class="node-row ${isSelected ? 'selected' : ''}" data-select-addr="${escapeHtml(n.addr)}">
        <td><div class="d-flex align-items-center">${statusHtml}</div></td>
        <td>
          <div class="fw-semibold">${escapeHtml(n.node_name || '-')}</div>
          <div class="text-muted addr-subtitle">${escapeHtml(n.addr)}</div>
        </td>
        <td>${primaryHtml}</td>
        <td><span class="font-monospace small">${n.key_count ?? '-'}</span></td>
        <td><span class="font-monospace small">${n.committed_index ?? '-'}</span></td>
        <td>${memHtml}</td>
        <td>${hbHtml}</td>
        <td>${uptimeHtml}</td>
        <td>${actionsHtml}</td>
      </tr>`;
  }).join('');
}

// ---------------------------------------------------------------------------
// Details panel
// ---------------------------------------------------------------------------
function selectNode(addr) {
  if (selectedAddr === addr) {
    closeDetails();
    return;
  }
  selectedAddr = addr;
  document.querySelectorAll('.node-row').forEach(r => r.classList.remove('selected'));
  refreshDetails(addr);
}

function refreshDetails(addr) {
  const node = nodesData.find(n => n.addr === addr);
  if (!node) return;

  document.getElementById('details-title').textContent = node.node_name || addr;
  document.getElementById('details-panel').classList.remove('d-none');

  const fields = [
    ['Node ID',        node.node_id],
    ['Address',        node.addr],
    ['Status',         node.status],
    ['Primary',        node.is_primary != null ? (node.is_primary ? 'Yes *' : 'No') : null],
    ['Committed Index',node.committed_index],
    ['Memory Used',    node.memory_used_bytes != null ? fmtBytes(node.memory_used_bytes) : null],
    ['Memory Max',     node.memory_max_bytes  != null ? fmtBytes(node.memory_max_bytes)  : null],
    ['Heartbeat',      node.heartbeat_ms != null ? node.heartbeat_ms + 'ms' : null],
    ['Uptime',         node.uptime_secs  != null ? fmtUptime(node.uptime_secs) : null],
    ['Backup Storage', node.backup_dir_bytes != null ? fmtBytes(node.backup_dir_bytes) : null],
    ['Keys',           node.key_count],
    ['Evictions',      node.evictions],
    ['Hits',           node.hit_count],
    ['Misses',         node.miss_count],
  ];

  document.getElementById('details-grid').innerHTML = fields
    .filter(([, v]) => v != null)
    .map(([label, value]) => `
      <div class="detail-card">
        <div class="detail-label">${escapeHtml(label)}</div>
        <div class="detail-value">${escapeHtml(value)}</div>
      </div>`)
    .join('');
}

function closeDetails() {
  selectedAddr = null;
  document.getElementById('details-panel').classList.add('d-none');
  document.querySelectorAll('.node-row').forEach(r => r.classList.remove('selected'));
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------
async function setActive(addr, active) {
  try {
    const resp = await fetch(apiUrl(`api/nodes/${addr}/set-active`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ active }),
    });
    const data = await resp.json();
    const ok = Array.isArray(data) ? data.every(r => r.ok) : data.ok;
    showToast(ok ? 'success' : 'warning',
      `${addr}: ${active ? 'Activated' : 'Deactivated'}${ok ? '' : ' (check server logs)'}`);
    await fetchNodes();
  } catch(e) {
    showToast('danger', 'Error: ' + e.message);
  }
}

async function triggerBackup(addr) {
  try {
    const resp = await fetch(apiUrl(`api/nodes/${addr}/backup`), { method: 'POST' });
    const data = await resp.json();
    const result = Array.isArray(data) ? data[0] : data;
    if (result && result.ok) {
      showToast('success', `Backup written: ${result.path}`);
    } else {
      showToast('warning', `Backup: ${result?.error || 'unknown error'}`);
    }
  } catch(e) {
    showToast('danger', 'Error: ' + e.message);
  }
}

async function restoreSnapshot(addr) {
  const confirmed = await requestConfirmation({
    title: 'Restore latest snapshot',
    message: 'This can replace the node cache contents with the latest local snapshot.',
    phrase: addr,
    fields: [
      ['Target node', addr],
      ['Operation', 'restore latest snapshot'],
    ],
  });
  if (!confirmed) return;
  try {
    const resp = await fetch(apiUrl(`api/nodes/${encodeURIComponent(addr)}/restore-snapshot`), { method: 'POST' });
    const data = await resp.json();
    const result = Array.isArray(data) ? data[0] : data;
    if (result && result.ok) {
      showToast('success', `Restored ${result.entries} entries from ${result.path}`);
    } else {
      showToast('warning', `Restore: ${result?.error || 'unknown error'}`);
    }
    await fetchNodes();
  } catch(e) {
    showToast('danger', 'Error: ' + e.message);
  }
}

async function runCacheAction(action) {
  const target = document.getElementById('cache-target').value || 'all';
  const namespace = document.getElementById('cache-namespace').value.trim();
  const pattern = document.getElementById('cache-pattern').value.trim();
  const key = document.getElementById('cache-key').value.trim();
  const value = document.getElementById('cache-value').value;
  const ttlRaw = document.getElementById('cache-ttl').value.trim();
  const compressed = document.getElementById('cache-compressed').value === 'true';
  const reveal = document.getElementById('cache-reveal').checked;
  const nsQuery = namespace ? `?namespace=${encodeURIComponent(namespace)}` : '';
  const nsJoin = namespace ? `&namespace=${encodeURIComponent(namespace)}` : '';

  try {
    let resp;
    if (action === 'list') {
      const query = `?pattern=${encodeURIComponent(pattern || '*')}${nsJoin}`;
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/keys${query}`));
    } else if (action === 'get') {
      requireKey(key);
      const query = `?reveal=${reveal ? 'true' : 'false'}${namespace ? `&namespace=${encodeURIComponent(namespace)}` : ''}`;
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/keys/${encodeURIComponent(key)}${query}`));
    } else if (action === 'set') {
      requireKey(key);
      const ttl = ttlRaw === '' ? null : Number(ttlRaw);
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/keys/${encodeURIComponent(key)}${nsQuery}`), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value, ttl_secs: ttl }),
      });
    } else if (action === 'delete') {
      requireKey(key);
      const confirmed = await requestConfirmation({
        title: 'Delete cache key',
        message: 'This removes one key from the selected target.',
        phrase: key,
        fields: [
          ['Target', target],
          ['Namespace', namespace || '(default)'],
          ['Key', key],
        ],
      });
      if (!confirmed) return;
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/keys/${encodeURIComponent(key)}${nsQuery}`), { method: 'DELETE' });
    } else if (action === 'ttl') {
      if (!pattern) throw new Error('Pattern is required for TTL updates.');
      const ttlDescription = ttlRaw === '' ? 'clear TTL' : `${Number(ttlRaw)} seconds`;
      const confirmed = await requestConfirmation({
        title: 'Update TTL by pattern',
        message: 'This applies a TTL change to every key that matches the pattern.',
        phrase: pattern,
        fields: [
          ['Target', target],
          ['Namespace', namespace || '(default)'],
          ['Pattern', pattern],
          ['TTL', ttlDescription],
        ],
      });
      if (!confirmed) return;
      const ttl = ttlRaw === '' ? null : Number(ttlRaw);
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/ttl${nsQuery}`), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pattern, ttl_secs: ttl }),
      });
    } else if (action === 'compressed') {
      requireKey(key);
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/keys/${encodeURIComponent(key)}/compressed${nsQuery}`), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ compressed }),
      });
    } else if (action === 'flush') {
      const phrase = namespace || target;
      const confirmed = await requestConfirmation({
        title: 'Flush cache target',
        message: namespace
          ? 'This deletes every key in the selected namespace on the selected target.'
          : 'This clears every key on the selected target.',
        phrase,
        fields: [
          ['Target', target],
          ['Namespace', namespace || '(all namespaces)'],
          ['Operation', 'flush cache'],
        ],
      });
      if (!confirmed) return;
      resp = await fetch(apiUrl(`api/cache/${encodeURIComponent(target)}/flush${nsQuery}`), { method: 'POST' });
    }

    const text = await resp.text();
    const parsed = parseJson(text);
    document.getElementById('cache-result').textContent = JSON.stringify(parsed, null, 2);
    showToast(resp.ok ? 'success' : 'warning', `${action} completed with HTTP ${resp.status}`);
    await fetchNodes();
  } catch(e) {
    document.getElementById('cache-result').textContent = e.message;
    showToast('danger', e.message);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeForSingleQuote(value) {
  return String(value ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n')
    .replace(/</g, '\\x3C');
}

function fmtBytes(b) {
  if (b == null) return '-';
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(2) + ' GB';
}

function fmtUptime(secs) {
  if (secs == null) return '-';
  if (secs < 60)   return secs + 's';
  if (secs < 3600) return Math.floor(secs/60) + 'm ' + (secs%60) + 's';
  const h = Math.floor(secs/3600);
  const m = Math.floor((secs%3600)/60);
  const s = secs % 60;
  return `${h}h ${m}m ${s}s`;
}

function sum(items, field) {
  return items.reduce((total, item) => total + Number(item[field] || 0), 0);
}

function max(items, field) {
  return items.reduce((highest, item) => Math.max(highest, Number(item[field] || 0)), 0);
}

function unique(values) {
  return [...new Set(values)];
}

function requireKey(key) {
  if (!key) throw new Error('Key is required.');
}

function parseJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function requestConfirmation({ title, message, phrase, fields }) {
  const modalEl = document.getElementById('confirmation-modal');
  const input = document.getElementById('confirmation-input');
  const approve = document.getElementById('confirmation-approve');
  document.getElementById('confirmation-title').textContent = title;
  document.getElementById('confirmation-message').textContent = message;
  document.getElementById('confirmation-summary').innerHTML = fields
    .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd>`)
    .join('');
  document.getElementById('confirmation-label').textContent = `Type "${phrase}" to confirm.`;
  input.value = '';
  approve.disabled = true;

  if (!confirmationModal) {
    confirmationModal = new bootstrap.Modal(modalEl);
  }
  if (pendingConfirmation) {
    pendingConfirmation(false);
  }

  return new Promise(resolve => {
    const cleanup = result => {
      modalEl.removeEventListener('hidden.bs.modal', onHidden);
      approve.removeEventListener('click', onApprove);
      input.removeEventListener('input', onInput);
      if (pendingConfirmation === cleanup) {
        pendingConfirmation = null;
      }
      resolve(result);
    };
    const onInput = () => {
      approve.disabled = input.value !== phrase;
    };
    const onApprove = () => {
      confirmationModal.hide();
      cleanup(true);
    };
    const onHidden = () => cleanup(false);

    input.addEventListener('input', onInput);
    approve.addEventListener('click', onApprove);
    modalEl.addEventListener('hidden.bs.modal', onHidden);
    pendingConfirmation = cleanup;
    confirmationModal.show();
    setTimeout(() => input.focus(), 150);
  });
}

function severityToLevel(severity) {
  const normalized = String(severity || '').toLowerCase();
  if (normalized === 'critical') return 'crit';
  if (normalized === 'warning') return 'warn';
  return 'ok';
}

function levelTextClass(level) {
  if (level === 'crit') return 'text-danger';
  if (level === 'warn') return 'text-warning';
  return 'text-success';
}

function severityBadge(severity) {
  const level = severityToLevel(severity);
  const cls = level === 'crit' ? 'bg-danger' : level === 'warn' ? 'bg-warning text-dark' : 'bg-success';
  return `<span class="badge ${cls}">${escapeHtml(severity || 'ok')}</span>`;
}

function boolBadge(value) {
  if (value === true) return '<span class="badge bg-success">enabled</span>';
  if (value === false) return '<span class="badge bg-secondary">disabled</span>';
  return '<span class="badge bg-secondary">unknown</span>';
}

function quotaBadge(value) {
  const pct = Number(value || 0);
  const cls = pct >= 90 ? 'bg-danger' : pct >= 80 ? 'bg-warning text-dark' : 'bg-success';
  return `<span class="badge ${cls}">${pct}%</span>`;
}

function showToast(type, message) {
  const id = 'toast-' + Date.now();
  const icons = { success: 'check-circle-fill', danger: 'x-circle-fill', warning: 'exclamation-triangle-fill', info: 'info-circle-fill' };
  const icon = icons[type] || 'info-circle-fill';
  const el = document.createElement('div');
  el.id = id;
  el.className = `toast align-items-center text-bg-${type} border-0`;
  el.setAttribute('role', 'alert');
  el.innerHTML = `
    <div class="d-flex">
      <div class="toast-body"><i class="bi bi-${icon} me-2"></i>${escapeHtml(message)}</div>
      <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
    </div>`;
  document.getElementById('toast-container').appendChild(el);
  const toast = new bootstrap.Toast(el, { delay: 4000 });
  toast.show();
  el.addEventListener('hidden.bs.toast', () => el.remove());
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
document.getElementById('auto-refresh-badge').addEventListener('click', toggleAutoRefresh);
document.getElementById('refresh-now').addEventListener('click', fetchNodes);
document.getElementById('close-details').addEventListener('click', closeDetails);
document.getElementById('nodes-tbody').addEventListener('click', event => {
  const action = event.target.closest('[data-action]');
  if (action) {
    event.stopPropagation();
    const nodeName = action.dataset.nodeName || '';
    if (action.dataset.action === 'set-active') {
      setActive(nodeName, action.dataset.active === 'true');
    } else if (action.dataset.action === 'backup') {
      triggerBackup(nodeName);
    }
    return;
  }

  const row = event.target.closest('[data-select-addr]');
  if (row) {
    selectNode(row.dataset.selectAddr);
  }
});
document.querySelector('.ops-panel').addEventListener('click', event => {
  const cacheAction = event.target.closest('[data-cache-action]');
  if (cacheAction) {
    runCacheAction(cacheAction.dataset.cacheAction);
    return;
  }

  const nodeOp = event.target.closest('[data-node-op]');
  if (nodeOp && nodeOp.dataset.nodeOp === 'restore') {
    restoreSnapshot(nodeOp.dataset.nodeName || '');
    return;
  }

  const nodeAction = event.target.closest('[data-action]');
  if (nodeAction && nodeAction.dataset.action === 'backup') {
    triggerBackup(nodeAction.dataset.nodeName || '');
  }
});

fetchNodes();
