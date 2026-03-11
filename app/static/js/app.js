/* ─────────────────────────────────────────
   NEM BESS SCADA Explorer — frontend logic
   ───────────────────────────────────────── */

const API = '';   // same origin

/* ── State ── */
let bessList = {};
let qualityFlags = {};
let currentData = null;
let currentDuid = null;
let currentDate = null;

/* ── DOM refs ── */
const selState   = document.getElementById('sel-state');
const selBess    = document.getElementById('sel-bess');
const inpDate    = document.getElementById('inp-date');
const btnLoad    = document.getElementById('btn-load');
const errorBox   = document.getElementById('error-box');
const errorMsg   = document.getElementById('error-msg');
const loadingBox = document.getElementById('loading-box');
const resultsBox = document.getElementById('results-box');
const statsGrid  = document.getElementById('stats-grid');
const qualStrip  = document.getElementById('quality-strip');
const tableBody  = document.getElementById('table-body');
const tableNote  = document.getElementById('table-note');
const btnCsv     = document.getElementById('btn-csv');
const btnParquet = document.getElementById('btn-parquet');

/* ── Boot ── */
async function init() {
  // Set default date to yesterday
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  inpDate.value = yesterday.toISOString().slice(0, 10);
  inpDate.max   = yesterday.toISOString().slice(0, 10);

  // Fetch app info for min date
  try {
    const info = await fetch(`${API}/api/info`).then(r => r.json());
    inpDate.min = info.data_start_date;
  } catch (_) {}

  // Load BESS list and quality flags in parallel
  try {
    [bessList, qualityFlags] = await Promise.all([
      fetch(`${API}/api/bess`).then(r => r.json()),
      fetch(`${API}/api/quality-flags`).then(r => r.json()),
    ]);
  } catch (e) {
    showError('Failed to load BESS list. Please refresh the page.');
    return;
  }

  // Populate state selector
  Object.keys(bessList).sort().forEach(state => {
    const opt = document.createElement('option');
    opt.value = state;
    opt.textContent = state;
    selState.appendChild(opt);
  });

  selState.disabled = false;
  selState.addEventListener('change', onStateChange);
  btnLoad.addEventListener('click', onLoad);
  selBess.addEventListener('change', () => { btnLoad.disabled = !selBess.value; });
}

function onStateChange() {
  const state = selState.value;
  selBess.innerHTML = '<option value="">— Select BESS —</option>';
  selBess.disabled  = !state;
  btnLoad.disabled  = true;

  if (!state || !bessList[state]) return;

  bessList[state].forEach(b => {
    const opt = document.createElement('option');
    opt.value = b.duid;
    opt.textContent = `${b.name} (${b.capacity_mw} MW)`;
    selBess.appendChild(opt);
  });
}

/* ── Load data ── */
async function onLoad() {
  const duid = selBess.value;
  const date = inpDate.value;

  if (!duid || !date) return;

  hideError();
  hideResults();
  showLoading(true);
  btnLoad.disabled = true;

  try {
    const resp = await fetch(`${API}/api/data?duid=${encodeURIComponent(duid)}&date=${date}`);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(err.detail || `Server error ${resp.status}`);
    }
    const payload = await resp.json();
    currentData  = payload;
    currentDuid  = duid;
    currentDate  = date;
    renderResults(payload, duid, date);
  } catch (e) {
    showError(e.message);
  } finally {
    showLoading(false);
    btnLoad.disabled = false;
  }
}

/* ── Render ── */
function renderResults(payload, duid, date) {
  const { summary, data } = payload;

  renderStats(summary, payload.total_rows);
  renderQualityStrip(summary.flag_breakdown);
  renderChart(data);
  renderTable(data, payload.total_rows);

  const base = `${API}/api/download`;
  const params = `duid=${encodeURIComponent(duid)}&date=${date}`;
  btnCsv.href     = `${base}/csv?${params}`;
  btnParquet.href = `${base}/parquet?${params}`;

  resultsBox.classList.remove('hidden');
}

function renderStats(summary, totalRows) {
  statsGrid.innerHTML = '';
  const stats = [
    { label: 'Total Readings', value: totalRows.toLocaleString() },
    { label: 'Min MW',  value: summary.min_mw  != null ? summary.min_mw.toFixed(2)  : '—' },
    { label: 'Max MW',  value: summary.max_mw  != null ? summary.max_mw.toFixed(2)  : '—' },
    { label: 'Mean MW', value: summary.mean_mw != null ? summary.mean_mw.toFixed(2) : '—' },
    { label: 'Std Dev', value: summary.std_mw  != null ? summary.std_mw.toFixed(2)  : '—' },
  ];
  stats.forEach(s => {
    statsGrid.insertAdjacentHTML('beforeend', `
      <div class="stat-card">
        <div class="stat-value">${s.value}</div>
        <div class="stat-label">${s.label}</div>
      </div>`);
  });
}

function renderQualityStrip(breakdown) {
  qualStrip.innerHTML = '';
  const order = ['0', '1', '2', '-1'];
  order.forEach(flag => {
    const info  = qualityFlags[flag] || { label: `Flag ${flag}`, description: '', color: '#94a3b8' };
    const entry = breakdown[flag];
    if (!entry) return;
    qualStrip.insertAdjacentHTML('beforeend', `
      <div class="q-badge" title="${info.description}">
        <span class="dot" style="background:${info.color}"></span>
        <strong>${info.label}</strong>: ${entry.count.toLocaleString()} readings (${entry.pct}%)
      </div>`);
  });
}

function renderChart(data) {
  if (!data || data.length === 0) return;

  // Group by quality flag for separate traces (so we can colour-code points)
  const flagGroups = {};
  data.forEach(row => {
    const flag = String(row.MW_QUALITY_FLAG ?? '-1');
    if (!flagGroups[flag]) flagGroups[flag] = { x: [], y: [] };
    flagGroups[flag].x.push(row.MEASUREMENT_DATETIME);
    flagGroups[flag].y.push(row.MEASURED_MW);
  });

  const traces = Object.entries(flagGroups).map(([flag, pts]) => {
    const info = qualityFlags[flag] || { label: `Flag ${flag}`, color: '#94a3b8' };
    return {
      x: pts.x,
      y: pts.y,
      mode: 'lines+markers',
      marker: { size: 2, color: info.color },
      line: { width: 1, color: info.color },
      name: info.label,
      type: 'scattergl',
    };
  });

  const layout = {
    paper_bgcolor: 'transparent',
    plot_bgcolor:  'transparent',
    font: { color: '#e2e8f0', size: 11 },
    xaxis: {
      title: 'Time',
      gridcolor: '#334155',
      linecolor: '#334155',
      tickfont: { color: '#94a3b8' },
    },
    yaxis: {
      title: 'Power (MW)',
      gridcolor: '#334155',
      linecolor: '#334155',
      tickfont: { color: '#94a3b8' },
      zeroline: true,
      zerolinecolor: '#475569',
    },
    legend: { orientation: 'h', y: -0.2 },
    margin: { t: 20, r: 20, b: 60, l: 60 },
    hovermode: 'x unified',
  };

  Plotly.react('chart-container', traces, layout, {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['select2d', 'lasso2d'],
  });
}

function flagClass(flag) {
  switch (String(flag)) {
    case '0':  return 'flag-good';
    case '1':  return 'flag-suspect';
    case '2':  return 'flag-bad';
    default:   return 'flag-na';
  }
}

function flagLabel(flag) {
  return (qualityFlags[String(flag)] || { label: flag }).label;
}

function renderTable(data, totalRows) {
  tableBody.innerHTML = '';
  data.forEach(row => {
    const cls = flagClass(row.MW_QUALITY_FLAG);
    tableBody.insertAdjacentHTML('beforeend', `
      <tr>
        <td>${row.MEASUREMENT_DATETIME ?? ''}</td>
        <td>${row.INTERVAL_DATETIME ?? ''}</td>
        <td>${row.MEASURED_MW != null ? row.MEASURED_MW.toFixed(4) : '—'}</td>
        <td class="${cls}">${flagLabel(row.MW_QUALITY_FLAG)}</td>
      </tr>`);
  });

  if (totalRows > data.length) {
    tableNote.textContent =
      `Showing ${data.length.toLocaleString()} of ${totalRows.toLocaleString()} rows. Download the full dataset using the buttons above.`;
    tableNote.classList.remove('hidden');
  } else {
    tableNote.classList.add('hidden');
  }
}

/* ── UI helpers ── */
function showError(msg) {
  errorMsg.textContent = msg;
  errorBox.classList.remove('hidden');
}
function hideError() { errorBox.classList.add('hidden'); }
function showLoading(show) {
  loadingBox.classList.toggle('hidden', !show);
}
function hideResults() { resultsBox.classList.add('hidden'); }

/* ── Start ── */
init();
