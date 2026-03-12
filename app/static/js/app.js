/* ─────────────────────────────────────────────────────────────────────────────
   NEM BESS SCADA & Energy Explorer — frontend logic
   ───────────────────────────────────────────────────────────────────────────── */

const API = '';   // same origin

/* ── Global state ── */
let bessList        = {};
let qualityFlags    = {};
let cutoverDate     = '2026-01-11';   // SCADA current/archive cutover (from /api/info)
let scadaStartDate  = '2025-02-28';   // from /api/info
let dispatchStartDate = '2025-02-11'; // from /api/info
let appEstimates    = {
  current:          { seconds: 120, sample_count: 0, is_default: true },
  archive:          { seconds: 480, sample_count: 0, is_default: true },
  dispatch_current: { seconds:  60, sample_count: 0, is_default: true },
  dispatch_archive: { seconds: 300, sample_count: 0, is_default: true },
};

/* ── DOM refs ── */
const selState    = document.getElementById('sel-state');
const selBess     = document.getElementById('sel-bess');
const inpDate     = document.getElementById('inp-date');
const chkScada    = document.getElementById('chk-scada');
const chkEnergy   = document.getElementById('chk-energy');
const btnLoad     = document.getElementById('btn-load');
const errorBox    = document.getElementById('error-box');
const errorMsg    = document.getElementById('error-msg');
const loadingBox  = document.getElementById('loading-box');
const loadingMsg  = document.getElementById('loading-msg');
const resultsBox  = document.getElementById('results-box');
const scadaResults  = document.getElementById('scada-results');
const energyResults = document.getElementById('energy-results');
// SCADA
const statsGrid   = document.getElementById('stats-grid');
const qualStrip   = document.getElementById('quality-strip');
const btnCsv      = document.getElementById('btn-csv');
const btnParquet  = document.getElementById('btn-parquet');
// Energy
const energyStatsGrid  = document.getElementById('energy-stats-grid');
const btnEnergyCsv     = document.getElementById('btn-energy-csv');
const btnEnergyParquet = document.getElementById('btn-energy-parquet');

/* ── Boot ── */
async function init() {
  // Default date = yesterday
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yd = yesterday.toISOString().slice(0, 10);
  inpDate.value = yd;
  inpDate.max   = yd;

  // Fetch app info: min dates, cutover, timing estimates
  try {
    const info = await fetch(`${API}/api/info`).then(r => r.json());
    if (info.data_start_date)      scadaStartDate    = info.data_start_date;
    if (info.dispatch_start_date)  dispatchStartDate = info.dispatch_start_date;
    if (info.cutover_date)         cutoverDate       = info.cutover_date;
    if (info.estimates)            appEstimates      = { ...appEstimates, ...info.estimates };
    inpDate.min = scadaStartDate;   // default: SCADA start (most restrictive)
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
    opt.value       = state;
    opt.textContent = state;
    selState.appendChild(opt);
  });

  selState.disabled = false;
  selState.addEventListener('change', onStateChange);
  selBess.addEventListener('change', onBessChange);
  chkScada.addEventListener('change', onCheckboxChange);
  chkEnergy.addEventListener('change', onCheckboxChange);
  btnLoad.addEventListener('click', onLoad);
}

function onStateChange() {
  const state = selState.value;
  selBess.innerHTML = '<option value="">— Select BESS —</option>';
  selBess.disabled  = !state;
  inpDate.disabled  = true;
  updateLoadButton();

  if (!state || !bessList[state]) return;

  bessList[state].forEach(b => {
    const mw  = b.capacity_mw  != null ? `${b.capacity_mw} MW`   : '? MW';
    const mwh = b.capacity_mwh != null ? `${b.capacity_mwh} MWh` : '? MWh';
    const opt = document.createElement('option');
    opt.value       = b.duid;
    opt.textContent = `${b.duid} (${b.name}) (${mw} / ${mwh})`;
    selBess.appendChild(opt);
  });
}

function onBessChange() {
  inpDate.disabled = !selBess.value;
  updateDateMin();
  updateLoadButton();
}

function onCheckboxChange() {
  updateDateMin();
  updateLoadButton();
}

function updateDateMin() {
  // If SCADA is selected (alone or combined) use SCADA start date (more restrictive).
  // If only energy is selected, allow the earlier dispatch start date.
  inpDate.min = chkScada.checked ? scadaStartDate : dispatchStartDate;
}

function updateLoadButton() {
  const ready      = !!selBess.value && !!inpDate.value;
  const anyChecked = chkScada.checked || chkEnergy.checked;
  btnLoad.disabled = !ready || !anyChecked;
}

/* ── Timing estimate helpers ── */

/**
 * Return the combined wait-time estimate for the selected data types.
 * When both are selected the requests run in parallel, so we use max().
 */
function getLoadEstimate(doScada, doEnergy, dateStr) {
  const scadaSrc = dateStr >= cutoverDate ? 'current' : 'archive';
  const scadaEst = appEstimates[scadaSrc]          || appEstimates.current;
  const energyEst = appEstimates.dispatch_current  || { seconds: 60, is_default: true, sample_count: 0 };

  if (doScada && doEnergy) {
    const maxSec = Math.max(scadaEst.seconds, energyEst.seconds);
    // is_default only if both are defaults; sample_count = min of the two
    return {
      seconds:       maxSec,
      is_default:    scadaEst.is_default && energyEst.is_default,
      sample_count:  Math.min(scadaEst.sample_count, energyEst.sample_count),
    };
  }
  return doScada ? scadaEst : energyEst;
}

function formatEstimate(est) {
  const sec = est.seconds || 60;
  const suffix = est.is_default
    ? ''
    : ` (based on ${est.sample_count} recent request${est.sample_count === 1 ? '' : 's'})`;
  let duration;
  if      (sec < 60)   duration = 'less than a minute';
  else if (sec < 100)  duration = 'about 1 minute';
  else if (sec < 160)  duration = 'about 2 minutes';
  else if (sec < 220)  duration = 'about 3 minutes';
  else if (sec < 310)  duration = 'about 4–5 minutes';
  else if (sec < 420)  duration = 'about 6–7 minutes';
  else if (sec < 570)  duration = 'about 8–10 minutes';
  else                 duration = '10 minutes or more';
  return `this typically takes ${duration}${suffix}`;
}

/* ── Load data ── */
function setFormLocked(locked) {
  selState.disabled  = locked;
  selBess.disabled   = locked || !selState.value;
  inpDate.disabled   = locked || !selBess.value;
  chkScada.disabled  = locked;
  chkEnergy.disabled = locked;
  btnLoad.disabled   = locked;
}

async function onLoad() {
  const duid     = selBess.value;
  const date     = inpDate.value;
  const doScada  = chkScada.checked;
  const doEnergy = chkEnergy.checked;

  if (!duid || !date || (!doScada && !doEnergy)) return;

  hideError();
  hideResults();

  // Build loading message with combined timing estimate
  const est = getLoadEstimate(doScada, doEnergy, date);
  let dataLabel;
  if (doScada && doEnergy) dataLabel = '4-second SCADA and 5-minute energy data';
  else if (doScada)        dataLabel = '4-second SCADA data';
  else                     dataLabel = '5-minute dispatch energy data';

  loadingMsg.textContent =
    `Fetching ${dataLabel} from AEMO NEMWEB\u2026 ${formatEstimate(est)}. Please wait.`;

  showLoading(true);
  setFormLocked(true);

  // Fire selected requests in parallel
  const [scadaResult, energyResult] = await Promise.allSettled([
    doScada  ? fetchScada(duid, date)  : Promise.resolve(null),
    doEnergy ? fetchEnergy(duid, date) : Promise.resolve(null),
  ]);

  showLoading(false);
  setFormLocked(false);

  // Collect error messages
  const errors = [];
  if (doScada  && scadaResult.status  === 'rejected')
    errors.push(`SCADA: ${scadaResult.reason?.message  || scadaResult.reason}`);
  if (doEnergy && energyResult.status === 'rejected')
    errors.push(`Energy: ${energyResult.reason?.message || energyResult.reason}`);
  if (errors.length) showError(errors.join(' | '));

  // Render whatever succeeded
  const scadaPayload  = scadaResult.status  === 'fulfilled' ? scadaResult.value  : null;
  const energyPayload = energyResult.status === 'fulfilled' ? energyResult.value : null;

  if (scadaPayload || energyPayload) {
    const base   = `${API}/api/download`;
    const params = `duid=${encodeURIComponent(duid)}&date=${date}`;

    if (scadaPayload) {
      renderScadaResults(scadaPayload);
      btnCsv.href     = `${base}/csv?${params}`;
      btnParquet.href = `${base}/parquet?${params}`;
      scadaResults.classList.remove('hidden');
    }
    if (energyPayload) {
      renderEnergyResults(energyPayload);
      btnEnergyCsv.href     = `${base}/energy-csv?${params}`;
      btnEnergyParquet.href = `${base}/energy-parquet?${params}`;
      energyResults.classList.remove('hidden');
    }
    resultsBox.classList.remove('hidden');
  }
}

/* ── Fetch helpers ── */
async function fetchScada(duid, date) {
  const resp = await fetch(
    `${API}/api/data?duid=${encodeURIComponent(duid)}&date=${date}`
  );
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: `Server error ${resp.status}` }));
    throw new Error(err.detail || `Server error ${resp.status}`);
  }
  return resp.json();
}

async function fetchEnergy(duid, date) {
  const resp = await fetch(
    `${API}/api/energy-data?duid=${encodeURIComponent(duid)}&date=${date}`
  );
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: `Server error ${resp.status}` }));
    throw new Error(err.detail || `Server error ${resp.status}`);
  }
  return resp.json();
}

/* ── SCADA render ── */
function renderScadaResults(payload) {
  const { summary, data } = payload;
  renderStats(summary, payload.total_rows);
  renderQualityStrip(summary.flag_breakdown);
  renderScadaChart(data);
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
  ['0', '1', '2', '-1'].forEach(flag => {
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

function renderScadaChart(data) {
  if (!data || data.length === 0) return;

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
      x:      pts.x,
      y:      pts.y,
      mode:   'lines+markers',
      marker: { size: 2, color: info.color },
      line:   { width: 1, color: info.color },
      name:   info.label,
      type:   'scattergl',
    };
  });

  const layout = _chartLayout('Time', 'Power (MW)');
  Plotly.react('scada-chart-container', traces, layout, _chartConfig());
}

/* ── Energy render ── */
function renderEnergyResults(payload) {
  const { summary, data } = payload;
  renderEnergyStats(summary, payload.total_rows);
  renderEnergyChart(data);
}

function renderEnergyStats(summary, totalRows) {
  energyStatsGrid.innerHTML = '';
  const stats = [
    { label: 'Total Intervals', value: totalRows.toLocaleString() },
    { label: 'Min Dispatch MW',  value: summary.min_mw  != null ? summary.min_mw.toFixed(1)  : '—' },
    { label: 'Max Dispatch MW',  value: summary.max_mw  != null ? summary.max_mw.toFixed(1)  : '—' },
    { label: 'Min Energy (MWh)', value: summary.min_init_mwh != null ? summary.min_init_mwh.toFixed(1) : '—' },
    { label: 'Max Energy (MWh)', value: summary.max_init_mwh != null ? summary.max_init_mwh.toFixed(1) : '—' },
    { label: 'Mean Energy (MWh)',value: summary.mean_init_mwh != null ? summary.mean_init_mwh.toFixed(1) : '—' },
  ];
  stats.forEach(s => {
    energyStatsGrid.insertAdjacentHTML('beforeend', `
      <div class="stat-card">
        <div class="stat-value">${s.value}</div>
        <div class="stat-label">${s.label}</div>
      </div>`);
  });
}

function renderEnergyChart(data) {
  if (!data || data.length === 0) return;

  const times = data.map(r => r.SETTLEMENTDATE);

  const traces = [];

  // Initial Energy Storage (left y-axis, MWh) — primary line
  if (data.some(r => r.INITIAL_ENERGY_STORAGE != null)) {
    traces.push({
      x:      times,
      y:      data.map(r => r.INITIAL_ENERGY_STORAGE),
      name:   'Initial Energy (MWh)',
      type:   'scatter',
      mode:   'lines+markers',
      yaxis:  'y1',
      line:   { width: 2, color: '#22c55e' },
      marker: { size: 4, color: '#22c55e' },
    });
  }

  // End-of-interval Energy Storage (left y-axis, MWh) — dashed
  if (data.some(r => r.ENERGY_STORAGE != null)) {
    traces.push({
      x:      times,
      y:      data.map(r => r.ENERGY_STORAGE),
      name:   'End Energy (MWh)',
      type:   'scatter',
      mode:   'lines+markers',
      yaxis:  'y1',
      line:   { width: 1.5, color: '#38bdf8', dash: 'dash' },
      marker: { size: 3, color: '#38bdf8' },
    });
  }

  // Initial MW dispatch (right y-axis, MW)
  if (data.some(r => r.INITIALMW != null)) {
    traces.push({
      x:      times,
      y:      data.map(r => r.INITIALMW),
      name:   'Dispatch MW',
      type:   'scatter',
      mode:   'lines+markers',
      yaxis:  'y2',
      line:   { width: 1.5, color: '#f97316' },
      marker: { size: 3, color: '#f97316' },
    });
  }

  const layout = {
    ..._chartLayout('Time', 'Energy (MWh)'),
    yaxis2: {
      title:       'Dispatch (MW)',
      overlaying:  'y',
      side:        'right',
      gridcolor:   '#334155',
      linecolor:   '#334155',
      tickfont:    { color: '#f97316' },
      titlefont:   { color: '#f97316' },
      zeroline:    true,
      zerolinecolor: '#475569',
    },
    legend: { orientation: 'h', y: -0.2 },
    margin: { t: 20, r: 70, b: 60, l: 60 },  // wider right margin for y2 label
  };

  Plotly.react('energy-chart-container', traces, layout, _chartConfig());
}

/* ── Shared chart helpers ── */
function _chartLayout(xTitle, yTitle) {
  return {
    autosize:      true,
    paper_bgcolor: 'transparent',
    plot_bgcolor:  'transparent',
    font:          { color: '#e2e8f0', size: 11 },
    xaxis: {
      title:     xTitle,
      gridcolor: '#334155',
      linecolor: '#334155',
      tickfont:  { color: '#94a3b8' },
    },
    yaxis: {
      title:          yTitle,
      gridcolor:      '#334155',
      linecolor:      '#334155',
      tickfont:       { color: '#94a3b8' },
      zeroline:       true,
      zerolinecolor:  '#475569',
    },
    legend:    { orientation: 'h', y: -0.2 },
    margin:    { t: 20, r: 20, b: 60, l: 60 },
    hovermode: 'x unified',
  };
}

function _chartConfig() {
  return {
    responsive:              true,
    displayModeBar:          true,
    modeBarButtonsToRemove:  ['select2d', 'lasso2d'],
  };
}

/* ── UI helpers ── */
function showError(msg) {
  errorMsg.textContent = msg;
  errorBox.classList.remove('hidden');
}
function hideError()   { errorBox.classList.add('hidden'); }
function showLoading(show) {
  loadingBox.classList.toggle('hidden', !show);
}
function hideResults() {
  resultsBox.classList.add('hidden');
  scadaResults.classList.add('hidden');
  energyResults.classList.add('hidden');
}

/* ── Start ── */
init();
