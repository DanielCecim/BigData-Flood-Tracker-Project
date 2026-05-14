"use strict";

// ------------------------------------------------------------------ //
//  Map
// ------------------------------------------------------------------ //

const map = L.map("map", { preferCanvas: true }).setView([54.5, -2.5], 6);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "© <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a>",
  maxZoom: 18,
}).addTo(map);

// ------------------------------------------------------------------ //
//  Alert styling
// ------------------------------------------------------------------ //

const ALERT_COLOUR = { normal:"#2ecc71", watch:"#f1c40f", alert:"#e67e22", warning:"#e74c3c" };
const ALERT_RADIUS = { normal:4, watch:5, alert:7, warning:9 };
const ALERT_LABEL  = { normal:"Normal", watch:"Flood Watch", alert:"Flood Alert", warning:"Flood Warning" };

const NO_DATA_COLOUR = "#555";
const NO_DATA_RADIUS = 3;
const NO_DATA_OPACITY = 0.35;

// ------------------------------------------------------------------ //
//  State
// ------------------------------------------------------------------ //

const markers     = {};
const stationMeta = {};
let ws            = null;
let playing       = false;
let speed         = 5.0;
let prevActiveRefs = new Set();

// ------------------------------------------------------------------ //
//  DOM refs
// ------------------------------------------------------------------ //

const btnPlay     = document.getElementById("btn-play");
const dateDisplay = document.getElementById("date-display");
const progressBar = document.getElementById("progress-bar");
const progressLbl = document.getElementById("progress-label");
const speedSlider = document.getElementById("speed-slider");
const speedVal    = document.getElementById("speed-val");
const statusBadge = document.getElementById("status-badge");
const stationCnt  = document.getElementById("station-count");
const sidebar     = document.getElementById("sidebar");
const btnToggle   = document.getElementById("btn-toggle-sidebar");

// ------------------------------------------------------------------ //
//  Sidebar toggle
// ------------------------------------------------------------------ //

btnToggle.addEventListener("click", () => {
  const collapsed = sidebar.classList.toggle("collapsed");
  btnToggle.textContent = collapsed ? "▶" : "◀";
  // Let Leaflet know the container size changed
  setTimeout(() => map.invalidateSize(), 220);
});

// ------------------------------------------------------------------ //
//  Station initialisation
// ------------------------------------------------------------------ //

function buildPopup(meta, pred) {
  const thresh = meta.threshold != null ? meta.threshold.toFixed(3) + " m" : "–";

  if (!pred) {
    return `
      <div class="popup-title">${meta.label}</div>
      <div class="popup-ref">${meta.station_ref}</div>
      <div class="popup-row" style="color:#8b949e;font-size:0.8rem;margin-top:6px">No data for this date</div>
    `;
  }

  const prob  = (pred.probability * 100).toFixed(1) + "%";
  const level = ALERT_LABEL[pred.alert_level] || pred.alert_level;
  const valM  = pred.value_m != null ? pred.value_m.toFixed(3) + " m" : "–";
  const cls   = `alert-${pred.alert_level}`;

  let forecastRow = "";
  if (pred.q50 != null) {
    forecastRow = `
    <div class="popup-row"><span class="popup-lbl">Forecast tomorrow</span><span class="popup-val">${pred.q50.toFixed(3)} m</span></div>
    <div class="popup-row"><span class="popup-lbl">80% range</span><span class="popup-val">${pred.q10.toFixed(3)}–${pred.q90.toFixed(3)} m</span></div>`;
  }

  return `
    <div class="popup-title">${meta.label}</div>
    <div class="popup-ref">${meta.station_ref}</div>
    <div class="popup-row"><span class="popup-lbl">River level</span><span class="popup-val">${valM}</span></div>
    <div class="popup-row"><span class="popup-lbl">Flood threshold</span><span class="popup-val">${thresh}</span></div>${forecastRow}
    <div class="popup-row"><span class="popup-lbl">Flood probability</span><span class="popup-val">${prob}</span></div>
    <span class="popup-alert ${cls}">${level}</span>
  `;
}

async function initStations() {
  setStatus("Loading stations…", "");
  const stations = await fetch("/stations").then(r => r.json());
  stations.forEach(s => {
    stationMeta[s.station_ref] = s;
    const m = L.circleMarker([s.lat, s.lon], {
      radius: NO_DATA_RADIUS, color: NO_DATA_COLOUR,
      fillColor: NO_DATA_COLOUR, fillOpacity: NO_DATA_OPACITY, weight: 1,
    });
    m.bindPopup(buildPopup(s, null), { maxWidth: 260 });
    m.addTo(map);
    markers[s.station_ref] = m;
  });
  stationCnt.textContent = `${stations.length.toLocaleString()} stations`;
  setStatus("Ready", "ready");
}

// ------------------------------------------------------------------ //
//  Statistics sidebar
// ------------------------------------------------------------------ //

function fmt(n) { return n.toLocaleString(); }

async function loadStats() {
  const s = await fetch("/stats").then(r => r.json());

  // Overview cards
  const grid = document.getElementById("overview-grid");
  grid.innerHTML = ["warning","alert","watch","normal"].map(lvl => `
    <div class="ov-card">
      <div class="ov-label col-${lvl}">${lvl}</div>
      <div class="ov-count col-${lvl}">${fmt(s.counts[lvl])}</div>
      <div class="ov-pct">${s.pct[lvl]}% of days</div>
      <div class="ov-bar-track">
        <div class="ov-bar bg-${lvl}" style="width:${s.pct[lvl]}%"></div>
      </div>
    </div>
  `).join("");

  // Year table
  const tbody = document.getElementById("year-tbody");
  tbody.innerHTML = s.by_year.map(row => `
    <tr>
      <td>${row.year}</td>
      <td class="col-watch">${fmt(row.watch)}</td>
      <td class="col-alert">${fmt(row.alert)}</td>
      <td class="col-warning">${fmt(row.warning)}</td>
    </tr>
  `).join("");

  // Top stations — clicking pans the map to that station
  const list = document.getElementById("top-stations-list");
  list.innerHTML = s.top_stations.map((st, i) => `
    <div class="station-row" data-ref="${st.station_ref}">
      <span class="station-rank">${i + 1}</span>
      <span class="station-name" title="${st.label}">${st.label}</span>
      <span class="station-badge">${st.warning_days}d</span>
    </div>
  `).join("");

  list.querySelectorAll(".station-row").forEach(row => {
    row.addEventListener("click", () => {
      const ref  = row.dataset.ref;
      const meta = stationMeta[ref];
      if (meta) {
        map.setView([meta.lat, meta.lon], 10, { animate: true });
        const m = markers[ref];
        if (m) m.openPopup();
      }
    });
  });
}

// ------------------------------------------------------------------ //
//  Live section — updated every frame
// ------------------------------------------------------------------ //

function updateLive(stations) {
  const counts = { normal: 0, watch: 0, alert: 0, warning: 0 };
  stations.forEach(s => { counts[s.alert_level] = (counts[s.alert_level] || 0) + 1; });
  ["normal","watch","alert","warning"].forEach(lvl => {
    document.getElementById(`live-${lvl}`).textContent = counts[lvl];
  });
}

// ------------------------------------------------------------------ //
//  Simulation frame
// ------------------------------------------------------------------ //

function applyFrame(frame) {
  // Stations that had data last frame but not this one → reset to no-data grey
  const activeRefs = new Set(frame.stations.map(p => p.station_ref));
  prevActiveRefs.forEach(ref => {
    if (!activeRefs.has(ref)) {
      const m = markers[ref];
      if (!m) return;
      m.setStyle({ fillColor: NO_DATA_COLOUR, color: NO_DATA_COLOUR,
                   radius: NO_DATA_RADIUS, fillOpacity: NO_DATA_OPACITY });
      const meta = stationMeta[ref];
      if (meta) m.bindPopup(buildPopup(meta, null), { maxWidth: 260 });
    }
  });
  prevActiveRefs = activeRefs;

  // Apply current frame data
  frame.stations.forEach(pred => {
    const m = markers[pred.station_ref];
    if (!m) return;
    const col = ALERT_COLOUR[pred.alert_level] || ALERT_COLOUR.normal;
    const rad = ALERT_RADIUS[pred.alert_level] || ALERT_RADIUS.normal;
    m.setStyle({ fillColor: col, color: col, radius: rad, fillOpacity: 0.75 });
    const meta = stationMeta[pred.station_ref];
    if (meta) m.bindPopup(buildPopup(meta, pred), { maxWidth: 260 });
  });

  dateDisplay.textContent = frame.date;
  const pct = frame.total_days > 0
    ? ((frame.day_index + 1) / frame.total_days * 100).toFixed(1) : 0;
  progressBar.style.width = pct + "%";
  progressLbl.textContent = `Day ${frame.day_index + 1} of ${frame.total_days}`;

  updateLive(frame.stations);
}

// ------------------------------------------------------------------ //
//  WebSocket
// ------------------------------------------------------------------ //

function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/simulate`);
  ws.onopen  = () => setStatus("Connected", "ready");
  ws.onmessage = ({ data }) => {
    const msg = JSON.parse(data);
    if      (msg.type === "frame") applyFrame(msg);
    else if (msg.type === "end")   { setStatus("Simulation complete", "ready"); setPlayState(false); }
    else if (msg.type === "error") { setStatus("Error: " + msg.message, "error"); setPlayState(false); }
  };
  ws.onclose = () => setStatus("Disconnected", "error");
  ws.onerror = () => setStatus("WebSocket error", "error");
}

function wsSend(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

// ------------------------------------------------------------------ //
//  Controls
// ------------------------------------------------------------------ //

function setPlayState(isPlaying) {
  playing = isPlaying;
  btnPlay.textContent = isPlaying ? "⏸ Pause" : "▶ Play";
  btnPlay.classList.toggle("paused", isPlaying);
}

btnPlay.addEventListener("click", () => {
  setPlayState(!playing);
  wsSend({ action: playing ? "play" : "pause" });
});

speedSlider.addEventListener("input", () => {
  speed = parseFloat(speedSlider.value);
  speedVal.textContent = speed + "×";
  wsSend({ action: "speed", value: speed });
});

document.getElementById("progress-bar-bg").addEventListener("click", async (e) => {
  const rect = e.currentTarget.getBoundingClientRect();
  const frac = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
  const { dates } = await fetch("/dates").then(r => r.json());
  wsSend({ action: "seek", date: dates[Math.floor(frac * (dates.length - 1))] });
});

function setStatus(text, cls) {
  statusBadge.textContent = text;
  statusBadge.className   = cls || "";
}

// ------------------------------------------------------------------ //
//  Boot
// ------------------------------------------------------------------ //

speedVal.textContent = speedSlider.value + "×";
initStations().then(() => { connectWS(); loadStats(); });
