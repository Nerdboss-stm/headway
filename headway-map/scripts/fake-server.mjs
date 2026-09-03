// Stand-in for the live bridge: ~400 trains walking real GTFS shapes,
// ~2500 Citi Bike stations, one agent card every 8s.  node scripts/fake-server.mjs
import {readFileSync} from 'node:fs';
import {WebSocketServer} from 'ws';

const PORT = 8787;
const TRAIN_COUNT = 400;
const HOP_MS = 15000;
const STEP = 20;                 // shape points per hop, ~ one stop
const STATION_UPDATES_PER_SEC = 20;
const AGENT_EVERY_MS = 8000;
const ROOT = new URL('..', import.meta.url).pathname;

const geo = JSON.parse(readFileSync(ROOT + 'public/subway.geojson', 'utf8'));
const shapes = geo.features.filter(f => f.geometry.coordinates.length > STEP * 2);
const bikes = JSON.parse(readFileSync(ROOT + 'data/citibike.json', 'utf8'));

const rand = n => Math.floor(Math.random() * n);

// Delay mix: most on time, ~12% ringed, ~5% severe.
const rollDelay = () => {
  const r = Math.random();
  if (r < 0.05) return 430 + rand(600);
  if (r < 0.17) return 190 + rand(220);
  return rand(150);
};

const trains = Array.from({length: TRAIN_COUNT}, (_, i) => {
  const shape = shapes[rand(shapes.length)];
  return {
    id: `T${i}`,
    route_id: shape.properties.route_id,
    coords: shape.geometry.coordinates,
    idx: rand(Math.max(1, shape.geometry.coordinates.length - STEP)),
    dir: Math.random() < 0.5 ? 1 : -1,
    delay_sec: rollDelay(),
    nextAt: Date.now() + rand(HOP_MS),   // stagger so hops spread over the window
  };
});

const stations = bikes.map(s => ({
  id: s.id, lat: s.lat, lon: s.lon,
  bikes_available: rand((s.capacity ?? 20) + 1),
}));

const ALERTS = [
  ['L', 40.717304, -73.956872, 'SIGNAL PROBLEM AT BEDFORD AV',
    'Southbound L trains are holding while crews work. Expect 12 to 15 minute waits.'],
  ['G', 40.746554, -73.943832, 'SWITCH TROUBLE AT COURT SQ',
    'G trains are running with delays in both directions. Some trains are being rerouted.'],
  ['J', 40.708359, -73.957757, 'SICK PASSENGER AT MARCY AV',
    'Manhattan-bound J and M trains are delayed while we assist a passenger.'],
  ['A', 40.718092, -74.000494, 'RAIL CONDITION AT CANAL ST',
    'A and C trains are running local and moving slowly past the work area.'],
  ['4', 40.734673, -73.989951, 'CROWDING AT UNION SQ-14 ST',
    'Downtown 4 and 5 trains are delayed by heavy platform crowding.'],
  ['N', 40.684359, -73.977666, 'TRACK FIRE AT ATLANTIC AV',
    'N and R service is suspended in both directions between Atlantic Av and DeKalb Av.'],
  ['M', 40.699814, -73.911586, 'DOOR PROBLEM AT MYRTLE-WYCKOFF',
    'A train is being taken out of service. Following M trains are delayed behind it.'],
  ['L', 40.714063, -73.950275, 'TRAINS BUNCHING AT LORIMER ST',
    'Four southbound L trains are running within three minutes of each other.'],
  ['F', 40.718611, -73.988114, 'SIGNAL PROBLEM AT DELANCEY ST',
    'Brooklyn-bound F trains are stopping and holding at Delancey St-Essex St.'],
  ['C', 40.678822, -73.905249, 'POLICE ACTIVITY AT BROADWAY JCT',
    'A, C and J trains are bypassing Broadway Junction in both directions.'],
];

const wss = new WebSocketServer({port: PORT});
const clients = new Set();

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}
function broadcast(payload) {
  if (!payload.length) return;
  const text = JSON.stringify(payload);
  for (const ws of clients) if (ws.readyState === ws.OPEN) ws.send(text);
}

wss.on('connection', ws => {
  clients.add(ws);
  ws.on('close', () => clients.delete(ws));
  for (let i = 0; i < stations.length; i += 500) {
    send(ws, stations.slice(i, i + 500).map(s => ({type: 'station', ...s})));
  }
  // Seed every train immediately so the map is full on the first frame.
  send(ws, trains.map(t => hop(t, false)));
  console.log(`client connected (${clients.size}); sent ${stations.length} stations, ${trains.length} trains`);
});

function hop(t, advance = true) {
  if (advance) {
    t.idx += t.dir * STEP;
    if (t.idx <= 0 || t.idx >= t.coords.length - STEP) { t.dir *= -1; t.idx = Math.max(0, Math.min(t.idx, t.coords.length - STEP - 1)); }
    if (Math.random() < 0.05) t.delay_sec = rollDelay();
  }
  const from = t.coords[t.idx];
  const to = t.coords[Math.max(0, Math.min(t.coords.length - 1, t.idx + t.dir * STEP))];
  return {type: 'train', id: t.id, route_id: t.route_id, from, to, delay_sec: t.delay_sec, ts: Date.now()};
}

setInterval(() => {
  const now = Date.now();
  const due = [];
  for (const t of trains) {
    if (now >= t.nextAt) { t.nextAt = now + HOP_MS; due.push(hop(t)); }
  }
  broadcast(due);
}, 50);

setInterval(() => {
  const batch = [];
  for (let i = 0; i < STATION_UPDATES_PER_SEC; i++) {
    const s = stations[rand(stations.length)];
    s.bikes_available = Math.max(0, s.bikes_available + rand(7) - 3);
    batch.push({type: 'station', ...s});
  }
  broadcast(batch);
}, 1000);

let n = 0;
setInterval(() => {
  const [route, lat, lon, title, body] = ALERTS[n % ALERTS.length];
  const severity = n % 3 === 2 ? 'critical' : 'warning';
  broadcast([{
    type: 'agent', id: `A${n}`, lat, lon, route_id: route, severity, title, body,
    riders_affected: 600 + rand(9000), ts: Date.now(),
  }]);
  n++;
}, AGENT_EVERY_MS);

console.log(`fake feed on ws://localhost:${PORT} — ${trains.length} trains, ` +
  `${stations.length} stations, agent every ${AGENT_EVERY_MS / 1000}s`);
