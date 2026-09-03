// Socket client + world state. Trains interpolate from their last stop to their
// next stop over one 15s hop; nothing else moves.
export const HOP_MS = 15000;
const TRAIN_TTL = 90000;
// The bridge releases trains in clumps a second or two apart, so a short window
// reads zero between them. Average over long enough to always span several.
const EPS_WINDOW = 10000;
const RIDERS_PER_DISRUPTION = 1200;  // fallback when the event carries no estimate

export function createStore(url, onAgent) {
  const trains = new Map();
  const stations = new Map();
  let trainList = [], stationList = [];
  let trainsDirty = false, stationsDirty = false;
  let disruptions = 0, riders = 0, connected = false;
  let stamps = [];

  const num = v => (typeof v === 'number' && Number.isFinite(v) ? v : null);

  function train(m) {
    const from = m.from ?? [num(m.lon), num(m.lat)];
    const to = m.to ?? [num(m.next_lon) ?? from[0], num(m.next_lat) ?? from[1]];
    if (from[0] == null || from[1] == null) return;
    const id = String(m.id ?? m.trip_id ?? '');
    if (!id) return;
    const prev = trains.get(id);
    if (!prev) trainsDirty = true;
    trains.set(id, {
      id, route_id: m.route_id ?? '', from, to,
      delay_sec: num(m.delay_sec) ?? 0, pos: prev?.pos ?? [from[0], from[1]],
      t0: performance.now(), seen: performance.now(),
    });
  }

  function station(m) {
    const lon = num(m.lon), lat = num(m.lat);
    if (lon == null || lat == null) return;
    const id = String(m.id ?? m.station_id ?? '');
    if (!id) return;
    if (!stations.has(id)) stationsDirty = true;
    const s = stations.get(id) ?? {id, pos: [lon, lat]};
    s.pos[0] = lon; s.pos[1] = lat;
    s.bikes = num(m.bikes_available) ?? num(m.num_bikes_available) ?? 0;
    stations.set(id, s);
    stationsDirty = true;
  }

  function agent(m) {
    if (num(m.lat) == null || num(m.lon) == null) return;
    disruptions += 1;
    riders += num(m.riders_affected) ?? RIDERS_PER_DISRUPTION;
    onAgent({
      id: m.id ?? String(disruptions), lat: m.lat, lon: m.lon, route_id: m.route_id ?? '',
      severity: String(m.severity ?? '').toLowerCase(),
      title: m.title ?? '', body: m.body ?? '',
    });
  }

  function handle(msg) {
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      stamps.push(performance.now());
      if (m?.type === 'train') train(m);
      else if (m?.type === 'station') station(m);
      else if (m?.type === 'agent') agent(m);
    }
  }

  let sock;
  (function connect() {
    sock = new WebSocket(url);
    sock.onopen = () => { connected = true; };
    sock.onclose = () => { connected = false; setTimeout(connect, 1000); };
    sock.onerror = () => sock.close();
    sock.onmessage = e => { try { handle(JSON.parse(e.data)); } catch { /* ignore junk */ } };
  })();

  return {
    // Advance interpolation in place; the arrays keep their identity so deck.gl
    // only re-uploads what actually changed.
    tick(now) {
      for (const [id, t] of trains) {
        if (now - t.seen > TRAIN_TTL) { trains.delete(id); trainsDirty = true; continue; }
        const k = Math.min(1, (now - t.t0) / HOP_MS);
        t.pos[0] = t.from[0] + (t.to[0] - t.from[0]) * k;
        t.pos[1] = t.from[1] + (t.to[1] - t.from[1]) * k;
      }
      if (trainsDirty) { trainList = [...trains.values()]; trainsDirty = false; }
      if (stationsDirty) { stationList = [...stations.values()]; stationsDirty = false; }
      const cut = now - EPS_WINDOW;
      if (stamps.length > 50000 || stamps[0] < cut) stamps = stamps.filter(s => s >= cut);
      return {trains: trainList, stations: stationList};
    },
    stats: () => ({
      trains: trains.size, stations: stations.size, disruptions, riders,
      eps: Math.round(stamps.length / (EPS_WINDOW / 1000)), connected,
    }),
  };
}
