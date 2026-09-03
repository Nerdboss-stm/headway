// One-time: MTA static GTFS shapes.txt -> public/subway.geojson,
// Citi Bike GBFS -> data/citibike.json (positions for the fake feed).
import {execFileSync} from 'node:child_process';
import {mkdirSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const GTFS = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip';
const GBFS = 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json';
const ROOT = new URL('..', import.meta.url).pathname;

function csv(text) {
  const lines = text.split('\n').filter(Boolean);
  const head = lines[0].trim().replace(/^﻿/, '').split(',');
  return lines.slice(1).map(line => {
    const out = [];
    let cur = '', q = false;
    for (const ch of line) {
      if (ch === '"') q = !q;
      else if (ch === ',' && !q) { out.push(cur); cur = ''; }
      else cur += ch;
    }
    out.push(cur);
    return Object.fromEntries(head.map((h, i) => [h, (out[i] ?? '').trim()]));
  });
}

const zip = join(tmpdir(), 'gtfs_subway.zip');
console.log('downloading', GTFS);
const buf = Buffer.from(await (await fetch(GTFS)).arrayBuffer());
writeFileSync(zip, buf);
const read = name => execFileSync('unzip', ['-p', zip, name], {maxBuffer: 1 << 28}).toString();

// shape_id -> route_id
const shapeRoute = new Map();
for (const t of csv(read('trips.txt'))) if (t.shape_id) shapeRoute.set(t.shape_id, t.route_id);

// shape_id -> ordered coordinates
const shapes = new Map();
for (const r of csv(read('shapes.txt'))) {
  if (!shapes.has(r.shape_id)) shapes.set(r.shape_id, []);
  shapes.get(r.shape_id).push([+r.shape_pt_lon, +r.shape_pt_lat, +r.shape_pt_sequence]);
}

const seen = new Set();
const features = [];
for (const [id, pts] of shapes) {
  const route = shapeRoute.get(id);
  if (!route || pts.length < 2) continue;
  const coords = pts.sort((a, b) => a[2] - b[2]).map(p => [+p[0].toFixed(5), +p[1].toFixed(5)]);
  const key = route + '|' + coords.length + '|' + coords[0] + '|' + coords[coords.length - 1];
  if (seen.has(key)) continue;           // identical duplicate shapes add nothing
  seen.add(key);
  features.push({type: 'Feature', properties: {route_id: route}, geometry: {type: 'LineString', coordinates: coords}});
}
mkdirSync(join(ROOT, 'public'), {recursive: true});
writeFileSync(join(ROOT, 'public/subway.geojson'), JSON.stringify({type: 'FeatureCollection', features}));
console.log('subway.geojson:', features.length, 'shapes,',
  new Set(features.map(f => f.properties.route_id)).size, 'routes');

// Citi Bike station positions, for the fake feed only (the app gets stations over the socket).
mkdirSync(join(ROOT, 'data'), {recursive: true});
try {
  const gbfs = await (await fetch(GBFS)).json();
  const st = (gbfs.data?.stations ?? []).map(s => ({
    id: s.station_id, lat: s.lat, lon: s.lon, capacity: s.capacity ?? 20,
  })).filter(s => s.lat && s.lon);
  writeFileSync(join(ROOT, 'data/citibike.json'), JSON.stringify(st));
  console.log('citibike.json:', st.length, 'stations');
} catch (err) {
  console.warn('citibike fetch failed, fake feed will scatter stations:', err.message);
  writeFileSync(join(ROOT, 'data/citibike.json'), '[]');
}
