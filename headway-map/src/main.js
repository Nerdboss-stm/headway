import {Map as MapLibreMap} from 'maplibre-gl';
import {Deck} from '@deck.gl/core';
import {GeoJsonLayer, IconLayer, PathLayer, ScatterplotLayer, TextLayer} from '@deck.gl/layers';
import {routeColor, bulletText, bulletLabel, rgb, AMBER, RED, GREY, WHITE, BLACK} from './palette.js';
import {createStore} from './store.js';
import {createCards} from './cards.js';
import {createHud, createQr} from './hud.js';
import {createAudio} from './audio.js';
import 'maplibre-gl/dist/maplibre-gl.css';
import './style.css';

const params = new URLSearchParams(location.search);
const LIGHT = params.get('basemap') === 'light';
const BG = LIGHT ? WHITE : BLACK;
const SOCKET = params.get('ws') ?? 'ws://localhost:8787';
// Fixed frame: lower Manhattan through north Brooklyn. Bearing 29 stands Manhattan
// upright, the way the official map does.
const VIEW = {center: [-73.9845, 40.7175], zoom: 13.7, pitch: 30, bearing: 29};

const AMBER_RGB = rgb(AMBER), RED_RGB = rgb(RED), GREY_RGB = rgb(GREY), WHITE_RGB = rgb(WHITE);
const DELAY_RING = 180, DELAY_SEVERE = 420;

document.body.classList.toggle('light', LIGHT);

const map = new MapLibreMap({
  container: 'map',
  style: {version: 8, sources: {}, layers: [{id: 'bg', type: 'background', paint: {'background-color': BG}}]},
  ...VIEW,
  attributionControl: false,
  dragRotate: false,
  pitchWithRotate: false,
  touchPitch: false,
});
map.touchZoomRotate.disableRotation();

const deck = new Deck({
  canvas: 'deck',
  controller: false,
  initialViewState: {longitude: VIEW.center[0], latitude: VIEW.center[1], ...VIEW},
  parameters: {depthTest: false},
});

const audio = createAudio(() => hud.audioReady());
const hud = createHud(document.getElementById('hud'));
const cards = createCards(document.getElementById('cards'), audio);
const qr = createQr(document.getElementById('qr'), params.get('qr') ?? location.href);
const store = createStore(SOCKET, ev => cards.push(ev));

let replay = false;
hud.setMode(replay);
addEventListener('keydown', e => {
  const k = e.key.toLowerCase();
  if (k === 'r') hud.setMode((replay = !replay));
  else if (k === 'q') qr.toggle();
});

// A 16px white square, tinted per-station by getColor.
const squareAtlas = (() => {
  const c = document.createElement('canvas');
  c.width = c.height = 16;
  const g = c.getContext('2d');
  g.fillStyle = '#ffffff';
  g.fillRect(0, 0, 16, 16);
  return c;
})();
const SQUARE = {sq: {x: 0, y: 0, width: 16, height: 16, mask: true}};
const bikeSize = b => (b <= 0 ? 2 : b < 5 ? 3 : b < 15 ? 4 : 5.5);

let lines = null;
fetch('subway.geojson')
  .then(r => r.json())
  .then(geo => {
    lines = new GeoJsonLayer({
      id: 'subway',
      data: geo,
      stroked: true,
      filled: false,
      lineWidthUnits: 'pixels',
      getLineWidth: 4,
      lineWidthMinPixels: 4,
      getLineColor: f => rgb(routeColor(f.properties.route_id)),
      parameters: {depthTest: false},
    });
  })
  .catch(err => console.error('subway.geojson', err));

let tick = 0;

function render(now) {
  const {trains, stations} = store.tick(now);
  const blinkOn = Math.floor(now / 500) % 2 === 0;
  const severe = trains.filter(t => t.delay_sec > DELAY_SEVERE);
  const ringed = trains.filter(t => t.delay_sec > DELAY_RING && t.delay_sec <= DELAY_SEVERE);
  tick++;

  deck.setProps({
    viewState: (() => {
      const c = map.getCenter();
      return {longitude: c.lng, latitude: c.lat, zoom: map.getZoom(), pitch: map.getPitch(), bearing: map.getBearing()};
    })(),
    layers: [
      lines,
      new PathLayer({
        id: 'severe-segments',
        data: severe,
        visible: blinkOn,
        getPath: d => [d.from, d.to],
        getColor: RED_RGB,
        widthUnits: 'pixels',
        getWidth: 6,
        updateTriggers: {getPath: tick},
        parameters: {depthTest: false},
      }),
      new IconLayer({
        id: 'citibike',
        data: stations,
        iconAtlas: squareAtlas,
        iconMapping: SQUARE,
        getIcon: () => 'sq',
        getPosition: d => d.pos,
        getSize: d => bikeSize(d.bikes),
        sizeUnits: 'pixels',
        getColor: GREY_RGB,
        parameters: {depthTest: false},
      }),
      new ScatterplotLayer({
        id: 'delay-rings',
        data: ringed,
        radiusUnits: 'pixels',
        getRadius: 14,
        filled: false,
        stroked: true,
        lineWidthUnits: 'pixels',
        getLineWidth: 2,
        getLineColor: AMBER_RGB,
        getPosition: d => d.pos,
        updateTriggers: {getPosition: tick},
        parameters: {depthTest: false},
      }),
      new ScatterplotLayer({
        id: 'trains',
        data: trains,
        radiusUnits: 'pixels',
        getRadius: 10,
        getPosition: d => d.pos,
        getFillColor: d => (d.delay_sec > DELAY_SEVERE ? RED_RGB : rgb(routeColor(d.route_id))),
        updateTriggers: {getPosition: tick, getFillColor: tick},
        parameters: {depthTest: false},
      }),
      new TextLayer({
        id: 'bullets',
        data: trains,
        getPosition: d => d.pos,
        getText: d => bulletLabel(d.route_id),
        getColor: d => (d.delay_sec > DELAY_SEVERE ? WHITE_RGB : rgb(bulletText(d.route_id))),
        getSize: 11,
        sizeUnits: 'pixels',
        fontFamily: '"Helvetica Neue", Helvetica, Arial, sans-serif',
        fontWeight: 700,
        characterSet: '0123456789ABCDEFGHIJLMNQRSWXZ?',
        getTextAnchor: 'middle',
        getAlignmentBaseline: 'center',
        updateTriggers: {getPosition: tick, getColor: tick},
        parameters: {depthTest: false},
      }),
    ],
  });

  cards.tick(now, map);
  hud.render(store.stats());
}

function frame(now) {
  render(now);
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
