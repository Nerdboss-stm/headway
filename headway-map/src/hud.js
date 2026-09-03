// Top-left bar and the full-screen QR overlay. Numbers change instantly.
import QRCode from 'qrcode';

const ROWS = [
  ['trains', 'TRAINS TRACKED'],
  ['stations', 'STATIONS TRACKED'],
  ['disruptions', 'DISRUPTIONS DETECTED'],
  ['riders', 'RIDERS AFFECTED'],
  ['eps', 'EVENTS/SEC'],
];

export function createHud(root) {
  root.innerHTML = `
    <div class="hud-top"><div class="wordmark">HEADWAY</div><div class="mode" id="mode">LIVE</div></div>
    <div class="rows">${ROWS.map(([k, label]) =>
      `<div class="row"><span class="label">${label}</span><span class="value" data-k="${k}">0</span></div>`
    ).join('')}</div>
    <div class="audio-hint" id="audio-hint">CLICK ONCE TO ENABLE AUDIO</div>`;

  const cells = Object.fromEntries(ROWS.map(([k]) => [k, root.querySelector(`[data-k="${k}"]`)]));
  const modeEl = root.querySelector('#mode');
  const hintEl = root.querySelector('#audio-hint');
  const shown = {};
  const fmt = new Intl.NumberFormat('en-US');

  return {
    render(s) {
      for (const [k] of ROWS) {
        const v = fmt.format(s[k] ?? 0);
        if (shown[k] !== v) { cells[k].textContent = v; shown[k] = v; }
      }
    },
    setMode(replay) {
      modeEl.textContent = replay ? 'REPLAY 20×' : 'LIVE';
      modeEl.classList.toggle('replay', replay);
    },
    audioReady() { hintEl.remove(); },
  };
}

export function createQr(root, url) {
  let open = false, drawn = false;
  root.innerHTML = `<canvas id="qr-canvas"></canvas><div class="qr-text">Live now. Open it.</div>`;
  const canvas = root.querySelector('#qr-canvas');

  return {
    toggle() {
      open = !open;
      root.style.display = open ? 'flex' : 'none';
      if (open && !drawn) {
        drawn = true;
        // margin 4 is the spec quiet zone; anything less scans badly across a room
        QRCode.toCanvas(canvas, url, {width: 620, margin: 4, color: {dark: '#000000', light: '#FFFFFF'}})
          .catch(err => console.error('qr', err));
      }
    },
  };
}
