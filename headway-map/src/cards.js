// Countdown-clock cards anchored to a coordinate. Three at a time, 20s each,
// hard cut in and out. Everything else queues.
import {routeColor, bulletText, bulletLabel} from './palette.js';

const HOLD_MS = 20000;
const MAX_VISIBLE = 3;
const BOX_W = 340;   // border-box width, matches .card .box in the stylesheet
const HUD_W = 372;   // the HUD block the cards must stay clear of
const HUD_H = 300;
const PAD = 12;
const GAP = 28;      // anchor dot to box

export function createCards(root, audio) {
  const live = [];
  const queue = [];

  function mount(ev, now) {
    const el = document.createElement('div');
    el.className = 'card';
    const color = routeColor(ev.route_id);
    el.innerHTML = `
      <div class="dot"></div>
      <div class="leader"></div>
      <div class="box">
        <div class="bullet" style="background:${color};color:${bulletText(ev.route_id)}">
          ${bulletLabel(ev.route_id)}
        </div>
        <div class="text">
          <div class="title"></div>
          <div class="body"></div>
        </div>
      </div>`;
    el.querySelector('.title').textContent = ev.title;
    el.querySelector('.body').textContent = ev.body;
    root.appendChild(el);
    return {ev, el, box: el.querySelector('.box'), until: now + HOLD_MS};
  }

  return {
    push(ev) { queue.push(ev); },

    tick(now, map) {
      for (let i = live.length - 1; i >= 0; i--) {
        if (now >= live[i].until) { live[i].el.remove(); live.splice(i, 1); }
      }
      while (live.length < MAX_VISIBLE && queue.length) {
        const ev = queue.shift();
        live.push(mount(ev, now));
        if (ev.severity === 'critical') audio.chime();
      }

      const placed = [];
      for (const c of live) {
        const p = map.project([c.ev.lon, c.ev.lat]);
        const h = c.box.offsetHeight || 92;
        const wantX = p.x - 14;
        let below = p.y - GAP - h < PAD;
        let x = wantX;
        let y = below ? p.y + GAP : p.y - GAP - h;

        x = Math.max(PAD, Math.min(x, innerWidth - BOX_W - PAD));
        y = Math.max(PAD, Math.min(y, innerHeight - h - PAD));
        if (y < HUD_H && x < HUD_W) x = HUD_W;
        for (const q of placed) {                       // never stack two cards on top of each other
          if (x < q.x + BOX_W && x + BOX_W > q.x && y < q.y + q.h && y + h > q.y) {
            y = Math.min(q.y + q.h + 10, innerHeight - h - PAD);
          }
        }
        placed.push({x, y, h});

        // The dot always marks the true location; the leader only claims a
        // connection when the box actually sits under or over it.
        const attached = Math.abs(x - wantX) < 1 && Math.abs(y - (below ? p.y + GAP : p.y - GAP - h)) < 1;
        c.el.classList.toggle('below', below);
        c.el.classList.toggle('detached', !attached);
        c.el.style.transform = `translate(${Math.round(p.x)}px, ${Math.round(p.y)}px)`;
        c.box.style.left = `${Math.round(x - p.x)}px`;
        c.box.style.top = `${Math.round(y - p.y)}px`;
      }
    },
  };
}
