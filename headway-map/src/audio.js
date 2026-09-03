// Two-tone chime for critical events. Silent until the operator clicks once.
export function createAudio(onEnable) {
  let ctx = null;

  addEventListener('click', () => {
    if (ctx) return;
    ctx = new (window.AudioContext ?? window.webkitAudioContext)();
    ctx.resume();
    onEnable?.();
  });

  // 10ms ramps only: without them the oscillator clicks. Not a fade.
  function note(freq, at, dur) {
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.value = freq;
    gain.gain.setValueAtTime(0.0001, at);
    gain.gain.exponentialRampToValueAtTime(0.22, at + 0.01);
    gain.gain.setValueAtTime(0.22, at + dur - 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, at + dur);
    osc.connect(gain).connect(ctx.destination);
    osc.start(at);
    osc.stop(at + dur + 0.02);
  }

  return {
    get enabled() { return ctx != null; },
    chime() {
      if (!ctx) return;
      const t = ctx.currentTime + 0.01;
      note(440, t, 0.15);
      note(330, t + 0.15, 0.15);
    },
  };
}
