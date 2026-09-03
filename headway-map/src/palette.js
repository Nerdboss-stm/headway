// Official MTA route colors. Nothing else on screen is colored.
export const BLACK = '#000000';
export const WHITE = '#FFFFFF';
export const GREY = '#808183';   // the one grey (MTA shuttle grey)
export const AMBER = '#FF6319';  // MTA orange, B D F M
export const RED = '#EE352E';    // MTA red, 1 2 3

const ROUTE = {
  1: '#EE352E', 2: '#EE352E', 3: '#EE352E',
  4: '#00933C', 5: '#00933C', 6: '#00933C', '5X': '#00933C', '6X': '#00933C',
  7: '#B933AD', '7X': '#B933AD',
  A: '#0039A6', C: '#0039A6', E: '#0039A6',
  B: '#FF6319', D: '#FF6319', F: '#FF6319', M: '#FF6319', FX: '#FF6319',
  G: '#6CBE45',
  J: '#996633', Z: '#996633',
  L: '#A7A9AC',
  N: '#FCCC0A', Q: '#FCCC0A', R: '#FCCC0A', W: '#FCCC0A',
  S: '#808183', GS: '#808183', FS: '#808183', H: '#808183',
  SI: '#0039A6', SIR: '#0039A6',
};

export const routeColor = r => ROUTE[String(r ?? '').toUpperCase()] ?? GREY;

// MTA bullet rule: black type only on the yellow N Q R W bullets.
export const bulletText = r => (routeColor(r) === '#FCCC0A' ? BLACK : WHITE);

// Shuttles all read as S; express diamonds carry the trunk number.
export const bulletLabel = r => {
  const k = String(r ?? '').toUpperCase();
  if (k === 'FS' || k === 'GS' || k === 'H') return 'S';
  if (k === 'SI' || k === 'SIR') return 'S';
  return k.replace('X', '') || '?';
};

export const rgb = hex => [
  parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16),
];
