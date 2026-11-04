// Eigenhosting-Brücke: stellt window.claude.use('db'|'room'|'user'|'downloads') über einen WebSocket-Server bereit.
(function () {
  const space = (new URLSearchParams(location.search).get('space') || 'kaufland-urs').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 40) || 'kaufland-urs';
  const wsUrl = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws?space=' + encodeURIComponent(space);
  const docs = new Map(); // path -> data
  const listeners = []; // {kind:'col'|'doc', path, orderBy, dir, cb, seen:Map}
  let ws, canWrite = false, ready = false, readyWaiters = [], peers = [], myId = null, peerCbs = [], lastPresence = {}, pendingAuth = null;
  const me = { name: '' };
  function conn() {
    ws = new WebSocket(wsUrl);
    ws.onopen = () => { send({ t: 'hello' }); if (lastPresence && Object.keys(lastPresence).length) send({ t: 'presence', state: lastPresence }); if (window.__pw) send({ t: 'auth', pw: window.__pw }); };
    ws.onmessage = ev => {
      let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      if (m.t === 'init') { docs.clear(); for (const [p, d] of Object.entries(m.docs)) docs.set(p, d); myId = m.id; canWrite = !!m.canWrite; ready = true; readyWaiters.splice(0).forEach(f => f()); notifyAll(); }
      else if (m.t === 'doc') { if (m.data == null) docs.delete(m.path); else docs.set(m.path, m.data); notifyPath(m.path); }
      else if (m.t === 'peers') { peers = m.peers; peerCbs.forEach(f => f({ peers: peers.map(p => ({ peer: p.id, isMe: p.id === myId, presence: p.state })) })); }
      else if (m.t === 'auth') { canWrite = !!m.ok; if (pendingAuth) { pendingAuth(canWrite); pendingAuth = null; } }
    };
    ws.onclose = () => { setTimeout(conn, 1500); };
  }
  function send(o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
  const split = p => { const i = p.indexOf('/'); return [p.slice(0, i), p.slice(i + 1)]; };
  const mkSnap = (l) => {
    if (l.kind === 'doc') { const d = docs.get(l.path); return { exists: d !== undefined, data: () => d, id: split(l.path)[1] }; }
    const col = l.path + '/'; let arr = [];
    for (const [p, d] of docs) if (p.startsWith(col) && !p.slice(col.length).includes('/')) arr.push({ id: p.slice(col.length), data: () => d, _p: p, _d: d });
    if (l.orderBy) arr.sort((a, b) => { const x = a._d[l.orderBy], y = b._d[l.orderBy]; return (x > y ? 1 : x < y ? -1 : 0) * (l.dir === 'desc' ? -1 : 1); });
    const changes = [];
    const seen = new Map(arr.map(a => [a._p, JSON.stringify(a._d)]));
    for (const a of arr) { const prev = l.seen.get(a._p); if (prev === undefined) changes.push({ type: 'added', doc: a }); else if (prev !== seen.get(a._p)) changes.push({ type: 'modified', doc: a }); }
    for (const [p] of l.seen) if (!seen.has(p)) changes.push({ type: 'removed', doc: { id: p.slice(col.length), data: () => ({}) } });
    l.seen = seen;
    return { docs: arr, docChanges: () => changes, empty: !arr.length };
  };
  function fire(l) { try { l.cb(mkSnap(l)); } catch (e) { console.error(e); } }
  const notifyAll = () => listeners.forEach(fire);
  function notifyPath(p) { for (const l of listeners) { if (l.kind === 'doc' ? l.path === p : p.startsWith(l.path + '/')) fire(l); } }
  const norm = d => JSON.parse(JSON.stringify(d));
  const docRef = path => ({
    path,
    get: async () => { await waitReady(); const d = docs.get(path); return { exists: d !== undefined, data: () => d, id: split(path)[1] }; },
    set: async d => { chk(); d = norm(d); docs.set(path, d); send({ t: 'set', path, data: d }); },
    update: async d => { chk(); d = norm(d); docs.set(path, { ...(docs.get(path) || {}), ...d }); send({ t: 'update', path, data: d }); },
    delete: async () => { chk(); docs.delete(path); send({ t: 'del', path }); },
    onSnapshot: (cb) => { const l = { kind: 'doc', path, cb, seen: new Map() }; listeners.push(l); waitReady().then(() => fire(l)); return () => { const i = listeners.indexOf(l); if (i >= 0) listeners.splice(i, 1); }; }
  });
  const colRef = (path, o = {}) => ({
    doc: id => docRef(path + '/' + id),
    orderBy: (f, dir) => colRef(path, { orderBy: f, dir }),
    onSnapshot: cb => { const l = { kind: 'col', path, orderBy: o.orderBy, dir: o.dir, cb, seen: new Map() }; listeners.push(l); waitReady().then(() => fire(l)); return () => { const i = listeners.indexOf(l); if (i >= 0) listeners.splice(i, 1); }; }
  });
  function chk() { if (!canWrite) { const e = new Error('Nur Ansicht'); e.code = 'invalid_argument'; throw e; } }
  const waitReady = () => ready ? Promise.resolve() : new Promise(r => readyWaiters.push(r));
  const caps = {
    db: { doc: docRef, collection: p => colRef(p) },
    room: { presence: async s => { lastPresence = s; send({ t: 'presence', state: s }); }, onPeers: f => { peerCbs.push(f); f({ peers: peers.map(p => ({ peer: p.id, isMe: p.id === myId, presence: p.state })) }); } },
    user: {
      me: async () => ({ name: me.name }), can: async () => { await waitReady(); return canWrite; }, id: async () => myId, isOwner: () => canWrite, canEdit: () => canWrite,
      unlock: pw => new Promise(res => { window.__pw = pw; pendingAuth = res; send({ t: 'auth', pw }); setTimeout(() => { if (pendingAuth) { pendingAuth(false); pendingAuth = null; } }, 6000); }),
      canWrite: () => canWrite
    },
    downloads: { save: async ({ filename, data }) => { const b = data instanceof Blob ? data : new Blob([data]); const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = filename; document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000); } }
  };
  window.claude = { use: async n => caps[n] || null };
  window.__selfhost = true;
  conn();
})();
