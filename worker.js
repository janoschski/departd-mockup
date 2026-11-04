// departd Mockup auf Cloudflare Workers: statische Seite + ein Durable Object pro Projekt ("space").
// Jeder mit dem Link sieht live mit; Bearbeiten nach Passwort (Secret PASSWORD, Standard: departd).
import seed from './seed-docs.json';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') return new Response('Expected websocket', { status: 426 });
      const name = (url.searchParams.get('space') || 'kaufland-urs').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 40) || 'kaufland-urs';
      return env.SPACE.get(env.SPACE.idFromName(name)).fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};

const VALID = new Set(['o', 'rooms', 'meta', 'vmeta', 'vsnap']);
const validPath = p => typeof p === 'string' && /^[A-Za-z0-9_-]+\/[A-Za-z0-9_.-]+$/.test(p) && VALID.has(p.split('/')[0]);

export class Space {
  constructor(ctx, env) {
    this.ctx = ctx; this.env = env; this.docs = null; this.sessions = new Map();
    this.ready = ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.list();
      if (stored.size) this.docs = Object.fromEntries(stored);
      else {
        this.docs = { ...seed };
        const entries = Object.entries(this.docs);
        for (let i = 0; i < entries.length; i += 100) await ctx.storage.put(Object.fromEntries(entries.slice(i, i + 100)));
      }
    });
  }
  async fetch(request) {
    await this.ready;
    const pair = new WebSocketPair(); const [client, server] = Object.values(pair);
    server.accept();
    const s = { id: crypto.randomUUID().slice(0, 8), canWrite: false, state: null };
    this.sessions.set(server, s);
    const send = (ws, m) => { try { ws.send(JSON.stringify(m)); } catch (e) {} };
    const broadcast = (m, except) => { for (const ws of this.sessions.keys()) if (ws !== except) send(ws, m); };
    const pushPeers = () => broadcast({ t: 'peers', peers: [...this.sessions.values()].filter(x => x.state).map(x => ({ id: x.id, state: x.state })) });
    const password = this.env.PASSWORD || 'departd';
    server.addEventListener('message', async ev => {
      let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      if (m.t === 'hello') { send(server, { t: 'init', id: s.id, docs: this.docs, canWrite: s.canWrite }); pushPeers(); }
      else if (m.t === 'auth') { s.canWrite = typeof m.pw === 'string' && m.pw === password; send(server, { t: 'auth', ok: s.canWrite }); }
      else if (m.t === 'presence') { if (m.state && typeof m.state === 'object') { s.state = m.state; pushPeers(); } }
      else if ((m.t === 'set' || m.t === 'update' || m.t === 'del') && s.canWrite && validPath(m.path)) {
        if (m.t === 'del') { delete this.docs[m.path]; await this.ctx.storage.delete(m.path); }
        else if (m.data && typeof m.data === 'object') { const d = m.t === 'set' ? m.data : { ...(this.docs[m.path] || {}), ...m.data }; this.docs[m.path] = d; await this.ctx.storage.put(m.path, d); }
        else return;
        broadcast({ t: 'doc', path: m.path, data: this.docs[m.path] === undefined ? null : this.docs[m.path] }, server);
      }
    });
    const bye = () => { this.sessions.delete(server); pushPeers(); };
    server.addEventListener('close', bye); server.addEventListener('error', bye);
    return new Response(null, { status: 101, webSocket: client });
  }
}
