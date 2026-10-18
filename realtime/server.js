// Code Battle realtime server: lobby presence over WebSocket.
// Run: npm install && npm start   (env: PORT, ALLOWED_ORIGIN)
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || ""; // e.g. https://codebattle.app
const rooms = new Map(); // room -> Map(id -> { ws, name })

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("Code Battle realtime: ok");
});

const wss = new WebSocketServer({
  server,
  maxPayload: 1024, // tiny messages only
  verifyClient: ({ origin }) => !ALLOWED_ORIGIN || origin === ALLOWED_ORIGIN,
});

function broadcast(room) {
  const r = rooms.get(room);
  if (!r) return;
  const users = Object.fromEntries([...r].map(([id, u]) => [id, u.name]));
  const msg = JSON.stringify({ t: "presence", room, users });
  for (const { ws } of r.values()) if (ws.readyState === 1) ws.send(msg);
}

wss.on("connection", (ws, req) => {
  const room = (new URL(req.url, "http://x").searchParams.get("room") || "lobby").slice(0, 40);
  let id = null;
  let lastMsg = 0;
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    const now = Date.now();
    if (now - lastMsg < 200) return; // basic rate limit
    lastMsg = now;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "join" && typeof m.name === "string" && m.id) {
      // TODO: verify a signed token here and take the name from it, not from the client.
      id = String(m.id).slice(0, 16);
      if (!rooms.has(room)) rooms.set(room, new Map());
      rooms.get(room).set(id, { ws, name: m.name.slice(0, 40) });
      broadcast(room);
    }
  });

  ws.on("close", () => {
    const r = rooms.get(room);
    if (!r || !id) return;
    r.delete(id);
    if (r.size) broadcast(room); else rooms.delete(room);
  });
});

// Drop dead connections every 30 seconds.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log("Realtime server on :" + PORT));