/**
 * 凡骨对战 · 联机服务端（主机-客户端模式）
 * 职责：静态托管 index.html + WebSocket 房间转发（建房/加房/指令透传/断线提示）
 * 用法：node server.js（默认端口 3000，可用环境变量 PORT 覆盖）
 * 零数据库、零配置；部署到任何 Node 环境（Replit / Railway / 腾讯云 / 本机）即可。
 */
const http = require('http');
const fs = require('fs');
const path = require('path');

/* ws 依赖自举：云平台只需上传 server.js + index.html 两个文件即可直接启动
   （首次启动若未安装 ws 会自动执行 npm install ws，之后正常复用） */
let WebSocketServer;
try {
  ({ WebSocketServer } = require('ws'));
} catch (e) {
  console.log('[setup] 未找到 ws 依赖，正在自动安装（npm install ws）…');
  try {
    require('child_process').execSync('npm install ws --no-fund --no-audit --loglevel=error', { stdio: 'inherit', timeout: 180000 });
  } catch (e2) {
    console.error('[setup] 自动安装 ws 失败：' + (e2 && e2.message));
    console.error('[setup] 请手动执行 npm install ws 后重新启动。');
    process.exit(1);
  }
  ({ WebSocketServer } = require('ws'));
}

const PORT = process.env.PORT || 3000;
const HTML_PATH = path.join(__dirname, 'index.html');

/* ---------- HTTP：静态托管单文件游戏 ---------- */
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(HTML_PATH, (err, data) => {
      if (err) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('index.html 不存在：请将 server.js 与 index.html 放在同一目录'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not Found');
});

/* ---------- WebSocket：房间转发 ---------- */
const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> { host, guest, hostName, guestName }

function genRoomId() {
  let id;
  do { id = String(Math.floor(1000 + Math.random() * 9000)); } while (rooms.has(id));
  return id;
}
function roomOf(ws) {
  for (const r of rooms.values()) if (r.host === ws || r.guest === ws) return r;
  return null;
}
function other(r, ws) { return (r.host === ws) ? r.guest : r.host; }
function send(ws, obj) { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) {} } }

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    switch (m.t) {
      case 'create': {
        const r = roomOf(ws);
        if (r) { send(ws, { t: 'error', msg: '已在房间中' }); break; }
        const id = genRoomId();
        rooms.set(id, { roomId: id, host: ws, guest: null, hostName: m.name || '玩家1', guestName: null });
        send(ws, { t: 'room', roomId: id, role: 'host' });
        break;
      }
      case 'join': {
        const r = rooms.get(String(m.roomId || '').trim());
        console.log('[ws] join', m.roomId, '->', !!r, r && r.guest ? 'full' : '');
        if (!r) { send(ws, { t: 'error', msg: '房间不存在，请检查房间号' }); break; }
        if (r.guest) { send(ws, { t: 'error', msg: '房间已满' }); break; }
        r.guest = ws;
        r.guestName = m.name || '玩家2';
        send(ws, { t: 'room', roomId: String(m.roomId).trim(), role: 'guest' });
        console.log('[ws] peer_join -> host', !!r.host);
        send(r.host, { t: 'peer_join', name: r.guestName });
        break;
      }
      case 'act': { // 任意一方发给另一方的指令
        const r = roomOf(ws);
        if (!r) break;
        send(other(r, ws), { t: 'act', data: m.data });
        break;
      }
      case 'ask': // host → guest：询问（掷骰/方向/融合/弃牌等）
      case 'host_state': { // host → guest：状态广播
        const r = roomOf(ws);
        if (!r || r.host !== ws) break;
        send(r.guest, m);
        break;
      }
      case 'answer': { // guest → host：询问答复
        const r = roomOf(ws);
        if (!r || r.guest !== ws) break;
        send(r.host, m);
        break;
      }
      case 'leave': {
        const r = roomOf(ws);
        if (!r) break;
        const o = other(r, ws);
        if (o) send(o, { t: 'peer_leave' });
        if (r.host === ws) { rooms.delete(r.roomId); }
        else { r.guest = null; }
        break;
      }
      default: break;
    }
  });

  ws.on('close', () => {
    const r = roomOf(ws);
    if (!r) return;
    const o = other(r, ws);
    if (o) send(o, { t: 'peer_leave' });
    if (r.host === ws) { rooms.delete(r.roomId); }
    else { r.guest = null; }
  });
});

/* 心跳：清理死连接 */
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);

server.listen(PORT, () => {
  console.log('凡骨对战 · 联机服务端已启动');
  console.log('  本机访问:  http://localhost:' + PORT);
  console.log('  局域网访问:  http://<本机IP>:' + PORT);
  console.log('  公网访问:  请将本服务部署到云平台后使用平台分配的域名');
});
