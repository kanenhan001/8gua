// 8gua API —— 卜卦 AI 解卦后端
// 职责：
//   1. POST /api/gua/explain  接收前端的问题 + 卦象信息，落库 SQLite，
//      然后代理转发 AI 大模型（当前为阿里通义千问，API Key 只存在服务端），SSE 流式透传给前端
//   2. GET  /api/health       健康检查
// 零 npm 依赖：http + node:sqlite + 内置 fetch（Node >= 22）
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---- 简易 .env 加载（无 dotenv 依赖）----
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const PORT = Number(process.env.PORT || 8123);
// 通用 OpenAI 兼容接口配置（阿里百炼 DashScope compatible-mode / 智谱 / DeepSeek 均可）
// 优先级：AI_* > DASHSCOPE_* > ZHIPU_*
const AI_BASE_URL = process.env.AI_BASE_URL || 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions';
const AI_API_KEY = process.env.AI_API_KEY || process.env.DASHSCOPE_API_KEY || process.env.ZHIPU_API_KEY || '';
const AI_MODEL = process.env.AI_MODEL || process.env.DASHSCOPE_MODEL || process.env.ZHIPU_MODEL || 'qwen-plus';
const DB_PATH = path.join(__dirname, 'data', '8gua.db');

// ---- SQLite ----
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
const db = new DatabaseSync(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS readings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT DEFAULT (datetime('now', 'localtime')),
    ip           TEXT,
    user_agent   TEXT,
    city         TEXT,
    question     TEXT,
    gua_name     TEXT,
    changing_yao TEXT,
    hexagram     TEXT,
    model        TEXT,
    ai_result    TEXT,
    ai_status    TEXT DEFAULT 'pending',
    ai_error     TEXT
  );
`);

function insertReading(r) {
  const stmt = db.prepare(`
    INSERT INTO readings (ip, user_agent, city, question, gua_name, changing_yao, hexagram, model, ai_status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
  `);
  const info = stmt.run(r.ip, r.userAgent, r.city, r.question, r.guaName, r.changingYao, r.hexagram, AI_MODEL);
  return Number(info.lastInsertRowid);
}

function finishReading(id, fields) {
  const stmt = db.prepare(`UPDATE readings SET ai_status = ?, ai_result = ?, ai_error = ? WHERE id = ?`);
  stmt.run(fields.status, fields.result || '', fields.error || '', id);
}

// ---- CORS ----
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}

function json(res, code, obj) {
  setCors(res);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

function readBody(req, limit = 20 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > limit) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (e) { reject(new Error('invalid json')); }
    });
    req.on('error', reject);
  });
}

function sseWrite(res, payload) {
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

// ---- 核心：解卦代理 ----
async function handleExplain(req, res) {
  let body;
  try { body = await readBody(req); }
  catch (e) { return json(res, 400, { error: { code: 'bad_request', message: e.message } }); }

  const question = String(body.question || '').slice(0, 500);
  const guaName = String(body.guaName || '').slice(0, 100);
  const changingYao = String(body.changingYao || '').slice(0, 50);
  const hexagram = String(body.hexagram || '').slice(0, 200);
  const city = String(body.city || '').slice(0, 100);
  const ip = (req.headers['x-real-ip'] || req.socket.remoteAddress || '').toString();
  const userAgent = (req.headers['user-agent'] || '').toString().slice(0, 300);

  if (!guaName) return json(res, 400, { error: { code: 'bad_request', message: '缺少卦名' } });
  if (!AI_API_KEY) return json(res, 500, { error: { code: 'no_key', message: '服务端未配置 AI Key' } });

  // 用户信息 + 占卜内容 + 卦象 先落库
  const rowId = insertReading({ ip, userAgent, city, question, guaName, changingYao, hexagram });

  // CORS + SSE 响应头
  setCors(res);
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  const prompt = `${guaName} ${changingYao}，请结合所提问题：${question}进行卦象解读，同时请尽量减少AI味儿，尽量人性化、生活化、口语化。`;

  const abort = new AbortController();
  req.on('close', () => { if (!res.writableEnded) abort.abort(); });

  let full = '';
  try {
    const upstream = await fetch(AI_BASE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${AI_API_KEY}`,
      },
      body: JSON.stringify({
        model: AI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        stream: true,
      }),
      signal: abort.signal,
    });

    if (!upstream.ok) {
      let code = String(upstream.status);
      let msg = `HTTP ${upstream.status}`;
      try {
        const errJson = JSON.parse(await upstream.text());
        if (errJson && errJson.error) { code = errJson.error.code || code; msg = errJson.error.message || msg; }
      } catch (_) { /* 忽略 */ }
      finishReading(rowId, { status: 'error', error: `${code} ${msg}` });
      sseWrite(res, { error: { code, message: msg } });
      res.write('data: [DONE]\n\n');
      return res.end();
    }

    // 流式透传：解析上游 SSE，聚合内容，原样下发
    const reader = upstream.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let done = false;
    while (!done) {
      const { value, done: readerDone } = await reader.read();
      if (readerDone) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop() || '';
      for (const raw of lines) {
        const line = raw.trim();
        if (!line.startsWith('data:')) continue;
        const data = line.slice(5).trim();
        if (data === '[DONE]') { done = true; break; }
        try {
          const j = JSON.parse(data);
          const delta = j.choices && j.choices[0] && j.choices[0].delta && j.choices[0].delta.content;
          if (delta) full += delta;
        } catch (_) { /* 非 JSON 行跳过 */ }
        res.write(`${raw.trimEnd()}\n\n`); // 原样透传给前端
      }
    }
    if (!res.writableEnded) {
      if (!done) res.write('data: [DONE]\n\n');
      res.end();
    }
    finishReading(rowId, { status: full ? 'done' : 'empty', result: full });
  } catch (err) {
    const aborted = err.name === 'AbortError';
    finishReading(rowId, aborted
      ? { status: 'aborted', result: full, error: 'client closed' }
      : { status: 'error', result: full, error: String(err && err.message || err) });
    if (!res.writableEnded) {
      if (!aborted) sseWrite(res, { error: { code: 'upstream_error', message: String(err && err.message || err) } });
      try { res.write('data: [DONE]\n\n'); res.end(); } catch (_) { /* ignore */ }
    }
  }
}

const server = http.createServer(async (req, res) => {
  const url = (req.url || '').split('?')[0];
  if (req.method === 'OPTIONS') { setCors(res); res.writeHead(204); return res.end(); }

  if (req.method === 'GET' && url === '/api/health') {
    return json(res, 200, { ok: true, model: AI_MODEL, time: new Date().toISOString() });
  }
  if (req.method === 'POST' && url === '/api/gua/explain') {
    return handleExplain(req, res);
  }
  return json(res, 404, { error: { code: 'not_found', message: 'unknown path' } });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[8gua-api] listening on 127.0.0.1:${PORT}, model=${AI_MODEL}, db=${DB_PATH}`);
});
