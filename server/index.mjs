// 8gua API —— 易经卦象 AI 解读后端
// 职责：
//   1. POST /api/gua/explain  接收前端的问题 + 卦象信息，落库 SQLite，
//      然后代理转发 AI 大模型（当前为阿里通义千问，API Key 只存在服务端），SSE 流式透传给前端
//   2. GET  /api/health       健康检查
// 零 npm 依赖：http + node:sqlite + 内置 fetch（Node >= 22）
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
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
// 兼容旧表：补充 style 列（记录用户选择的讲卦风格）
try { db.exec(`ALTER TABLE readings ADD COLUMN style TEXT`); } catch (_) { /* 列已存在 */ }

function insertReading(r) {
  const stmt = db.prepare(`
    INSERT INTO readings (ip, user_agent, city, question, gua_name, changing_yao, hexagram, model, ai_status, style)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
  `);
  const info = stmt.run(r.ip, r.userAgent, r.city, r.question, r.guaName, r.changingYao, r.hexagram, AI_MODEL, r.style || '');
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

// ---- 核心：讲卦（卦象解读）代理 ----
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
  const style = body.style === 'plain' ? 'plain' : 'master';

  if (!guaName) return json(res, 400, { error: { code: 'bad_request', message: '缺少卦名' } });
  if (!AI_API_KEY) return json(res, 500, { error: { code: 'no_key', message: '服务端未配置 AI Key' } });

  // 用户信息 + 所问之事 + 卦象 先落库
  const rowId = insertReading({ ip, userAgent, city, question, guaName, changingYao, hexagram, style });

  // CORS + SSE 响应头
  setCors(res);
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  // 讲卦风格提示词：master = 易学讲师（半文半白但易读），plain = 白话讲解（全白话）
  const STYLE_PROMPTS = {
    master: [
      '你是一位潜心研习《周易》多年的易学讲师，从事易经文化的讲解与教学，风格沉稳、专业、可信。',
      '解读要求：',
      '1. 释卦：简述该卦的上下卦构成与卦名含义；可以引用卦辞或变爻爻辞原文，但原文之后必须紧跟通俗的白话解释，让没有古文基础的读者也能看懂；',
      '2. 断事：把卦理与所问之事相结合，指出当前态势、趋势与关键变量，用语平实克制、有理有据；',
      '3. 建议：结合卦理给出审慎、可行的思路，说明取舍之理，不作绝对化推断。',
      '语言要求：庄重平实、条理清晰；整体以现代书面语为主，避免大段文言和生僻字；不用网络用语和卖萌表达；判断留有余地，不过度绝对化。',
      '篇幅 300～500 字，分「释卦」「断事」「建议」三个小节，无需客套开场白与结尾寒暄；本内容仅作易经文化学习与思考参考，不作命运预测。',
    ].join('\n'),
    plain: [
      '你是一位亲切、靠谱的易经文化讲解者，用现代大白话把卦象与卦理讲清楚，像一位懂《易经》的朋友在帮忙梳理思路，内容专业可信、不故弄玄虚。',
      '解读要求：',
      '1. 先给结论：直接告诉用户这一卦对其所问之事给出的思路倾向（如顺畅/有阻力/平稳/宜缓行等）；',
      '2. 再讲道理：用日常语言解释这一卦的含义（卦名象征什么、这一爻在提示什么），让完全不懂易经的人也能听懂，不要引用文言原文；',
      '3. 最后给建议：给出具体、可操作的建议，说清楚该做什么、需要注意什么。',
      '语言要求：自然口语但不过分活泼，不堆砌感叹词和卖萌语气词；判断留有余地，不绝对化；结合用户所问之事具体展开。',
      '篇幅 250～400 字，用短段落组织，无需客套开场白与结尾寒暄；本内容仅作易经文化学习与思考参考，不作命运预测。',
    ].join('\n'),
  };

  const systemPrompt = STYLE_PROMPTS[style];
  const userPrompt = `${guaName} ${changingYao}。所问之事：${question}。请按上述体例讲解此卦。`;

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
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt },
        ],
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

/* ================= 云端语音朗读（Edge TTS，内联勿拆文件） =================
 * 微信内置浏览器的原生 speechSynthesis 不可用（见健身指导专家项目的同款结论），
 * 改为服务端用 Edge Read Aloud 合成 mp3，前端 <audio> 播放，全浏览器通用。
 * 依赖 msedge-tts（软依赖：未安装时本接口返回 503，前端回落浏览器 TTS）。
 * Sec-MS-GEC v2 令牌算法完整移植自健身指导专家项目（rany2/edge-tts PR #492）。 */

let MsEdgeTTS = null;
let OUTPUT_FORMAT = null;
try {
  ({ MsEdgeTTS, OUTPUT_FORMAT } = await import('msedge-tts'));
} catch {
  console.warn('[tts] msedge-tts 未安装，语音朗读接口将返回 503');
}

/* ================= Sec-MS-GEC v2 令牌（内联，勿拆文件） =================
 * 部署白名单只收 index.mjs，令牌算法必须内联在本文件里。
 *
 * 背景：微软 2026 年 10 月变更了 Read Aloud 合成服务——且握手令牌强制要求
 * v2 格式（Sec-MS-GEC-Version 必须以 "2-" 开头）。msedge-tts@2.1.0 令牌仍是
 * v1，握手会被服务端静默拒绝（返回 200 而非 101），报
 * "Edge TTS WebSocket error: Unexpected server response: 200"。
 * 另外库 2.1.0 默认的新端点 api.msedgeservices.com 即使带正确 v2 令牌也会被
 * 拒（200），必须改回经典端点 speech.platform.bing.com/.../edge/v1。
 *
 * 本节把 rany2/edge-tts PR #492 的 v2 令牌算法（修改版 SHA-256 + CS64 双摘要，
 * 输出 "<16hex>-<64hex>"）移植到 JS，并覆盖 MsEdgeTTS.getSynthUrl。
 * 自验锚点（来自 PR #492，用 Edge 155 抓包逐字节核对）：
 *   generateSecMsGecV2(1791888000)
 *     === '492D7694A183A45E-56E821F95D4579C5B7C7E33DC7303FF379EC36D510213555BBBB9EB17EF15876'
 */
const GEC_WIN_EPOCH = 11644473600;
const GEC_TOKEN = '6A5AA1D4EAFF4E9FB37E23D68491D6F4';
const GEC_VERSION = '2-143.0.3650.75';

/* ---- 常量表（源自 edge-tts/src/edge_tts/drm.py，微软折叠版 SHA-256/CS64） ---- */
const GEC_SHA256_INITIAL = [
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
const GEC_SHA256_STATE = [
  0xfd9fbea2, 0x91243ce7, 0xbe499094, 0xcd2e1e0a,
  0x6c006f49, 0x04bd35ab, 0xfeb0a7e4, 0x03effbd2,
];
const GEC_SHA256_SCHEDULE = [
  0xeb24bf9e, 0x1558691f, 0xa0c09ee2, 0x0cfa32ba, 0xa622ee8c, 0xd1dd9491,
  0xc0dd25a0, 0xf9e70ab6, 0xf0c31573, 0x7058ce4e, 0x7188e606, 0x00e5ab33,
  0xd258b060, 0x72766564,
];
// 注意：K 表是标准 SHA-256 全部 64 项，漏一项（如 0xa81a664b）会导致轮 40 起全错
const GEC_SHA256_ROUND_CONSTANTS = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
  0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];
const GEC_SHA256_ADJUSTMENT = 0xc9dc06a7;
// BLOCK = ROUND 常量表，但第 14 位的 K 换成折叠调整值（首个块从轮 14 恢复时使用）
const GEC_SHA256_BLOCK_CONSTANTS = [
  ...GEC_SHA256_ROUND_CONSTANTS.slice(0, 14),
  GEC_SHA256_ADJUSTMENT,
  ...GEC_SHA256_ROUND_CONSTANTS.slice(15),
];
const GEC_SIGMA0_ADJUSTMENT = 0x059c0b80;
const GEC_SIGMA1_ADJUSTMENT = 0x000b92c0;

const GEC_CS64_COEFFICIENTS = [
  [0xe82e91b1, 0xb32700d0], [0x44de24c6, 0x88dff51d],
  [0x3beb65bc, 0x29514038], [0xc89c5933, 0xb328cbdf],
  [0x8d1ec211, 0xd405b4d5], [0x314ef5d5, 0x26e55f3c],
  [0xe92491b1, 0x607801c6], [0xf0a5b9d5, 0x82b2c1fb],
  [0x06064cf3, 0xfa51a3da], [0xda79fa60, 0x207cc464],
  [0xbed20560, 0x3c3c74f3], [0x89f9c9f8, 0xd2d14b61],
  [0x35fa38c6, 0xbb04a2c1], [0xcb49e3c6, 0xc5626167],
];
const GEC_CS64_FACTOR = 0x82893933;
const GEC_CS64_ADJUSTMENT0 = 0xf2f90b1a;
const GEC_CS64_ADJUSTMENT1 = 0x0b1a0000;

const gecRol32 = (x, r) => ((x << r) | (x >>> (32 - r))) >>> 0; // 循环左移
const gecRotr32 = (x, r) => ((x >>> r) | (x << (32 - r))) >>> 0; // 循环右移
const gecRol16 = (x) => ((x << 16) | (x >>> 16)) >>> 0;
// σ0/σ1（小 sigma）与 Σ0/Σ1（大 sigma）：标准 SHA-256 定义（循环右移）
const gecSigma0 = (x) => (gecRotr32(x, 7) ^ gecRotr32(x, 18) ^ (x >>> 3)) >>> 0;
const gecSigma1 = (x) => (gecRotr32(x, 17) ^ gecRotr32(x, 19) ^ (x >>> 10)) >>> 0;
const gecBigSigma1 = (e) => (gecRotr32(e, 6) ^ gecRotr32(e, 11) ^ gecRotr32(e, 25)) >>> 0;
const gecBigSigma0 = (a) => (gecRotr32(a, 2) ^ gecRotr32(a, 13) ^ gecRotr32(a, 22)) >>> 0;

/** SHA-256 压缩函数（从 firstRound 轮恢复；firstRound=14 时首块用折叠 K 表） */
function gecSha256Rounds(state, words, K, firstRound) {
  let [a, b, c, d, e, f, g, h] = state;
  for (let i = firstRound; i < 64; i++) {
    const t1 = (h + gecBigSigma1(e) + (g ^ (e & (f ^ g))) + K[i] + words[i]) >>> 0;
    const t2 = (gecBigSigma0(a) + ((a & b) | (c & (a | b)))) >>> 0;
    h = g; g = f; f = e; e = (d + t1) >>> 0;
    d = c; c = b; b = a; a = (t1 + t2) >>> 0;
  }
  return [a, b, c, d, e, f, g, h];
}

/** 标准 SHA-256 消息调度（word 16..63） */
function gecExpandSchedule(words, firstWord) {
  for (let i = firstWord; i < 64; i++) {
    words[i] =
      (gecSigma1(words[i - 2]) + words[i - 7] + gecSigma0(words[i - 15]) + words[i - 16]) >>> 0;
  }
}

/**
 * 折叠版 SHA-256 初始态：data[:7] 作为盐注入工作区，
 * 等价于标准 SHA-256 已完成轮 0..13 的中间状态。
 */
function gecSha256InitState(salt7) {
  const words = new Array(14).fill(0);
  words.push(((salt7[0] << 16) | (salt7[1] << 8) | salt7[2]) >>> 0); // data[:3] 仅 3 字节
  words.push(salt7.readUInt32BE(3)); // data[3:7] 4 字节
  for (let index = 16; index <= 29; index++) {
    let value = GEC_SHA256_SCHEDULE[index - 16];
    if (index - 2 >= 14) {
      value += index === 16
        ? (gecSigma1(words[index - 2]) ^ GEC_SIGMA1_ADJUSTMENT)
        : gecSigma1(words[index - 2]);
    }
    if (index - 7 >= 14) value += words[index - 7];
    if (index - 15 >= 14) {
      value += index === 29
        ? (gecSigma0(words[index - 15]) ^ GEC_SIGMA0_ADJUSTMENT)
        : gecSigma0(words[index - 15]);
    }
    words.push(value >>> 0);
  }
  words.push(
    (gecSigma1(words[28]) + words[23] + gecSigma0(words[15]) + words[14]
      + GEC_SHA256_ADJUSTMENT - GEC_SHA256_ROUND_CONSTANTS[14]) >>> 0,
  );
  gecExpandSchedule(words, 31);
  const working = gecSha256Rounds(GEC_SHA256_STATE, words, GEC_SHA256_BLOCK_CONSTANTS, 14);
  return GEC_SHA256_INITIAL.map((x, i) => (x + working[i]) >>> 0);
}

/** 折叠版 SHA-256 摘要（v2 令牌的后 64 位 hex） */
function gecSha256Digest(data) {
  if (data.length < 7) throw new Error('token salt too short');
  const length = 57 + data.length;
  const zeroPad = (((55 - length) % 64) + 64) % 64;
  const padded = Buffer.concat([
    data.subarray(7),
    Buffer.from([0x80]),
    Buffer.alloc(zeroPad),
    (() => { const b = Buffer.alloc(8); b.writeBigUInt64BE(BigInt(length) * 8n); return b; })(),
  ]);
  let state = gecSha256InitState(data.subarray(0, 7));
  for (let offset = 0; offset < padded.length; offset += 64) {
    const words = [];
    for (let i = 0; i < 16; i++) words.push(padded.readUInt32BE(offset + i * 4));
    gecExpandSchedule(words, 16);
    const working = gecSha256Rounds(state, words, GEC_SHA256_ROUND_CONSTANTS, 0);
    state = state.map((x, i) => (x + working[i]) >>> 0);
  }
  return Buffer.concat(state.map((x) => { const b = Buffer.alloc(4); b.writeUInt32BE(x); return b; }));
}

/** CS64 摘要（v2 令牌的前 16 位 hex）：自定义 64 位校验和 */
function gecCs64Digest(data, digest) {
  const keyA = digest.readUInt32LE(0) | 1;
  const keyB = digest.readUInt32LE(4) | 1;
  const factors = [
    Math.imul(keyA, GEC_CS64_FACTOR) >>> 0,
    Math.imul(keyB, GEC_CS64_FACTOR) >>> 0,
  ];
  const terms = GEC_CS64_COEFFICIENTS.map(([c], i) => Math.imul(c, factors[i % 2]) >>> 0);
  const offsets = GEC_CS64_COEFFICIENTS.map(([, o]) => o);

  // LE 4 字节分词（前补 1 个 0 字节），补零对齐 4 字节并补齐到偶数个词
  const unaligned = Buffer.concat([Buffer.from([0]), data]);
  const aligned = Buffer.concat([
    unaligned,
    Buffer.alloc((4 - (unaligned.length % 4)) % 4),
  ]);
  const words = [];
  for (let i = 0; i < aligned.length; i += 4) words.push(aligned.readUInt32LE(i));
  if (words.length % 2) words.push(0);

  words.forEach((word, i) => {
    let term = Math.imul(word, i % 2 === 0 ? keyA : keyB) >>> 0;
    let offset = Math.imul(gecRol16(word), i % 2 === 0 ? 0xef0569fb : 0xc31713db) >>> 0;
    if (i === 0) {
      term = (term + Math.imul(GEC_CS64_ADJUSTMENT0, factors[0])) >>> 0;
      offset = (offset + GEC_CS64_ADJUSTMENT1) >>> 0;
    }
    terms.push(term);
    offsets.push(offset);
  });

  let swapAcc = 0, swapLast = 0, state = 0, acc = 0;
  for (let i = 0; i < terms.length; i += 2) {
    const x = terms[i], y = terms[i + 1];

    let t = gecRol16((offsets[i] + x) >>> 0);
    let q = Math.imul(t, 0x76f50f34) >>> 0;
    swapAcc = (swapAcc + Math.imul(gecRol16(q), 0xc3efea97) + q) >>> 0;
    t = gecRol16((offsets[i + 1] + y) >>> 0);
    q = Math.imul(t, 0x1881ce3c) >>> 0;
    swapLast = (Math.imul(gecRol16(q), 0x35bd1ec9) + q) >>> 0;
    swapAcc = (swapAcc + swapLast) >>> 0;

    state = (Math.imul(state, keyA) + x) >>> 0;
    state = Math.imul(gecRol16(state), 0xcf98b111) >>> 0;
    state = Math.imul(gecRol16(state), 0x87085b9f) >>> 0;
    state = Math.imul(gecRol16(state), 0x12ceb96d) >>> 0;
    state = Math.imul(gecRol16(state), 0x257e1d83) >>> 0;
    acc = (acc + state) >>> 0;

    let t2 = (Math.imul(state, keyB) + y) >>> 0;
    t2 = Math.imul(gecRol16(t2), 0xa27416f5) >>> 0;
    t2 = Math.imul(gecRol16(t2), 0xd38396ff) >>> 0;
    state = Math.imul(gecRol16(Math.imul(gecRol16(t2), 0x7c932b89)), 0xbfa49f69) >>> 0;
    acc = (acc + state) >>> 0;
  }
  return [(swapLast ^ state) >>> 0, (swapAcc ^ acc) >>> 0];
}

/** v2 令牌："<16hex(CS64)>-<64hex(SHA256)>" */
function generateSecMsGecV2(unixSeconds) {
  const ticks =
    (unixSeconds !== undefined ? unixSeconds : Math.floor(Date.now() / 1000)) + GEC_WIN_EPOCH;
  const rounded = ticks - (ticks % 300);
  const windowsTicks = BigInt(rounded) * 10000000n; // 100ns 单位（Win FILETIME）
  const data = Buffer.from(`${windowsTicks.toString()}${GEC_TOKEN}`, 'ascii');
  const digest = gecSha256Digest(data);
  const [a, b] = gecCs64Digest(data, digest);
  const le = Buffer.alloc(8);
  le.writeUInt32LE(a, 0);
  le.writeUInt32LE(b, 4);
  return `${le.toString('hex').toUpperCase()}-${digest.toString('hex').toUpperCase()}`;
}

/** 覆盖 MsEdgeTTS.getSynthUrl：v2 令牌 + "2-" 版本前缀 + 经典端点（详见上注释） */
function applyGecV2Patch() {
  // 启动自验：官方测试向量必须逐字节匹配，否则令牌实现有误，尽早暴露
  const expect = '492D7694A183A45E-56E821F95D4579C5B7C7E33DC7303FF379EC36D510213555BBBB9EB17EF15876';
  const got = generateSecMsGecV2(1791888000);
  if (got !== expect) {
    throw new Error(`Sec-MS-GEC v2 自验失败：got=${got}`);
  }

  MsEdgeTTS.getSynthUrl = async function () {
    const reqId = MsEdgeTTS.generateUUID();
    const now = Math.floor((Date.now() + (MsEdgeTTS._clockOffsetMs ?? 0)) / 1000);
    const gec = generateSecMsGecV2(now);
    return `wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1`
      + `?TrustedClientToken=${MsEdgeTTS.TRUSTED_CLIENT_TOKEN}`
      + `&Sec-MS-GEC=${gec}&Sec-MS-GEC-Version=${GEC_VERSION}&ConnectionId=${reqId}`;
  };
}

/* ---- 合成引擎（单音色 + 串行队列 + 缓存 + 自愈重试，精简自健身指导专家） ---- */
const TTS_VOICE = 'zh-CN-XiaoxiaoNeural'; // 晓晓（女·自然）
const TTS_IDLE_MS = 3 * 60 * 1000;   // 连接空闲多久后重建（微软会静默掐断空闲 WSS）
const TTS_TIMEOUT_MS = 15 * 1000;    // 单次合成最长等待
const TTS_ATTEMPTS = 3;              // 失败自动重试次数（每次重建连接）
const TTS_CACHE_MAX = 200;
const ttsCache = new Map();          // key: rate|sha1(text) -> Buffer
let ttsEntry = null;                 // { tts, ready, lastUsed }
let ttsChain = Promise.resolve();    // 串行：并发共用一条 WSS 易得「turn.end 零音频」

function ttsEnabled() {
  return Boolean(MsEdgeTTS && OUTPUT_FORMAT);
}

function dropTts() {
  if (!ttsEntry) return;
  const entry = ttsEntry;
  ttsEntry = null;
  try { entry.tts.close(); } catch (_) { /* 已关闭 */ }
}

async function getTts() {
  if (ttsEntry && Date.now() - ttsEntry.lastUsed > TTS_IDLE_MS) {
    dropTts();
  }
  if (!ttsEntry) {
    const tts = new MsEdgeTTS({ enableLogger: false });
    ttsEntry = { tts, ready: null, lastUsed: Date.now() };
    ttsEntry.ready = tts
      .setMetadata(TTS_VOICE, OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3)
      .catch((e) => {
        dropTts();
        throw e;
      });
  }
  await ttsEntry.ready;
  ttsEntry.lastUsed = Date.now();
  return ttsEntry.tts;
}

/** SSML 转义：未转义的 & < > 会让微软解析失败并返回空音频 */
function escapeSsml(s) {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

async function synthOnce(text, rate) {
  const tts = await getTts();
  const { audioStream } = tts.toStream(escapeSsml(text), { rate });
  const buf = await new Promise((resolve, reject) => {
    const chunks = [];
    let settled = false;
    const finish = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (err) reject(err);
      else resolve(Buffer.concat(chunks));
    };
    const timer = setTimeout(() => {
      try { audioStream.destroy(); } catch (_) { /* ignore */ }
      finish(new Error(`synthesis timeout (${TTS_TIMEOUT_MS}ms)`));
    }, TTS_TIMEOUT_MS);
    audioStream.on('data', (c) => chunks.push(c));
    audioStream.on('close', () => finish());
    audioStream.on('error', (e) => finish(e));
  });
  // 短于 512B 视为空音频（MP3 一帧都不到）
  if (buf.length < 512) throw new Error(`empty audio (${buf.length}B)`);
  return buf;
}

/** 带自愈的串行合成：失败丢弃连接 → 重建重试 */
async function synthTts(text, rate) {
  let lastErr;
  for (let attempt = 1; attempt <= TTS_ATTEMPTS; attempt++) {
    try {
      const run = ttsChain.then(() => synthOnce(text, rate));
      ttsChain = run.catch(() => {});
      return await run;
    } catch (e) {
      lastErr = e;
      console.error(`[tts] 合成失败(第${attempt}/${TTS_ATTEMPTS}次):`, e && e.message || e);
      dropTts();
      if (attempt < TTS_ATTEMPTS) await new Promise((r) => setTimeout(r, 250 + attempt * 350));
    }
  }
  throw lastErr;
}

async function handleTts(req, res) {
  setCors(res);
  if (!ttsEnabled()) return json(res, 503, { error: { code: 'tts_unavailable', message: 'server tts not installed' } });
  try {
    const body = await readBody(req);
    const text = String(body && body.text || '').trim();
    const rate = Math.min(1.5, Math.max(0.5, Number(body && body.rate) || 1));
    if (!text) return json(res, 400, { error: { code: 'bad_request', message: 'text required' } });
    if (text.length > 500) return json(res, 400, { error: { code: 'bad_request', message: 'text too long (max 500 chars)' } });
    const key = `${rate}|${crypto.createHash('sha1').update(text).digest('hex')}`;
    let buf = ttsCache.get(key);
    if (!buf) {
      buf = await synthTts(text, rate);
      ttsCache.set(key, buf);
      if (ttsCache.size > TTS_CACHE_MAX) ttsCache.delete(ttsCache.keys().next().value);
    }
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Cache-Control', 'no-store');
    res.writeHead(200, { 'Content-Type': 'audio/mpeg', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' });
    res.end(buf);
  } catch (err) {
    console.error('[tts] 接口错误:', err && err.message || err);
    if (!res.headersSent) json(res, 503, { error: { code: 'tts_failed', message: String(err && err.message || err) } });
    else res.end();
  }
}

const server = http.createServer(async (req, res) => {
  const url = (req.url || '').split('?')[0];
  if (req.method === 'OPTIONS') { setCors(res); res.writeHead(204); return res.end(); }

  if (req.method === 'GET' && url === '/api/health') {
    return json(res, 200, { ok: true, model: AI_MODEL, time: new Date().toISOString() });
  }
  if (req.method === 'POST' && url === '/api/gua/tts') {
    return handleTts(req, res);
  }
  if (req.method === 'POST' && url === '/api/gua/explain') {
    return handleExplain(req, res);
  }
  return json(res, 404, { error: { code: 'not_found', message: 'unknown path' } });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[8gua-api] listening on 127.0.0.1:${PORT}, model=${AI_MODEL}, db=${DB_PATH}`);
});
