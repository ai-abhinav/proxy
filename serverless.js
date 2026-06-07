#!/usr/bin/env node
/**
 * serverless.js — single-file OpenCode proxy (zero npm deps)
 *
 * Per request: fetch exit IP → stream OpenCode SSE tokens in real time
 *
 * Local:  node serverless.js
 * Lambda: exports handler (buffered) or streamifyResponse on Node 20+
 * Env:    PORT, MODEL, PROXY_URL, PROXY_USER, PROXY_PASS, UPSTREAM, IPINFO
 */

"use strict";

const http = require("http");
const https = require("https");
const net = require("net");
const tls = require("tls");
const { URL } = require("url");

const UPSTREAM = process.env.UPSTREAM || "https://opencode.ai/zen/v1/chat/completions";
const IPINFO = process.env.IPINFO || "http://ipinfo.io/ip";
const MODEL = process.env.MODEL || "deepseek-v4-flash-free";
const PORT = parseInt(process.env.PORT || "8888", 10);
const PROXY_HOST = process.env.PROXY_URL || "http://global.rotgb.711proxy.com:10000";
const PROXY_USER = process.env.PROXY_USER || "USER500841-zone-custom";
const PROXY_PASS = process.env.PROXY_PASS || "3bf0e3";

let sessionSeq = 0;

function proxySessionUser() {
  return `${PROXY_USER}-${Date.now()}-${++sessionSeq}`;
}

function parseProxy() {
  const u = new URL(PROXY_HOST);
  return {
    host: u.hostname,
    port: parseInt(u.port || "80", 10),
    auth: Buffer.from(`${PROXY_USER}:${PROXY_PASS}`).toString("base64"),
  };
}

function sessionAuth(sessionUser) {
  return Buffer.from(`${sessionUser}:${PROXY_PASS}`).toString("base64");
}

function connectProxy(sessionUser, targetHost, targetPort, useTls) {
  const px = parseProxy();
  const auth = sessionAuth(sessionUser);

  return new Promise((resolve, reject) => {
    const sock = net.connect(px.port, px.host);
    const timer = setTimeout(() => {
      sock.destroy();
      reject(new Error("proxy connect timeout"));
    }, 8000);

    sock.once("error", (e) => { clearTimeout(timer); reject(e); });

    sock.once("connect", () => {
      if (useTls) {
        sock.write(
          `CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\n` +
          `Host: ${targetHost}:${targetPort}\r\n` +
          `Proxy-Authorization: Basic ${auth}\r\n` +
          `Proxy-Connection: keep-alive\r\n\r\n`
        );
      } else {
        resolve({ socket: sock, timer });
      }
    });

    if (!useTls) return;

    let buf = "";
    const onData = (chunk) => {
      buf += chunk.toString("latin1");
      const end = buf.indexOf("\r\n\r\n");
      if (end < 0) return;
      sock.removeListener("data", onData);
      const statusLine = buf.split("\r\n")[0];
      if (!statusLine.includes(" 200 ")) {
        clearTimeout(timer);
        sock.destroy();
        reject(new Error(`proxy CONNECT failed: ${statusLine}`));
        return;
      }
      const secure = tls.connect({ socket: sock, servername: targetHost }, () => {
        clearTimeout(timer);
        resolve({ socket: secure, timer: null });
      });
      secure.once("error", reject);
    };
    sock.on("data", onData);
  });
}

function httpRequestOnSocket(socket, { method, path, host, headers, body }) {
  return new Promise((resolve, reject) => {
    let hdr =
      `${method} ${path} HTTP/1.1\r\n` +
      `Host: ${host}\r\n` +
      `Connection: close\r\n`;
    for (const [k, v] of Object.entries(headers || {})) hdr += `${k}: ${v}\r\n`;
    if (body && !headers["Content-Length"]) hdr += `Content-Length: ${Buffer.byteLength(body)}\r\n`;
    hdr += "\r\n";

    socket.write(hdr);
    if (body) socket.write(body);

    let raw = Buffer.alloc(0);
    socket.on("data", (c) => { raw = Buffer.concat([raw, c]); });
    socket.on("end", () => {
      const sep = raw.indexOf("\r\n\r\n");
      if (sep < 0) return reject(new Error("bad response"));
      const head = raw.slice(0, sep).toString("latin1");
      const status = parseInt(head.split("\r\n")[0].split(" ")[1], 10);
      resolve({ status, body: raw.slice(sep + 4).toString("utf8").trim() });
    });
    socket.on("error", reject);
  });
}

async function proxyHttpGet(sessionUser, urlStr) {
  const u = new URL(urlStr);
  const px = parseProxy();
  const auth = sessionAuth(sessionUser);
  const path = u.pathname + u.search;

  return new Promise((resolve, reject) => {
    const req = http.request({
      host: px.host,
      port: px.port,
      method: "GET",
      path: urlStr,
      headers: {
        Host: u.host,
        "Proxy-Authorization": `Basic ${auth}`,
        Connection: "close",
      },
    }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString("utf8").trim() }));
    });
    req.on("error", reject);
    req.setTimeout(8000, () => { req.destroy(); reject(new Error("timeout")); });
    req.end();
  });
}

function createChunkedDecoder(onData) {
  let buf = Buffer.alloc(0);
  let mode = "size"; // size | body | tail
  let chunkRemaining = 0;

  const feed = (input) => {
    buf = Buffer.concat([buf, input]);
    for (;;) {
      if (mode === "size") {
        const lineEnd = buf.indexOf("\r\n");
        if (lineEnd < 0) return;
        const line = buf.slice(0, lineEnd).toString("latin1").trim();
        buf = buf.slice(lineEnd + 2);
        chunkRemaining = parseInt(line, 16);
        if (!Number.isFinite(chunkRemaining) || chunkRemaining < 0) {
          throw new Error("bad chunk size");
        }
        if (chunkRemaining === 0) {
          mode = "tail";
          continue;
        }
        mode = "body";
      } else if (mode === "body") {
        if (buf.length < chunkRemaining + 2) return;
        const chunk = buf.slice(0, chunkRemaining);
        onData(chunk);
        buf = buf.slice(chunkRemaining + 2);
        chunkRemaining = 0;
        mode = "size";
      } else {
        return;
      }
    }
  };

  return { feed };
}

async function proxyHttpsStream(sessionUser, urlStr, { method, headers, body, onChunk }) {
  const u = new URL(urlStr);
  const { socket } = await connectProxy(sessionUser, u.hostname, 443, true);

  let hdr =
    `${method} ${u.pathname}${u.search} HTTP/1.1\r\n` +
    `Host: ${u.host}\r\n` +
    `Connection: close\r\n`;
  for (const [k, v] of Object.entries(headers || {})) hdr += `${k}: ${v}\r\n`;
  const bodyBuf = body ? Buffer.from(body) : null;
  if (bodyBuf) hdr += `Content-Length: ${bodyBuf.length}\r\n`;
  hdr += "\r\n";
  socket.write(hdr);
  if (bodyBuf) socket.write(bodyBuf);

  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    let headerDone = false;
    let status = 0;
    let encoding = "";
    let contentLength = -1;
    let bodyStarted = false;
    let bodyBytes = 0;
    let decoder = null;

    socket.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      if (!headerDone) {
        const sep = buf.indexOf("\r\n\r\n");
        if (sep < 0) return;
        const head = buf.slice(0, sep).toString("latin1");
        status = parseInt(head.split("\r\n")[0].split(" ")[1], 10);
        for (const line of head.split("\r\n").slice(1)) {
          const ci = line.indexOf(":");
          if (ci < 0) continue;
          const k = line.slice(0, ci).trim().toLowerCase();
          const v = line.slice(ci + 1).trim().toLowerCase();
          if (k === "transfer-encoding" && v.includes("chunked")) encoding = "chunked";
          if (k === "content-length") contentLength = parseInt(v, 10);
        }
        headerDone = true;
        buf = buf.slice(sep + 4);
        bodyStarted = true;
        if (encoding === "chunked") decoder = createChunkedDecoder(onChunk);
      }

      if (!bodyStarted || !buf.length) return;

      if (encoding === "chunked") {
        decoder.feed(buf);
        buf = Buffer.alloc(0);
      } else if (contentLength >= 0) {
        const take = Math.min(buf.length, contentLength - bodyBytes);
        if (take > 0) {
          onChunk(buf.slice(0, take));
          bodyBytes += take;
          buf = buf.slice(take);
        }
      } else {
        onChunk(buf);
        buf = Buffer.alloc(0);
      }
    });
    socket.on("end", () => resolve({ status }));
    socket.on("error", reject);
  });
}

async function fetchExitIp(sessionUser) {
  const res = await proxyHttpGet(sessionUser, IPINFO);
  if (res.status !== 200 || !res.body) throw new Error(`ipinfo ${res.status}`);
  return res.body.replace(/\s/g, "");
}

function patchBody(raw) {
  let body;
  try {
    body = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch {
    body = { messages: [{ role: "user", content: String(raw || "") }] };
  }
  body.model = MODEL;
  body.stream = true;
  return JSON.stringify(body);
}

async function streamChat(rawBody, write, end) {
  const session = proxySessionUser();
  const proxyIp = await fetchExitIp(session);

  write(`event: proxy_ip\ndata: ${JSON.stringify({ proxy_ip: proxyIp })}\n\n`);

  const { status } = await proxyHttpsStream(session, UPSTREAM, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
    },
    body: patchBody(rawBody),
    onChunk: (chunk) => write(chunk.toString("utf8")),
  });

  if (status < 200 || status >= 300) {
    write(`data: ${JSON.stringify({ error: { message: "upstream failed", status } })}\n\n`);
  }
  end();
}

function sseHeaders(extra) {
  return {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
    ...extra,
  };
}

function jsonHeaders(extra) {
  return { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", ...extra };
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

async function handleHttp(req, res) {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    });
    res.end();
    return;
  }

  if (req.method === "GET" && url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok\n");
    return;
  }

  if (req.method === "GET" && (url.pathname === "/v1/models" || url.pathname === "/models")) {
    res.writeHead(200, jsonHeaders());
    res.end(JSON.stringify({ object: "list", data: [{ id: MODEL, object: "model", owned_by: "opencode" }] }));
    return;
  }

  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/v1")) {
    res.writeHead(200, jsonHeaders());
    res.end(JSON.stringify({ ok: true, model: MODEL, mode: "serverless", stream: true }));
    return;
  }

  const isChat = req.method === "POST" &&
    (url.pathname === "/v1/chat/completions" || url.pathname === "/chat/completions");

  if (!isChat) {
    res.writeHead(404, jsonHeaders());
    res.end(JSON.stringify({ error: { message: "not found" } }));
    return;
  }

  let raw;
  try { raw = await readBody(req); } catch {
    res.writeHead(400, jsonHeaders());
    res.end(JSON.stringify({ error: { message: "bad body" } }));
    return;
  }

  res.writeHead(200, sseHeaders());
  let closed = false;
  const write = (c) => { if (!closed) res.write(c); };
  const end = () => { if (!closed) { closed = true; res.end(); } };
  req.on("close", () => { closed = true; });

  try {
    await streamChat(raw, write, end);
  } catch (e) {
    if (!closed) {
      write(`data: ${JSON.stringify({ error: { message: String(e.message || e) } })}\n\n`);
      write("data: [DONE]\n\n");
      end();
    }
  }
}

async function lambdaHandler(event) {
  const body = event.isBase64Encoded
    ? Buffer.from(event.body || "", "base64").toString("utf8")
    : (event.body || "{}");
  const chunks = [];
  await streamChat(body, (c) => chunks.push(c), () => {});
  return { statusCode: 200, headers: sseHeaders(), body: chunks.join("") };
}

const handler =
  typeof globalThis.awslambda !== "undefined" && globalThis.awslambda?.streamifyResponse
    ? globalThis.awslambda.streamifyResponse(async (event, responseStream) => {
        const rs = globalThis.awslambda.HttpResponseStream.from(responseStream, {
          statusCode: 200,
          headers: sseHeaders(),
        });
        const body = event.isBase64Encoded
          ? Buffer.from(event.body || "", "base64").toString("utf8")
          : (event.body || "{}");
        try {
          await streamChat(body, (c) => rs.write(c), () => rs.end());
        } catch (e) {
          rs.write(`data: ${JSON.stringify({ error: { message: String(e.message || e) } })}\n\n`);
          rs.end();
        }
      })
    : lambdaHandler;

module.exports = { handler, streamChat, fetchExitIp, patchBody, handleHttp };

if (require.main === module) {
  http.createServer(handleHttp).listen(PORT, "0.0.0.0", () => {
    console.error(`[serverless] http://0.0.0.0:${PORT} | model=${MODEL}`);
    console.error(`[serverless] fetch IP → stream tokens | zero deps`);
  });
}
