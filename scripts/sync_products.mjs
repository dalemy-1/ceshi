// scripts/sync_products.mjs
// One-shot pipeline:
// 1) Fetch CSV from CSV_URL (source of truth)
// 2) Rebuild products.json + archive.json
// 3) Generate /p/{market}/{asinKey}/index.html pages for stable share URLs
//
// Required env:
//   CSV_URL=https://.../export_csv
//
// Optional env:
//   SITE_ORIGIN=https://ama.omino.top   (used for /p page generation)
//   OUT_DIR=./p                         (defaults to ./p)

import fs from "fs";
import path from "path";
import process from "process";

// ================== ENV ==================
const CSV_URL = (process.env.CSV_URL || "").trim();
if (!CSV_URL) {
  console.error("[error] CSV_URL is empty. Set env CSV_URL in workflow.");
  process.exit(1);
}

const SITE_ORIGIN = (process.env.SITE_ORIGIN || "https://ama.omino.top").replace(/\/+$/, "");
const OUT_DIR = (process.env.OUT_DIR || "./p").trim();

// ================== PATHS ==================
const ROOT = process.cwd();
const PRODUCTS_PATH = path.join(ROOT, "products.json");
const ARCHIVE_PATH = path.join(ROOT, "archive.json");

// ================== HELPERS ==================
function norm(s) {
  return String(s ?? "").trim();
}
function upper(s) {
  return norm(s).toUpperCase();
}
function normalizeMarket(v) {
  return upper(v);
}
function normalizeAsin(v) {
  return upper(v);
}
function normalizeUrl(v) {
  const s = norm(v);
  if (!s) return "";
  if (/^https?:\/\//i.test(s)) return s;
  return "";
}
function safeHttps(url) {
  return norm(url).replace(/^http:\/\//i, "https://");
}
function safeReadJson(file, fallback) {
  try {
    if (!fs.existsSync(file)) return fallback;
    const txt = fs.readFileSync(file, "utf8");
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}
function writeJsonPretty(file, obj) {
  const json = JSON.stringify(obj, null, 2) + "\n";
  fs.writeFileSync(file, json, "utf8");
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// Minimal CSV parser supporting quoted fields and commas inside quotes.
function parseCSV(text) {
  const rows = [];
  let i = 0;
  let field = "";
  let row = [];
  let inQuotes = false;

  while (i < text.length) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      field += c;
      i++;
      continue;
    }

    if (c === '"') {
      inQuotes = true;
      i++;
      continue;
    }
    if (c === ",") {
      row.push(field);
      field = "";
      i++;
      continue;
    }
    if (c === "\r") {
      i++;
      continue;
    }
    if (c === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      i++;
      continue;
    }

    field += c;
    i++;
  }

  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

function mapRow(headers, cells) {
  const obj = {};
  headers.forEach((h, idx) => {
    obj[h] = norm(cells[idx] ?? "");
  });
  return obj;
}

function isActiveStatus(v) {
  const s = norm(v).toLowerCase();
  if (!s) return true; // CSV 未填 status => 默认上架
  return ["1", "true", "yes", "on", "active", "enabled", "publish", "published", "online"].includes(s);
}
function isAllDigits(s) {
  const x = norm(s);
  return x !== "" && /^[0-9]+$/.test(x);
}

// 你的规则：纯数字 => 其它平台（Walmart 等），不展示
function isNonAmazonByAsin(asin) {
  return isAllDigits(asin);
}


function keyOf(p) {
  return `${upper(p.market)}|${upper(p.asin)}`;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "github-actions-sync/1.0",
      "cache-control": "no-cache",
      pragma: "no-cache",
    },
  });
  if (!res.ok) throw new Error(`Fetch CSV failed: HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}

// ================== /p PAGE GENERATOR ==================
// Always use Weserv for OG image (stable https)
function toWeservOg(url) {
  const u = safeHttps(url);
  if (!u) return "";
  const cleaned = u.replace(/^https?:\/\//i, "");
  return `https://images.weserv.nl/?url=${encodeURIComponent(cleaned)}&w=1200&h=630&fit=cover&output=jpg`;
}

function annotateAsinKeys(list) {
  const counter = new Map();
  for (const p of list) {
    const m = upper(p.market);
    const a = upper(p.asin);
    const k = `${m}|${a}`;
    const seen = counter.get(k) || 0;
    p.__asinKey = seen === 0 ? a : `${a}_${seen}`;
    counter.set(k, seen + 1);
  }
}

function buildPreviewHtml({ market, asinKey, imageUrl }) {
  const mLower = String(market).toLowerCase();
  const pagePath = `/p/${encodeURIComponent(mLower)}/${encodeURIComponent(asinKey)}/`;
  const pageUrl = SITE_ORIGIN + pagePath;

  const ogTitle = `Product Reference • ${upper(market)} • ${asinKey}`;
  const ogDesc =
    "Independent product reference. Purchases are completed on Amazon. As an Amazon Associate, we earn from qualifying purchases.";

  const ogImage = toWeservOg(imageUrl) || `${SITE_ORIGIN}/og-placeholder.jpg`;

  // 这个版本：不再 meta refresh 跳转
  // 页面在 /p/... 原地渲染，并从 products.json / archive.json 里查找对应 market+asin
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>${escapeHtml(ogTitle)} • Product Picks</title>
  <meta name="description" content="${escapeHtml(ogDesc)}" />

  <meta property="og:type" content="website" />
  <meta property="og:site_name" content="Product Picks" />
  <meta property="og:title" content="${escapeHtml(ogTitle)}" />
  <meta property="og:description" content="${escapeHtml(ogDesc)}" />
  <meta property="og:url" content="${escapeHtml(pageUrl)}" />
  <meta property="og:image" content="${escapeHtml(ogImage)}" />
  <meta property="og:image:secure_url" content="${escapeHtml(ogImage)}" />
  <meta property="og:image:type" content="image/jpeg" />
  <meta property="og:image:width" content="1200" />
  <meta property="og:image:height" content="630" />

  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="${escapeHtml(ogTitle)}" />
  <meta name="twitter:description" content="${escapeHtml(ogDesc)}" />
  <meta name="twitter:image" content="${escapeHtml(ogImage)}" />

  <!-- 建议：独立页不参与索引，降低审核与重复页风险 -->
  <meta name="robots" content="noindex,nofollow" />

  <style>
    :root{
      --bg:#0b1220;--card:#0f1b33;--txt:#e5e7eb;--muted:#9ca3af;--bd:rgba(255,255,255,.12);--btn:#2563eb
    }
    *{box-sizing:border-box}
    body{margin:0;background:linear-gradient(180deg,#070b14,#0b1220);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;color:var(--txt)}
    .wrap{max-width:980px;margin:0 auto;padding:18px}
    .card{background:rgba(15,27,51,.92);border:1px solid var(--bd);border-radius:16px;padding:16px}
    .top{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap}
    .badge{display:inline-flex;gap:8px;align-items:center;padding:6px 10px;border-radius:999px;border:1px solid var(--bd);background:rgba(255,255,255,.06);color:var(--muted);font-size:12px}
    .grid{display:grid;grid-template-columns:1fr;gap:14px;margin-top:14px}
    @media(min-width:860px){.grid{grid-template-columns:420px 1fr}}
    .imgbox{border-radius:14px;border:1px solid var(--bd);overflow:hidden;background:rgba(255,255,255,.04);display:flex;align-items:center;justify-content:center;min-height:320px}
    .imgbox img{width:100%;height:auto;display:block}
    .title{font-size:20px;font-weight:700;line-height:1.25;margin:0 0 8px}
    .meta{color:var(--muted);font-size:13px;line-height:1.55}
    .btn{display:inline-flex;align-items:center;justify-content:center;gap:10px;padding:12px 14px;border-radius:12px;border:1px solid rgba(37,99,235,.35);
         background:rgba(37,99,235,.18);color:#fff;text-decoration:none;font-weight:700}
    .btn:hover{background:rgba(37,99,235,.28)}
    .disclaimer{margin-top:12px;color:var(--muted);font-size:12px;line-height:1.55}
    .err{color:#fecaca;background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.25);padding:10px 12px;border-radius:12px}
    .row{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="top">
        <div class="badge" id="badge">Loading…</div>
        <div class="row">
          <a class="btn" id="ctaAmazon" href="#" rel="nofollow noopener" target="_blank" style="display:none">Open on Amazon</a>
          <a class="btn" id="ctaHome" href="${escapeHtml(SITE_ORIGIN)}/" style="background:rgba(255,255,255,.08);border:1px solid var(--bd)">Back to list</a>
        </div>
      </div>

      <div class="grid">
        <div class="imgbox"><img id="img" alt="" style="display:none"/></div>
        <div>
          <h1 class="title" id="title">Loading…</h1>
          <div class="meta" id="meta"></div>
          <div class="disclaimer">
            As an Amazon Associate, we earn from qualifying purchases. This page is an assumption-free product reference; purchases are completed on Amazon.
          </div>
        </div>
      </div>

      <div id="msg" style="margin-top:12px"></div>
    </div>
  </div>

<script>
(function(){
  function norm(s){return String(s||"").trim();}
  function upper(s){return norm(s).toUpperCase();}
  function setText(id,t){var el=document.getElementById(id); if(el) el.textContent=t;}
  function setHTML(id,t){var el=document.getElementById(id); if(el) el.innerHTML=t;}

  function parsePath(){
    var p = location.pathname || "/";
    if (p.length > 1 && p.endsWith("/")) p = p.slice(0,-1);
    // /p/uk/B07RYTFHCR 或 /p/uk/B07RYTFHCR_1
    var m = p.match(/^\\/p\\/([a-zA-Z]{2})\\/([A-Za-z0-9]{10})(?:_(\\d+))?$/);
    if(!m) return null;
    return { market: upper(m[1]), asin: upper(m[2]) };
  }

  async function loadJson(url){
    var r = await fetch(url, { cache:"no-store" });
    if(!r.ok) throw new Error("HTTP " + r.status);
    return await r.json();
  }

  function findItem(list, market, asin){
    if(!list) return null;
    var arr = Array.isArray(list) ? list : (list.items || list.products || []);
    for (var i=0;i<arr.length;i++){
      var x = arr[i] || {};
      if(upper(x.market)===market && upper(x.asin)===asin) return x;
    }
    return null;
  }

  function safeLinkAmazon(link){
    var s = norm(link);
    if(!s) return "";
    // 简单校验：只接受 http/https
    if(!/^https?:\\/\\//i.test(s)) return "";
    return s.replace(/^http:\\/\\//i,"https://");
  }

  async function main(){
    var info = parsePath();
    if(!info){
      setText("title","Invalid link");
      setHTML("msg",'<div class="err">Path format not recognized. Expected /p/{market}/{asin}.</div>');
      return;
    }

    setText("badge", info.market + " · " + info.asin);

    var products=null, archive=null;
    try{ products = await loadJson("/products.json"); }catch(e){}
    try{ archive  = await loadJson("/archive.json"); }catch(e){}

    var p = findItem(products, info.market, info.asin) || findItem(archive, info.market, info.asin);

    if(!p){
      setText("title","Product not found");
      setText("meta","Market: " + info.market + "\\nASIN: " + info.asin);
      setHTML("msg",'<div class="err">This item is not in the active list or archive.</div>');
      return;
    }

    var title = norm(p.title) || ("ASIN " + info.asin);
    setText("title", title);

    var meta = [];
    meta.push("Market: " + info.market);
    meta.push("ASIN: " + info.asin);
    setText("meta", meta.join(" • "));

    var img = norm(p.image_url);
    if(img){
      var im = document.getElementById("img");
      im.src = img.replace(/^http:\\/\\//i,"https://");
      im.style.display = "block";
      im.alt = title;
    }

    var link = safeLinkAmazon(p.link);
    if(link){
      var btn = document.getElementById("ctaAmazon");
      btn.href = link;
      btn.style.display = "inline-flex";
    }else{
      setHTML("msg",'<div class="err">Amazon link is missing or invalid for this item.</div>');
    }
  }

  main().catch(function(e){
    setText("title","Error");
    setHTML("msg",'<div class="err">'+String(e&&e.message||e)+'</div>');
  });
})();
</script>
</body>
</html>`;
}


function generatePPages(activeList, archiveList) {
    const outDirAbs = path.isAbsolute(OUT_DIR) ? OUT_DIR : path.join(ROOT, OUT_DIR);

  // 每次全量重建 /p，避免旧页面残留（非常关键）
  if (fs.existsSync(outDirAbs)) {
    fs.rmSync(outDirAbs, { recursive: true, force: true });
  }
  ensureDir(outDirAbs);


  const all = [...(activeList || []), ...(archiveList || [])].filter((p) => p && p.market && p.asin);

  // stable ordering -> stable asinKey assignment
  all.sort((a, b) => {
    const ma = upper(a.market),
      mb = upper(b.market);
    if (ma !== mb) return ma.localeCompare(mb);
    const aa = upper(a.asin),
      ab = upper(b.asin);
    if (aa !== ab) return aa.localeCompare(ab);
    return 0;
  });

  annotateAsinKeys(all);

  let count = 0;
  for (const p of all) {
    const market = upper(p.market);
    const asinKey = p.__asinKey;
    const img = norm(p.image_url);

    const dir = path.join(outDirAbs, market.toLowerCase(), asinKey);
    ensureDir(dir);

    const html = buildPreviewHtml({ market, asinKey, imageUrl: img });
    fs.writeFileSync(path.join(dir, "index.html"), html, "utf-8");
    count++;
  }

  console.log(`[p] generated ${count} pages under ${OUT_DIR}`);
}

// ================== MAIN ==================
(async () => {
  console.log("[sync] CSV_URL =", CSV_URL);

  const csvText = await fetchText(CSV_URL);
  console.log("[sync] CSV bytes =", csvText.length);

  const rows = parseCSV(csvText);
  if (!rows.length) throw new Error("CSV is empty");

  const headers = rows[0].map((h) => norm(h));
  const dataRows = rows
    .slice(1)
    .filter((r) => r.some((c) => norm(c) !== ""));

  console.log("[sync] headers =", headers.join(" | "));
  console.log("[sync] data rows =", dataRows.length);

  const prevProducts = Array.isArray(safeReadJson(PRODUCTS_PATH, [])) ? safeReadJson(PRODUCTS_PATH, []) : [];
  const prevArchive = Array.isArray(safeReadJson(ARCHIVE_PATH, [])) ? safeReadJson(ARCHIVE_PATH, []) : [];

  const prevMap = new Map();
  prevProducts.forEach((p) => prevMap.set(keyOf(p), p));

  const archiveMap = new Map();
  prevArchive.forEach((p) => archiveMap.set(keyOf(p), p));

  // Build new active list from CSV (source of truth)
    const nextProducts = [];
  for (const r of dataRows) {
    const o = mapRow(headers, r);

    const market = normalizeMarket(o.market || o.Market || o.MARKET);
    const asin = normalizeAsin(o.asin || o.ASIN);
    if (!market || !asin) continue;

    // 统一只声明一次，避免重复 const
    const title = norm(o.title || o.Title || "");
    const link = normalizeUrl(o.link || o.Link);
    const image_url = norm(o.image_url || o.image || o.Image || o.imageUrl || "");

    // ✅ 规则：纯数字 ASIN => 非 Amazon（Walmart 等），隐藏：进 archive，不进 products
    if (isNonAmazonByAsin(asin)) {
      const nonAmazonItem = {
        market,
        asin,
        title,
        link,
        image_url,
        _hidden_reason: "non_amazon_numeric_asin",
      };
      const k = keyOf(nonAmazonItem);
      if (!archiveMap.has(k)) archiveMap.set(k, nonAmazonItem);
      continue;
    }

    // ✅ status 下架：不进 products，但必须进 archive（确保独立页仍能展示）
    const statusVal = o.status ?? o.Status ?? o.STATUS;
    if (!isActiveStatus(statusVal)) {
      const archivedItem = {
        market,
        asin,
        title,
        link,
        image_url,
        _hidden_reason: "inactive_status",
      };
      const k = keyOf(archivedItem);
      if (!archiveMap.has(k)) archiveMap.set(k, archivedItem);
      continue;
    }

    // ✅ 正常上架：进 products
    nextProducts.push({
      market,
      asin,
      title,
      link,
      image_url,
    });
  }


    const statusVal = o.status ?? o.Status ?? o.STATUS;
const link = normalizeUrl(o.link || o.Link);
const image_url = norm(o.image_url || o.image || o.Image || o.imageUrl || "");
const title = norm(o.title || o.Title || "");

// ✅ status 下架：不进 products，但必须进 archive发现
if (!isActiveStatus(statusVal)) {
  const archivedItem = {
    market,
    asin,
    title,
    link,
    image_url,
    _hidden_reason: "inactive_status",
  };
  const k = keyOf(archivedItem);
  if (!archiveMap.has(k)) archiveMap.set(k, archivedItem);
  continue;
}


    const link = normalizeUrl(o.link || o.Link);
    const image_url = norm(o.image_url || o.image || o.Image || o.imageUrl || "");

    
  }


  nextProducts.sort((a, b) => {
    const am = a.market.localeCompare(b.market);
    if (am) return am;
    return a.asin.localeCompare(b.asin);
  });

  // Move removed items into archive
  const nextKeys = new Set(nextProducts.map(keyOf));
  let removedCount = 0;

  for (const [k, oldP] of prevMap.entries()) {
    if (!nextKeys.has(k)) {
      if (!archiveMap.has(k)) archiveMap.set(k, oldP);
      removedCount++;
    }
  }

  const nextArchive = Array.from(archiveMap.values());
  nextArchive.sort((a, b) => {
    const am = a.market.localeCompare(b.market);
    if (am) return am;
    return a.asin.localeCompare(b.asin);
  });

  console.log("[sync] next active =", nextProducts.length);
  console.log("[sync] removed from active =", removedCount);
  console.log("[sync] archive size =", nextArchive.length);

  writeJsonPretty(PRODUCTS_PATH, nextProducts);
  writeJsonPretty(ARCHIVE_PATH, nextArchive);
  console.log("[sync] wrote products.json & archive.json");

  // Generate /p pages
  generatePPages(nextProducts, nextArchive);

  console.log("[done] sync + generate /p pages completed");
})().catch((err) => {
  console.error("[fatal]", err?.stack || err);
  process.exit(1);
});
