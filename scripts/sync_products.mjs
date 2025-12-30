// scripts/sync_products.mjs
// One-shot pipeline:
// 1) Fetch CSV from CSV_URL (source of truth)
// 2) Rebuild products.json + archive.json
// 3) Generate /p/{market}/{asin}/index.html pages (stable share URLs -> redirect to SPA)
//
// Required env:
//   CSV_URL=http(s)://.../export_csv
//
// Optional env:
//   SITE_ORIGIN=https://ama.omino.top
//   OUT_DIR=./p (relative to site root dir)
//   SITE_DIR=product-list   (if your GitHub Pages publishes a subfolder)

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

// Auto-detect site dir (GitHub Pages root folder)
const ROOT = process.cwd();
const SITE_DIR = (() => {
  const explicit = (process.env.SITE_DIR || "").trim();
  if (explicit) return explicit.replace(/^\/+|\/+$/g, "");
  // auto: if product-list/products.json exists -> use product-list
  if (fs.existsSync(path.join(ROOT, "product-list", "products.json")) || fs.existsSync(path.join(ROOT, "product-list", "index.html"))) {
    return "product-list";
  }
  return "";
})();

function siteJoin(...segs) {
  return path.join(ROOT, SITE_DIR ? SITE_DIR : "", ...segs);
}

const PRODUCTS_PATH = siteJoin("products.json");
const ARCHIVE_PATH  = siteJoin("archive.json");

// OUT_DIR is relative to SITE_DIR
const OUT_DIR = (() => {
  const explicit = (process.env.OUT_DIR || "").trim();
  if (explicit) return explicit.replace(/^\/+/, "");
  return "p";
})();

// ================== HELPERS ==================
function norm(s) { return String(s ?? "").trim(); }
function upper(s) { return norm(s).toUpperCase(); }
function normalizeMarket(v) { return upper(v); }
function normalizeAsin(v) { return upper(v); }

function normalizeUrl(v) {
  const s = norm(v);
  if (!s) return "";
  if (/^https?:\/\//i.test(s)) return s;
  return "";
}

function safeHttps(url) { return norm(url).replace(/^http:\/\//i, "https://"); }

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
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, json, "utf8");
}

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

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

    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ",") { row.push(field); field = ""; i++; continue; }
    if (c === "\r") { i++; continue; }
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
  headers.forEach((h, idx) => { obj[h] = norm(cells[idx] ?? ""); });
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

// 纯数字 => 其它平台（Walmart 等），不展示（隐藏进 archive）
function isNonAmazonByAsin(asin) {
  return isAllDigits(asin);
}

function keyOf(p) {
  return `${upper(p.market)}|${upper(p.asin)}`;
}

function isValidHttpUrl(u) {
  return /^https?:\/\//i.test(norm(u));
}

// 简单去重：同 market+asin 只保留一个；如果后来的更完整则替换
function pickBetter(existing, incoming) {
  if (!existing) return incoming;

  const exLink = isValidHttpUrl(existing.link);
  const inLink = isValidHttpUrl(incoming.link);

  const exImg = !!norm(existing.image_url);
  const inImg = !!norm(incoming.image_url);

  const exTitleLen = norm(existing.title).length;
  const inTitleLen = norm(incoming.title).length;

  if (!exLink && inLink) return incoming;
  if (exLink === inLink && !exImg && inImg) return incoming;
  if (exLink === inLink && exImg === inImg && inTitleLen > exTitleLen) return incoming;

  return existing;
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
function toWeservOg(url) {
  const u = safeHttps(url);
  if (!u) return "";
  const cleaned = u.replace(/^https?:\/\//i, "");
  return `https://images.weserv.nl/?url=${encodeURIComponent(cleaned)}&w=1200&h=630&fit=cover&output=jpg`;
}

// 关键点：静态站点下，/p/... 不是 SPA rewrite，
// 所以独立页要先跳首页，再由首页把路由切到 /p/market/asin
function buildPreviewHtml({ market, asin, imageUrl }) {
  const mLower = String(market).toLowerCase();
  const asinKey = String(asin).toUpperCase();

  const pagePath = `/p/${encodeURIComponent(mLower)}/${encodeURIComponent(asinKey)}`;
  const landing = `${SITE_ORIGIN}/?to=${encodeURIComponent(pagePath)}`;

  const ogTitle = `Product Reference • ${String(market).toUpperCase()} • ${asinKey}`;
  const ogDesc =
    "Independent product reference. Purchases are completed on Amazon. As an Amazon Associate, we earn from qualifying purchases.";
  const ogImage = toWeservOg(imageUrl) || `${SITE_ORIGIN}/og-placeholder.jpg`;

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
  <meta property="og:url" content="${escapeHtml(SITE_ORIGIN + pagePath)}" />
  <meta property="og:image" content="${escapeHtml(ogImage)}" />
  <meta property="og:image:secure_url" content="${escapeHtml(ogImage)}" />

  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="${escapeHtml(ogTitle)}" />
  <meta name="twitter:description" content="${escapeHtml(ogDesc)}" />
  <meta name="twitter:image" content="${escapeHtml(ogImage)}" />

  <meta http-equiv="refresh" content="0;url=${escapeHtml(landing)}" />
  <noscript><meta http-equiv="refresh" content="0;url=${escapeHtml(landing)}" /></noscript>
</head>
<body>
<script>
  location.replace(${JSON.stringify(landing)});
</script>
</body>
</html>`;
}

function generatePPages(activeList, archiveList) {
  const outDirAbs = siteJoin(OUT_DIR);

  // 每次全量重建 /p，避免旧页面残留
  if (fs.existsSync(outDirAbs)) {
    fs.rmSync(outDirAbs, { recursive: true, force: true });
  }
  ensureDir(outDirAbs);

  const all = [...(activeList || []), ...(archiveList || [])].filter((p) => p && p.market && p.asin);

  // 去重：同 market+asin 只保留一个（输出稳定）
  const seen = new Set();
  const uniq = [];
  for (const p of all) {
    const k = keyOf(p);
    if (seen.has(k)) continue;
    seen.add(k);
    uniq.push(p);
  }

  // 稳定排序输出
  uniq.sort((a, b) => {
    const ma = upper(a.market), mb = upper(b.market);
    if (ma !== mb) return ma.localeCompare(mb);
    const aa = upper(a.asin), ab = upper(b.asin);
    if (aa !== ab) return aa.localeCompare(ab);
    return 0;
  });

  let count = 0;
  for (const p of uniq) {
    const market = upper(p.market);
    const asin = upper(p.asin);
    const img = norm(p.image_url);

    const dir = path.join(outDirAbs, market.toLowerCase(), asin);
    ensureDir(dir);

    const html = buildPreviewHtml({ market, asin, imageUrl: img });
    fs.writeFileSync(path.join(dir, "index.html"), html, "utf-8");
    count++;
  }

  console.log(`[p] generated ${count} pages under ${SITE_DIR ? SITE_DIR + "/" : ""}${OUT_DIR}`);
}

// ================== MAIN ==================
(async () => {
  console.log("[sync] CSV_URL =", CSV_URL);
  console.log("[sync] SITE_DIR =", SITE_DIR || "(repo root)");
  console.log("[sync] write products to =", PRODUCTS_PATH);
  console.log("[sync] write archive  to =", ARCHIVE_PATH);
  console.log("[sync] p out dir        =", siteJoin(OUT_DIR));

  const csvText = await fetchText(CSV_URL);
  console.log("[sync] CSV bytes =", csvText.length);

  const rows = parseCSV(csvText);
  if (!rows.length) throw new Error("CSV is empty");

  const headers = rows[0].map((h) => norm(h));
  const dataRows = rows.slice(1).filter((r) => r.some((c) => norm(c) !== ""));

  console.log("[sync] headers =", headers.join(" | "));
  console.log("[sync] data rows =", dataRows.length);

  const prevProductsRaw = safeReadJson(PRODUCTS_PATH, []);
  const prevArchiveRaw = safeReadJson(ARCHIVE_PATH, []);

  const prevProducts = Array.isArray(prevProductsRaw) ? prevProductsRaw : [];
  const prevArchive = Array.isArray(prevArchiveRaw) ? prevArchiveRaw : [];

  const prevMap = new Map();
  prevProducts.forEach((p) => prevMap.set(keyOf(p), p));

  const archiveMap = new Map();
  prevArchive.forEach((p) => archiveMap.set(keyOf(p), p));

  // Active 用 Map 去重（同 market+asin 只保留一个）
  const activeMap = new Map();
  let dupActive = 0;

  for (const r of dataRows) {
    const o = mapRow(headers, r);

    const market = normalizeMarket(o.market || o.Market || o.MARKET);
    const asin = normalizeAsin(o.asin || o.ASIN);
    if (!market || !asin) continue;

    const title = norm(o.title || o.Title || "");
    const link = normalizeUrl(o.link || o.Link);
    const image_url = norm(o.image_url || o.image || o.Image || o.imageUrl || "");

    // 纯数字 ASIN => 非 Amazon：隐藏进 archive
    if (isNonAmazonByAsin(asin)) {
      const nonAmazonItem = { market, asin, title, link, image_url, _hidden_reason: "non_amazon_numeric_asin" };
      const k = keyOf(nonAmazonItem);
      if (!archiveMap.has(k)) archiveMap.set(k, nonAmazonItem);
      continue;
    }

    // 下架 status：进 archive（确保独立页仍可打开）
    const statusVal = o.status ?? o.Status ?? o.STATUS;
    if (!isActiveStatus(statusVal)) {
      const archivedItem = { market, asin, title, link, image_url, _hidden_reason: "inactive_status" };
      const k = keyOf(archivedItem);
      if (!archiveMap.has(k)) archiveMap.set(k, archivedItem);
      continue;
    }

    // 上架：写入 activeMap（去重）
    const item = { market, asin, title, link, image_url };
    const k = keyOf(item);

    if (!activeMap.has(k)) {
      activeMap.set(k, item);
    } else {
      dupActive++;
      const kept = pickBetter(activeMap.get(k), item);
      activeMap.set(k, kept);
    }
  }

  const nextProducts = Array.from(activeMap.values());
  nextProducts.sort((a, b) => {
    const am = a.market.localeCompare(b.market);
    if (am) return am;
    return a.asin.localeCompare(b.asin);
  });

  // Move removed items into archive (present before, missing now)
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
  console.log("[sync] duplicate active rows dropped/replaced =", dupActive);
  console.log("[sync] removed from active =", removedCount);
  console.log("[sync] archive size =", nextArchive.length);

  writeJsonPretty(PRODUCTS_PATH, nextProducts);
  writeJsonPretty(ARCHIVE_PATH, nextArchive);
  console.log("[sync] wrote products.json & archive.json");

  // Generate /p pages (active + archive)
  generatePPages(nextProducts, nextArchive);

  console.log("[done] sync + generate /p pages completed");
})().catch((err) => {
  console.error("[fatal]", err?.stack || err);
  process.exit(1);
});
