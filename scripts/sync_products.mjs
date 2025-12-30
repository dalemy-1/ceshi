// scripts/sync_products.mjs
// One-shot pipeline:
// 1) Fetch CSV from CSV_URL (source of truth)
// 2) Rebuild products.json + archive.json
// 3) Generate /p/{market}/{asin}/index.html pages (stable share URLs -> redirect to SPA)
// 4) Generate /og/{market}/{asin}.(jpg|png|webp) for WhatsApp previews (best-effort; never fail the job)
//
// Required env:
//   CSV_URL=http(s)://.../export_csv
//
// Optional env:
//   SITE_ORIGIN=https://ama.omino.top
//   OUT_DIR=p
//   SITE_DIR=product-list   (if your GitHub Pages publishes a subfolder)
//   OG_DIR=og
//   OG_MAX_PER_RUN=120      (download at most N OG images per run; best for stability)

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

const ROOT = process.cwd();
const SITE_DIR = (() => {
  const explicit = (process.env.SITE_DIR || "").trim();
  if (explicit) return explicit.replace(/^\/+|\/+$/g, "");
  if (
    fs.existsSync(path.join(ROOT, "product-list", "products.json")) ||
    fs.existsSync(path.join(ROOT, "product-list", "index.html"))
  ) {
    return "product-list";
  }
  return "";
})();

function siteJoin(...segs) {
  return path.join(ROOT, SITE_DIR ? SITE_DIR : "", ...segs);
}

const PRODUCTS_PATH = siteJoin("products.json");
const ARCHIVE_PATH = siteJoin("archive.json");

const OUT_DIR = (() => {
  const explicit = (process.env.OUT_DIR || "").trim();
  if (explicit) return explicit.replace(/^\/+/, "");
  return "p";
})();

const OG_DIR = (process.env.OG_DIR || "og").trim().replace(/^\/+/, "") || "og";
const OG_PLACEHOLDER_FILE = "og-placeholder.jpg"; // must exist at site root
const OG_PLACEHOLDER_URL = `${SITE_ORIGIN}/${OG_PLACEHOLDER_FILE}`;

// limit OG downloads per run (stability)
const OG_MAX_PER_RUN = Math.max(0, parseInt(process.env.OG_MAX_PER_RUN || "120", 10) || 120);

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

// Minimal CSV parser (quoted fields supported)
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
  if (!s) return true;
  return ["1", "true", "yes", "on", "active", "enabled", "publish", "published", "online"].includes(s);
}

function isAllDigits(s) {
  const x = norm(s);
  return x !== "" && /^[0-9]+$/.test(x);
}

// numeric-only "asin" => hide into archive
function isNonAmazonByAsin(asin) {
  return isAllDigits(asin);
}

function keyOf(p) {
  return `${upper(p.market)}|${upper(p.asin)}`;
}

function isValidHttpUrl(u) {
  return /^https?:\/\//i.test(norm(u));
}

// keep better record when duplicate
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

// ================== OG IMAGE (best-effort; never fatal) ==================
function guessExtFromContentType(ct) {
  const s = String(ct || "").toLowerCase();
  if (s.includes("image/jpeg") || s.includes("image/jpg")) return "jpg";
  if (s.includes("image/png")) return "png";
  if (s.includes("image/webp")) return "webp";
  return "";
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 10000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: ctrl.signal, redirect: "follow" });
  } finally {
    clearTimeout(t);
  }
}

function pickExistingOgFile(outNoExtAbs) {
  const exts = ["jpg", "png", "webp"];
  for (const ext of exts) {
    const p = `${outNoExtAbs}.${ext}`;
    if (fs.existsSync(p)) return { ext, abs: p };
  }
  return null;
}

/**
 * Download og image:
 * - returns {ext, bytes} or null
 * - NEVER throws outward (best-effort)
 */
async function downloadOgImageBestEffort(imageUrl, outAbsNoExt) {
  try {
    const url = safeHttps(imageUrl);
    if (!isValidHttpUrl(url)) return null;

    // if already exists, skip
    const existing = pickExistingOgFile(outAbsNoExt);
    if (existing) return { ext: existing.ext, bytes: fs.statSync(existing.abs).size, skipped: true };

    const headers = {
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
      "accept-language": "en-US,en;q=0.9",
    };

    const tryOnce = async () => {
      const res = await fetchWithTimeout(url, { headers }, 10000);
      if (!res || !res.ok) return null;

      const ct = res.headers.get("content-type") || "";
      const ext = guessExtFromContentType(ct);
      if (!ext) return null;

      const buf = Buffer.from(await res.arrayBuffer());
      // 防呆：过小通常是错误页/重定向页
      if (!buf || buf.length < 2048) return null;

      const outAbs = `${outAbsNoExt}.${ext}`;
      ensureDir(path.dirname(outAbs));
      fs.writeFileSync(outAbs, buf);
      return { ext, bytes: buf.length };
    };

    // retry 2 times
    for (let attempt = 1; attempt <= 2; attempt++) {
      const r = await tryOnce().catch(() => null);
      if (r) return r;
    }
    return null;
  } catch {
    return null;
  }
}

// ================== /p PAGE GENERATOR ==================
function buildPreviewHtml({ market, asin, ogImageUrl }) {
  const mLower = String(market).toLowerCase();
  const asinKey = String(asin).toUpperCase();

  const pagePath = `/p/${encodeURIComponent(mLower)}/${encodeURIComponent(asinKey)}`;
  const landing = `${SITE_ORIGIN}/?to=${encodeURIComponent(pagePath)}`;

  const ogTitle = `Product Reference • ${String(market).toUpperCase()} • ${asinKey}`;
  const ogDesc =
    "Independent product reference. Purchases are completed on Amazon. As an Amazon Associate, we earn from qualifying purchases.";

  const ogImage = ogImageUrl || OG_PLACEHOLDER_URL;

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

async function generatePPagesAndOgImages(activeList, archiveList) {
  const outDirAbs = siteJoin(OUT_DIR);
  const ogDirAbs = siteJoin(OG_DIR);

  // 每次全量重建 /p（保证删除旧页面）
  if (fs.existsSync(outDirAbs)) fs.rmSync(outDirAbs, { recursive: true, force: true });
  ensureDir(outDirAbs);

  // /og 不再每次清空：只补齐缺失，避免频繁全量下载导致失败
  ensureDir(ogDirAbs);

  const all = [...(activeList || []), ...(archiveList || [])].filter((p) => p && p.market && p.asin);

  // uniq by market+asin
  const seen = new Set();
  const uniq = [];
  for (const p of all) {
    const k = keyOf(p);
    if (seen.has(k)) continue;
    seen.add(k);
    uniq.push(p);
  }

  uniq.sort((a, b) => {
    const ma = upper(a.market), mb = upper(b.market);
    if (ma !== mb) return ma.localeCompare(mb);
    const aa = upper(a.asin), ab = upper(b.asin);
    if (aa !== ab) return aa.localeCompare(ab);
    return 0;
  });

  let pageCount = 0;
  let ogOk = 0;
  let ogSkip = 0;
  let ogFail = 0;
  let ogTried = 0;

  for (const p of uniq) {
    const market = upper(p.market);
    const asin = upper(p.asin);
    const img = norm(p.image_url);

    // 1) best-effort og image (limited per run)
    let ogImageUrl = "";
    const outNoExt = path.join(ogDirAbs, market.toLowerCase(), asin);

    // if already exists, use it
    const existing = pickExistingOgFile(outNoExt);
    if (existing) {
      ogImageUrl = `${SITE_ORIGIN}/${OG_DIR}/${market.toLowerCase()}/${asin}.${existing.ext}`;
      ogSkip++;
    } else if (img && ogTried < OG_MAX_PER_RUN) {
      ogTried++;
      const r = await downloadOgImageBestEffort(img, outNoExt);
      if (r && r.ext) {
        ogImageUrl = `${SITE_ORIGIN}/${OG_DIR}/${market.toLowerCase()}/${asin}.${r.ext}`;
        if (r.skipped) ogSkip++;
        else ogOk++;
        console.log(`[og] ok ${market}/${asin}.${r.ext} (${r.bytes} bytes)`);
      } else {
        ogFail++;
      }
    } else {
      // no image or exceeded cap
      if (!img) ogFail++;
    }

    // 2) generate /p page with og image fallback
    const dir = path.join(outDirAbs, market.toLowerCase(), asin);
    ensureDir(dir);

    const html = buildPreviewHtml({ market, asin, ogImageUrl: ogImageUrl || OG_PLACEHOLDER_URL });
    fs.writeFileSync(path.join(dir, "index.html"), html, "utf-8");
    pageCount++;
  }

  console.log(`[p] generated ${pageCount} pages under ${SITE_DIR ? SITE_DIR + "/" : ""}${OUT_DIR}`);
  console.log(`[og] tried=${ogTried}, ok=${ogOk}, skipped(existing)=${ogSkip}, failed=${ogFail}, cap=${OG_MAX_PER_RUN}`);
  console.log(`[og] fallback placeholder = ${OG_PLACEHOLDER_URL}`);
}

// ================== MAIN ==================
(async () => {
  console.log("[sync] CSV_URL =", CSV_URL);
  console.log("[sync] SITE_DIR =", SITE_DIR || "(repo root)");
  console.log("[sync] write products to =", PRODUCTS_PATH);
  console.log("[sync] write archive  to =", ARCHIVE_PATH);
  console.log("[sync] p out dir        =", siteJoin(OUT_DIR));
  console.log("[sync] og out dir       =", siteJoin(OG_DIR));
  console.log("[sync] og placeholder   =", OG_PLACEHOLDER_URL);
  console.log("[sync] og max per run   =", OG_MAX_PER_RUN);

  // Ensure placeholder exists (warn only; do not fail)
  const placeholderAbs = siteJoin(OG_PLACEHOLDER_FILE);
  if (!fs.existsSync(placeholderAbs)) {
    console.warn(`[warn] Missing ${OG_PLACEHOLDER_FILE} at site root: ${placeholderAbs}`);
  }

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

    if (isNonAmazonByAsin(asin)) {
      const nonAmazonItem = { market, asin, title, link, image_url, _hidden_reason: "non_amazon_numeric_asin" };
      const k = keyOf(nonAmazonItem);
      if (!archiveMap.has(k)) archiveMap.set(k, nonAmazonItem);
      continue;
    }

    const statusVal = o.status ?? o.Status ?? o.STATUS;
    if (!isActiveStatus(statusVal)) {
      const archivedItem = { market, asin, title, link, image_url, _hidden_reason: "inactive_status" };
      const k = keyOf(archivedItem);
      if (!archiveMap.has(k)) archiveMap.set(k, archivedItem);
      continue;
    }

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

  await generatePPagesAndOgImages(nextProducts, nextArchive);

  console.log("[done] sync + generate /p pages + /og images completed");
})().catch((err) => {
  console.error("[fatal]", err?.stack || err);
  process.exit(1);
});
