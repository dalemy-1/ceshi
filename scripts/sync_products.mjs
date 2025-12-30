// scripts/sync_products.mjs
// One-shot pipeline:
// 1) Fetch CSV from CSV_URL (source of truth)
// 2) Rebuild products.json + archive.json
// 3) Generate /p/{market}/{asin}/index.html pages (share URLs; include OG tags)
// 4) Download OG images to /og/{market}/{asin}.(jpg|png) for WhatsApp previews (no weserv)
//
// Required env:
//   CSV_URL=http(s)://.../export_csv
//
// Optional env:
//   SITE_ORIGIN=https://ama.omino.top
//   OUT_DIR=p
//   SITE_DIR=product-list   (auto-detected if repo has product-list/index.html)

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
    fs.existsSync(path.join(ROOT, "product-list", "index.html")) ||
    fs.existsSync(path.join(ROOT, "product-list", "products.json"))
  ) return "product-list";
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

// OG output
const OG_DIR = "og";
const OG_PLACEHOLDER_PATH = "og-placeholder.jpg"; // file should exist at site root
const OG_PLACEHOLDER_URL = `${SITE_ORIGIN}/${OG_PLACEHOLDER_PATH}`;

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
    return JSON.parse(fs.readFileSync(file, "utf8"));
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
  let i = 0, field = "", row = [], inQuotes = false;

  while (i < text.length) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += c; i++; continue;
    }

    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ",") { row.push(field); field = ""; i++; continue; }
    if (c === "\r") { i++; continue; }
    if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; i++; continue; }

    field += c; i++;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
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
function isNonAmazonByAsin(asin) { return isAllDigits(asin); }

function keyOf(p) { return `${upper(p.market)}|${upper(p.asin)}`; }

function isValidHttpUrl(u) { return /^https?:\/\//i.test(norm(u)); }

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

function sameCoreFields(a, b) {
  // 用于判断“本次 CSV 是否真正更新了这个产品”
  // 只比较会影响展示/跳转/预览的字段
  return (
    upper(a?.market) === upper(b?.market) &&
    upper(a?.asin) === upper(b?.asin) &&
    norm(a?.title) === norm(b?.title) &&
    norm(a?.link) === norm(b?.link) &&
    norm(a?.image_url) === norm(b?.image_url)
  );
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

// ================== OG IMAGE DOWNLOADER ==================
function guessExtFromContentType(ct) {
  const s = String(ct || "").toLowerCase();
  if (s.includes("image/jpeg") || s.includes("image/jpg")) return "jpg";
  if (s.includes("image/png")) return "png";
  // WhatsApp/FB 预览对 webp 经常不稳，直接当失败，用 placeholder
  return "";
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

/**
 * 下载单张OG图：
 * - 永不抛异常（失败返回 null）
 * - 超时中断
 * - 重试 2 次
 */
async function downloadOgImage(imageUrl, outAbsNoExt) {
  const url = safeHttps(imageUrl);
  if (!isValidHttpUrl(url)) return null;

  const headers = {
    "user-agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
  };

  const tryOnce = async () => {
    const res = await fetchWithTimeout(url, { redirect: "follow", headers }, 8000);
    if (!res || !res.ok) return null;

    const ct = res.headers.get("content-type") || "";
    const ext = guessExtFromContentType(ct);
    if (!ext) return null;

    const buf = Buffer.from(await res.arrayBuffer());
    // 防呆：太小往往是错误页/拦截页
    if (!buf || buf.length < 4096) return null;

    const outAbs = `${outAbsNoExt}.${ext}`;
    ensureDir(path.dirname(outAbs));
    fs.writeFileSync(outAbs, buf);
    return { ext, bytes: buf.length };
  };

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const r = await tryOnce();
      if (r) return r;
    } catch {
      // swallow
    }
  }
  return null;
}

// ================== /p PAGE GENERATOR ==================
function buildPreviewHtml({ market, asin, ogImageUrl }) {
  const mLower = String(market).toLowerCase();
  const asinKey = String(asin).toUpperCase();

  const pagePath = `/p/${encodeURIComponent(mLower)}/${encodeURIComponent(asinKey)}`;

  // 独立页不做 302，保持 OG 标签可被抓取；页面再用 meta refresh / JS 跳转
  const landing = `${SITE_ORIGIN}/?to=${encodeURIComponent(pagePath)}`;

  // 你要求：只显示 “US • B07J43CWC3” 这种标题
  const ogTitle = `${String(market).toUpperCase()} • ${asinKey}`;

  const ogDesc =
    "Independent product reference. Purchases are completed on Amazon. As an Amazon Associate, we earn from qualifying purchases.";

  const ogImage = ogImageUrl || OG_PLACEHOLDER_URL;

  // WhatsApp/FB 抓取更稳定：补齐 type/width/height
  const ogType = ogImage.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg";

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>${escapeHtml(ogTitle)}</title>
  <meta name="description" content="${escapeHtml(ogDesc)}" />

  <meta property="og:type" content="website" />
  <meta property="og:site_name" content="Product Picks" />
  <meta property="og:title" content="${escapeHtml(ogTitle)}" />
  <meta property="og:description" content="${escapeHtml(ogDesc)}" />
  <meta property="og:url" content="${escapeHtml(SITE_ORIGIN + pagePath)}" />
  <meta property="og:image" content="${escapeHtml(ogImage)}" />
  <meta property="og:image:secure_url" content="${escapeHtml(ogImage)}" />
  <meta property="og:image:type" content="${ogType}" />
  <meta property="og:image:width" content="1200" />
  <meta property="og:image:height" content="630" />

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

  // 全量重建 /p
  if (fs.existsSync(outDirAbs)) fs.rmSync(outDirAbs, { recursive: true, force: true });
  ensureDir(outDirAbs);

  // /og 也重建（只删目录，不影响根部的 og-placeholder.jpg）
  if (fs.existsSync(ogDirAbs)) fs.rmSync(ogDirAbs, { recursive: true, force: true });
  ensureDir(ogDirAbs);

  const all = [...(activeList || []), ...(archiveList || [])].filter((p) => p && p.market && p.asin);

  // 去重：同 market+asin 只保留一个
  const seen = new Set();
  const uniq = [];
  for (const p of all) {
    const k = keyOf(p);
    if (seen.has(k)) continue;
    seen.add(k);
    uniq.push(p);
  }

  // 稳定排序输出（生成静态文件用，和列表显示排序无关）
  uniq.sort((a, b) => {
    const ma = upper(a.market), mb = upper(b.market);
    if (ma !== mb) return ma.localeCompare(mb);
    const aa = upper(a.asin), ab = upper(b.asin);
    if (aa !== ab) return aa.localeCompare(ab);
    return 0;
  });

  let pageCount = 0, ogOk = 0, ogFail = 0;

  for (const p of uniq) {
    const market = upper(p.market);
    const asin = upper(p.asin);
    const img = norm(p.image_url);

    // 1) OG image download -> /og/{market}/{asin}.jpg|png
    let ogImageUrl = "";
    if (img) {
      const outNoExt = path.join(ogDirAbs, market.toLowerCase(), asin);
      const r = await downloadOgImage(img, outNoExt);
      if (r?.ext) {
        ogImageUrl = `${SITE_ORIGIN}/${OG_DIR}/${market.toLowerCase()}/${asin}.${r.ext}`;
        ogOk++;
        console.log(`[og] ok ${market}/${asin}.${r.ext} (${r.bytes} bytes)`);
      } else {
        ogFail++;
        console.log(`[og] fail ${market}/${asin} -> placeholder`);
      }
    } else {
      ogFail++;
      console.log(`[og] no image_url ${market}/${asin} -> placeholder`);
    }

    // 2) /p page
    const dir = path.join(outDirAbs, market.toLowerCase(), asin);
    ensureDir(dir);
    const html = buildPreviewHtml({ market, asin, ogImageUrl: ogImageUrl || OG_PLACEHOLDER_URL });
    fs.writeFileSync(path.join(dir, "index.html"), html, "utf-8");
    pageCount++;
  }

  console.log(`[p] generated ${pageCount} pages under ${SITE_DIR ? SITE_DIR + "/" : ""}${OUT_DIR}`);
  console.log(`[og] downloaded ok=${ogOk}, failed=${ogFail} (fallback to og-placeholder.jpg)`);
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

  // 先确保 placeholder 文件存在（避免 OG 全挂）
  const placeholderAbs = siteJoin(OG_PLACEHOLDER_PATH);
  if (!fs.existsSync(placeholderAbs)) {
    console.warn(`[warn] ${OG_PLACEHOLDER_PATH} not found at site root. Please add it (repo root or SITE_DIR root).`);
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

  // prevMap / archiveMap：key 必须用 keyOf()（全大写），避免你之前遇到的 prev 取不到
  const prevMap = new Map();
  prevProducts.forEach((p) => prevMap.set(keyOf(p), p));

  const archiveMap = new Map();
  prevArchive.forEach((p) => archiveMap.set(keyOf(p), p));

  const activeMap = new Map();
  let dupActive = 0;

  // 这一次同步的“批次时间”
  const batchNow = Date.now();

  for (const r of dataRows) {
    const o = mapRow(headers, r);

    const market = normalizeMarket(o.market || o.Market || o.MARKET);
    const asin = normalizeAsin(o.asin || o.ASIN);
    if (!market || !asin) continue;

    const title = norm(o.title || o.Title || "");
    const link = normalizeUrl(o.link || o.Link);
    const image_url = norm(o.image_url || o.image || o.Image || o.imageUrl || "");

    // 非 Amazon（纯数字）-> archive
    if (isNonAmazonByAsin(asin)) {
      const item = { market, asin, title, link, image_url, _hidden_reason: "non_amazon_numeric_asin" };
      const k = keyOf(item);
      if (!archiveMap.has(k)) archiveMap.set(k, item);
      continue;
    }

    // status 下架 -> archive
    const statusVal = o.status ?? o.Status ?? o.STATUS;
    if (!isActiveStatus(statusVal)) {
      const item = { market, asin, title, link, image_url, _hidden_reason: "inactive_status" };
      const k = keyOf(item);
      if (!archiveMap.has(k)) archiveMap.set(k, item);
      continue;
    }

    // 上架 -> active
    const k = keyOf({ market, asin });     // ✅ 统一 key
    const prev = prevMap.get(k) || null;

    const draft = { market, asin, title, link, image_url };

    // ✅ 排序用更新时间：只有当“核心字段变化 or 新增”时才更新 _updated
    const isChanged = !prev || !sameCoreFields(prev, draft);

    const item = {
      ...draft,
      _ts: prev?._ts || batchNow,                         // 首次出现时间
      _updated: isChanged ? batchNow : (prev?._updated || prev?._ts || batchNow) // 变化才更新时间
    };

    if (!activeMap.has(k)) {
      activeMap.set(k, item);
    } else {
      dupActive++;
      // 注意：pickBetter 可能挑出“更完整”的字段；如果 pickBetter 改变了字段，也应视为 changed
      const kept = pickBetter(activeMap.get(k), item);
      activeMap.set(k, kept);
    }
  }

  // ================== LIST SORT (你要求：新增/更新在上) ==================
  // 先按 _updated 倒序；再按 _ts 倒序；最后按 market/asin 保证稳定
  const nextProducts = Array.from(activeMap.values()).sort((a, b) => {
    const ua = Number(a._updated || 0);
    const ub = Number(b._updated || 0);
    if (ua !== ub) return ub - ua;

    const ta = Number(a._ts || 0);
    const tb = Number(b._ts || 0);
    if (ta !== tb) return tb - ta;

    const am = upper(a.market).localeCompare(upper(b.market));
    if (am) return am;
    return upper(a.asin).localeCompare(upper(b.asin));
  });

  // 从 active 消失的旧数据 -> archive
  const nextKeys = new Set(nextProducts.map(keyOf));
  let removedCount = 0;
  for (const [k, oldP] of prevMap.entries()) {
    if (!nextKeys.has(k)) {
      if (!archiveMap.has(k)) archiveMap.set(k, oldP);
      removedCount++;
    }
  }

  const nextArchive = Array.from(archiveMap.values()).sort((a, b) => {
    const am = upper(a.market).localeCompare(upper(b.market));
    if (am) return am;
    return upper(a.asin).localeCompare(upper(b.asin));
  });

  console.log("[sync] next active =", nextProducts.length);
  console.log("[sync] duplicate active rows dropped/replaced =", dupActive);
  console.log("[sync] removed from active =", removedCount);
  console.log("[sync] archive size =", nextArchive.length);

  writeJsonPretty(PRODUCTS_PATH, nextProducts);
  writeJsonPretty(ARCHIVE_PATH, nextArchive);
  console.log("[sync] wrote products.json & archive.json");

  // 生成 /p + /og（即使 og 下载大量失败，也不会让 workflow 失败）
  await generatePPagesAndOgImages(nextProducts, nextArchive);

  console.log("[done] sync + generate /p pages + /og images completed");
})().catch((err) => {
  console.error("[fatal]", err?.stack || err);
  process.exit(1);
});
