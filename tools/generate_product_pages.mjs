// tools/generate_product_pages.mjs
// Generate static preview pages with OG tags for WhatsApp/Telegram link previews.
// Output: /p/{marketLower}/{asinKey}/index.html
// Data source: /products.json (root)
// Key improvements:
// 1) No meta refresh (some crawlers stop parsing).
// 2) og:image uses a stable proxy (weserv) + direct image fallback.

import fs from "fs";
import path from "path";

const ROOT = process.cwd();

// Your public site origin (custom domain for GitHub Pages)
const SITE_ORIGIN = "https://ama.omino.top";

// Input data file
const PRODUCTS_JSON = path.join(ROOT, "products.json");

// Output root folder under repository root
const OUT_DIR = path.join(ROOT, "p");

// ===== Helpers =====
function safeText(s) {
  return String(s ?? "").trim();
}

function escHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizeMarket(m) {
  return safeText(m).toUpperCase();
}

function normalizeAsin(a) {
  return safeText(a).toUpperCase();
}

function ensureAbsUrl(u) {
  const s = safeText(u);
  if (!s) return "";
  if (/^https?:\/\//i.test(s)) return s;
  return "";
}

function toWeserv(url) {
  // weserv requires URL without protocol for best compatibility
  const cleaned = String(url || "").replace(/^https?:\/\//i, "");
  return "https://images.weserv.nl/?url=" + encodeURIComponent(cleaned);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function buildPageUrl(marketLower, asinKey) {
  return `${SITE_ORIGIN}/p/${marketLower}/${encodeURIComponent(asinKey)}`;
}

function buildOpenUrl(marketLower, asinKey) {
  // We will redirect humans to the SPA which understands /p/... route
  // Your index.html already added support for ?open=...
  const openPath = `/p/${marketLower}/${asinKey}`;
  return `${SITE_ORIGIN}/?open=${encodeURIComponent(openPath)}`;
}

// ===== Duplicate ASIN handling =====
// Same market + same asin may appear multiple times -> ASIN, ASIN_1, ASIN_2...
const counter = new Map();
function buildAsinKey(market, asin) {
  const k = `${market}|${asin}`;
  const n = counter.get(k) || 0;
  counter.set(k, n + 1);
  return n === 0 ? asin : `${asin}_${n}`;
}

// ===== HTML builder =====
function buildHtml({ market, marketLower, asinKey, asin, title, imageUrl }) {
  const pageUrl = buildPageUrl(marketLower, asinKey);
  const openUrl = buildOpenUrl(marketLower, asinKey);

  const ogTitle = title ? title : `ASIN ${asin}`;
  const ogDesc =
    "Independent product reference. Purchases are completed on Amazon. As an Amazon Associate, we earn from qualifying purchases.";

  // Improve preview reliability:
  // - Use image proxy first (Telegram/WhatsApp crawlers often fail on Amazon CDN)
  // - Keep direct image as fallback
  const ogImageProxy = toWeserv(imageUrl);
  const ogImageDirect = imageUrl;

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>

  <title>${escHtml(ogTitle)} • ${escHtml(market)} • Product Picks</title>
  <meta name="description" content="${escHtml(ogDesc)}"/>

  <meta property="og:type" content="product"/>
  <meta property="og:site_name" content="Product Picks"/>
  <meta property="og:title" content="${escHtml(ogTitle)}"/>
  <meta property="og:description" content="${escHtml(ogDesc)}"/>
  <meta property="og:url" content="${escHtml(pageUrl)}"/>

  <!-- Prefer proxy image for higher crawler success -->
  <meta property="og:image" content="${escHtml(ogImageProxy)}"/>
  <!-- Fallback to direct image -->
  <meta property="og:image" content="${escHtml(ogImageDirect)}"/>
  <meta property="og:image:width" content="1200"/>
  <meta property="og:image:height" content="630"/>

  <meta name="twitter:card" content="summary_large_image"/>
  <meta name="twitter:title" content="${escHtml(ogTitle)}"/>
  <meta name="twitter:description" content="${escHtml(ogDesc)}"/>
  <meta name="twitter:image" content="${escHtml(ogImageProxy)}"/>

  <meta name="robots" content="index,follow"/>
</head>
<body>
  <!-- Important: do NOT use meta refresh; redirect via JS for humans only. -->
  <script>
    // JS redirect for humans; most preview crawlers do not execute JS.
    location.replace(${JSON.stringify(openUrl)});
  </script>

  <noscript>
    <p>Redirecting… <a href="${escHtml(openUrl)}">Open product page</a></p>
  </noscript>
</body>
</html>`;
}

// ===== Main =====
if (!fs.existsSync(PRODUCTS_JSON)) {
  console.error("Missing products.json at:", PRODUCTS_JSON);
  process.exit(1);
}

let items = [];
try {
  items = JSON.parse(fs.readFileSync(PRODUCTS_JSON, "utf-8"));
} catch (e) {
  console.error("products.json parse error:", e);
  process.exit(1);
}

if (!Array.isArray(items)) {
  console.error("products.json must be an array");
  process.exit(1);
}

// Optional: clean old outputs to avoid stale pages
if (fs.existsSync(OUT_DIR)) {
  fs.rmSync(OUT_DIR, { recursive: true, force: true });
}
ensureDir(OUT_DIR);

let count = 0;
let skipped = 0;

for (const it of items) {
  const market = normalizeMarket(it.market ?? it.Market ?? it.MARKET);
  const asin = normalizeAsin(it.asin ?? it.ASIN);
  const title = safeText(it.title ?? it.Title);
  const imageUrl = ensureAbsUrl(it.image_url ?? it.imageUrl ?? it.image ?? it.Image);

  // Required fields for previews: market + asin + image_url
  if (!market || !asin || !imageUrl) {
    skipped++;
    continue;
  }

  const marketLower = market.toLowerCase();
  const asinKey = buildAsinKey(market, asin);

  const dir = path.join(OUT_DIR, marketLower, asinKey);
  ensureDir(dir);

  const html = buildHtml({ market, marketLower, asinKey, asin, title, imageUrl });
  fs.writeFileSync(path.join(dir, "index.html"), html, "utf-8");

  count++;
}

console.log(`Generated ${count} product preview pages under /p`);
if (skipped) console.log(`Skipped ${skipped} items missing market/asin/image_url`);
