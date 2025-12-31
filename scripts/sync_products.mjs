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

// 简单去重：同 market+asin 只保留一个；如果后来的更完整则替换
function pickBetter(existing, incoming) {
  if (!existing) return incoming;

  const exLink = isValidHttpUrl(existing.link);
  const inLink = isValidHttpUrl(incoming.link);

  const exImg = !!norm(existing.image_url);
  const inImg = !!norm(incoming.image_url);

  const exTitleLen = norm(existing.title).length;
  const inTitleLen = norm(incoming.title).length;

  const exKeyLen = norm(existing.keyword).length;
  const inKeyLen = norm(incoming.keyword).length;

  const exStoreLen = norm(existing.store).length;
  const inStoreLen = norm(incoming.store).length;

  const exRemarkLen = norm(existing.remark).length;
  const inRemarkLen = norm(incoming.remark).length;

  // 让“更靠前（CSV 顶部）”的行优先（你的 CSV：靠前=新；用于保持文档顺序）
  const exIdx = Number(existing._idx || 0);
  const inIdx = Number(incoming._idx || 0);

  if (!exLink && inLink) return incoming;
  if (exLink === inLink && !exImg && inImg) return incoming;

  // 字段更完整优先
  if (exLink === inLink && exImg === inImg && inTitleLen > exTitleLen) return incoming;
  if (exLink === inLink && exImg === inImg && inTitleLen === exTitleLen && inKeyLen > exKeyLen) return incoming;
  if (exLink === inLink && exImg === inImg && inTitleLen === exTitleLen && inKeyLen === exKeyLen && inStoreLen > exStoreLen) return incoming;
  if (exLink === inLink && exImg === inImg && inTitleLen === exTitleLen && inKeyLen === exKeyLen && inStoreLen === exStoreLen && inRemarkLen > exRemarkLen) return incoming;

  // 最后按 CSV 行号优先（更靠前更优先）
  if (exLink === inLink && exImg === inImg && inTitleLen === exTitleLen && inIdx < exIdx) return incoming;

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

// ================== OG IMAGE DOWNLOADER ==================
function guessExtFromContentType(ct) {
  const s = String(ct || "").toLowerCase();
  if (s.includes("image/jpeg") || s.includes("image/jpg")) return "jpg";
  if (s.includes("image/png")) return "png";
  // WhatsApp/FB 预览对 webp 经常不稳：直接回退 placeholder
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
  let url = safeHttps(imageUrl);
  if (!isValidHttpUrl(url)) return null;

  // ===== Amazon 图片特殊处理：FMwebp 强制改 FMjpg，避免返回 webp 导致失败 =====
  if (/^https?:\/\/m\.media-amazon\.com\/images\//i.test(url)) {
    url = url.replace(/_FMwebp_/gi, "_FMjpg_");
  }
  // =====================================================================

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

// ====== NEW (incremental OG reuse helpers) ======
function findExistingOgFile(ogDirAbs, market, asin) {
  const base = path.join(ogDirAbs, market.toLowerCase(), asin);
  const jpg = base + ".jpg";
  const png = base + ".png";
  if (fs.existsSync(jpg)) return { ext: "jpg", abs: jpg };
  if (fs.existsSync(png)) return { ext: "png", abs: png };
  return null;
}
function ogUrlFor(market, asin, ext) {
  return `${SITE_ORIGIN}/${OG_DIR}/${market.toLowerCase()}/${asin}.${ext}`;
}
// ====== END NEW ======

// ================== /p PAGE GENERATOR ==================
function buildPreviewHtml({ market, asin, ogImageUrl, dstUrl }) {
  const mLower = String(market).toLowerCase();
  const asinKey = String(asin).toUpperCase();

  const pagePath = `/p/${encodeURIComponent(mLower)}/${encodeURIComponent(asinKey)}`;
  const dst = String(dstUrl || "").trim();
  const landing = `${SITE_ORIGIN}/?to=${encodeURIComponent(pagePath)}` + (dst ? `&dst=${encodeURIComponent(dst)}` : "");

  const ogTitle = `${String(market).toUpperCase()} • ${asinKey}`;
  const ogDesc =
    "Independent product reference. Purchases are completed on Amazon. As an Amazon Associate, we earn from qualifying purchases.";

  // dstUrl: optional destination link from CSV (e.g., Walmart). It is forwarded to the front-end
  // template via ?dst=... so the template can set the primary CTA target without changing UI.

  const ogImage = ogImageUrl || OG_PLACEHOLDER_URL;
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

  // 为避免 Actions 失败时把既有 /p 清空导致“独立页瞬间全部失效”，这里采用“临时目录生成 + 原子替换”的方式：
  // 1) 生成到 /p__tmp
  // 2) 全部生成成功后，再替换为 /p
  const outTmpAbs = siteJoin(`${OUT_DIR}__tmp`);

  // 清理临时目录
  if (fs.existsSync(outTmpAbs)) fs.rmSync(outTmpAbs, { recursive: true, force: true });
  ensureDir(outTmpAbs);

  // /og 不再全量重建：保留已有文件，结合 manifest 仅为新增/变更项下载（大幅缩短耗时）
  ensureDir(ogDirAbs);

  // OG manifest（记录每个 market|asin 对应的源图与本地文件扩展名）
  const ogManifestPath = path.join(ogDirAbs, "og-manifest.json");
  const ogManifest = safeReadJson(ogManifestPath, {});

  // 注意：archiveList 现在是“全量最新库”，因此下架也会生成 /p 页，避免断链
  const all = [...(activeList || []), ...(archiveList || [])].filter((p) => p && p.market && p.asin);

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

  let pageCount = 0, ogOk = 0, ogFail = 0;

  for (const p of uniq) {
    const market = upper(p.market);
    const asin = upper(p.asin);
    const img = norm(p.image_url);

    // 1) OG image download -> /og/{market}/{asin}.jpg|png (incremental)
    // 规则：
    // - 如果 manifest 里记录的 src 与当前 image_url 相同，且本地文件存在 => 直接复用（跳过下载）
    // - 若 src 变化或本地文件不存在 => 尝试下载；成功则更新 manifest；失败则如有旧文件则继续复用，否则 placeholder
    let ogImageUrl = "";

    const k = keyOf({ market, asin });

    // 先尝试复用旧文件（若存在）
    const existing = findExistingOgFile(ogDirAbs, market, asin);
    const rec = ogManifest && typeof ogManifest === "object" ? (ogManifest[k] || null) : null;
    const desiredSrc = img ? safeHttps(img) : "";

    const canReuse =
      !!existing &&
      !!rec &&
      rec.ext === existing.ext &&
      String(rec.src || "") === String(desiredSrc || "");

    if (canReuse) {
      ogImageUrl = ogUrlFor(market, asin, existing.ext);
      ogOk++;
      // console.log(`[og] reuse ${market}/${asin}.${existing.ext}`);
    } else if (img) {
      // 需要下载或刷新
      try {
        const outNoExt = path.join(ogDirAbs, market.toLowerCase(), asin);
        const r = await downloadOgImage(img, outNoExt);
        if (r?.ext) {
          ogImageUrl = ogUrlFor(market, asin, r.ext);
          ogOk++;
          ogManifest[k] = {
            src: String(desiredSrc || ""),
            ext: r.ext,
            bytes: r.bytes,
            at: Date.now(),
          };
          console.log(`[og] ok ${market}/${asin}.${r.ext} (${r.bytes} bytes)`);
        } else {
          // 下载失败：如果已有旧文件，则继续用旧文件；否则 placeholder
          if (existing) {
            ogImageUrl = ogUrlFor(market, asin, existing.ext);
            ogOk++;
            console.log(`[og] keep-old ${market}/${asin}.${existing.ext} (download failed)`);
          } else {
            ogFail++;
            console.log(`[og] fail ${market}/${asin} -> placeholder`);
          }
        }
      } catch {
        if (existing) {
          ogImageUrl = ogUrlFor(market, asin, existing.ext);
          ogOk++;
          console.log(`[og] keep-old ${market}/${asin}.${existing.ext} (error)`);
        } else {
          ogFail++;
          console.log(`[og] error ${market}/${asin} -> placeholder`);
        }
      }
    } else {
      // 没有 image_url：若有旧文件仍可用；否则 placeholder
      if (existing) {
        ogImageUrl = ogUrlFor(market, asin, existing.ext);
        ogOk++;
        console.log(`[og] keep-old ${market}/${asin}.${existing.ext} (no image_url)`);
      } else {
        ogFail++;
        console.log(`[og] no image_url ${market}/${asin} -> placeholder`);
      }
    }

    // 2) /p page
    const dir = path.join(outTmpAbs, market.toLowerCase(), asin);
    ensureDir(dir);
    const html = buildPreviewHtml({ market, asin, ogImageUrl: ogImageUrl || OG_PLACEHOLDER_URL, dstUrl: p.link || "" });
    fs.writeFileSync(path.join(dir, "index.html"), html, "utf-8");
    pageCount++;
  }

// 3) 原子替换：用临时目录覆盖正式 /p 目录
try {
  if (fs.existsSync(outDirAbs)) fs.rmSync(outDirAbs, { recursive: true, force: true });
  fs.renameSync(outTmpAbs, outDirAbs);
} catch (e) {
  // 如果替换失败，至少不要把旧 /p 删除掉（上面已删），这里兜底为：保留 tmp 目录，便于排查
  console.warn("[warn] swap /p__tmp -> /p failed:", e?.message || e);
}

  console.log(`[p] generated ${pageCount} pages under ${SITE_DIR ? SITE_DIR + "/" : ""}${OUT_DIR}`);
  console.log(`[og] downloaded ok=${ogOk}, failed=${ogFail} (fallback to og-placeholder.jpg)`);

  // 写入 manifest（用于下次跳过未变更项）
  try {
    writeJsonPretty(ogManifestPath, ogManifest || {});
  } catch (e) {
    console.warn("[warn] write og-manifest.json failed:", e?.message || e);
  }
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

  const placeholderAbs = siteJoin(OG_PLACEHOLDER_PATH);
  if (!fs.existsSync(placeholderAbs)) {
    console.warn(`[warn] ${OG_PLACEHOLDER_PATH} not found at site root. Add it to repo root (or product-list root).`);
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

  const nowTs = Date.now();

  let rowIdx = 0;

  for (const r of dataRows) {
    const o = mapRow(headers, r);

    const market = normalizeMarket(o.market || o.Market || o.MARKET);
    const asin = normalizeAsin(o.asin || o.ASIN);
    if (!market || !asin) { rowIdx++; continue; }

    const title = norm(o.title || o.Title || "");
    const keyword = norm(o.keyword || o.Keyword || "");
    const store = norm(o.store || o.Store || "");
    const remark = norm(o.remark || o.Remark || "");

    const link = normalizeUrl(o.link || o.Link);
    const image_url = norm(o.image_url || o.image || o.Image || o.imageUrl || "");

    const statusVal = o.status ?? o.Status ?? o.STATUS;

    // prev 用于 _ts（首次出现时间）复用
    const prev = prevMap.get(keyOf({ market, asin })) || null;

    // 统一构建“完整 item”（无论 active / inactive 都可用）
    const baseItem = {
      market,
      asin,
      title,
      keyword,
      store,
      remark,
      status: norm(statusVal),          // 关键：保留原 status（即使为空也保留）
      link,
      image_url,
      _idx: rowIdx,                     // CSV 顺序（用于 tie-break）
      _ts: prev?._ts || nowTs,           // 首次出现时间（可选）
      _updated: nowTs,                   // 每次同步刷新（用于“新到旧”排序）
    };

    // 非 Amazon（纯数字）-> archive（全量库）
    if (isNonAmazonByAsin(asin)) {
      const item = { ...baseItem, _hidden_reason: "non_amazon_numeric_asin" };
      const k = keyOf(item);
      archiveMap.set(k, item);          // 关键：允许覆盖，保持最新
      rowIdx++;
      continue;
    }

    // status 下架 -> archive（全量库）
    if (!isActiveStatus(statusVal)) {
      const item = { ...baseItem, _hidden_reason: "inactive_status" };
      const k = keyOf(item);
      archiveMap.set(k, item);          // 关键：允许覆盖，保持最新
      rowIdx++;
      continue;
    }

    // 上架 -> active 去重（并同步写入 archive）
    const item = { ...baseItem };

    const k = keyOf(item);

    if (!activeMap.has(k)) activeMap.set(k, item);
    else {
      dupActive++;
      activeMap.set(k, pickBetter(activeMap.get(k), item));
    }

    // 关键：active 的最终版本也写入 archive（全量库始终最新完整）
    archiveMap.set(k, activeMap.get(k));

    rowIdx++;
  }

  // products.json 按 CSV 原始顺序输出（你的 CSV：靠前=新，列表应保持该顺序）
  const nextProducts = Array.from(activeMap.values()).sort((a, b) => {
    return Number(a._idx || 0) - Number(b._idx || 0);
  });

  // 从 active 消失的旧数据 -> archive（当 CSV 彻底删除该产品时仍保留历史）
  const nextKeys = new Set(nextProducts.map(keyOf));
  let removedCount = 0;
  for (const [k, oldP] of prevMap.entries()) {
    if (!nextKeys.has(k)) {
      if (!archiveMap.has(k)) {
        // 旧数据可能字段不全，但至少保留；并标记来源，便于排查
        archiveMap.set(k, { ...oldP, _hidden_reason: oldP?._hidden_reason || "removed_from_active" });
      }
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

  // 生成 /p + /og：无论 OG 成功与否都不应失败
  try {
    await generatePPagesAndOgImages(nextProducts, nextArchive);
  } catch (e) {
    console.warn("[warn] generate /p or /og failed, but continuing:", e?.message || e);
  }

  console.log("[done] sync + generate /p pages + /og images completed");
})().catch((err) => {
  console.error("[fatal]", err?.stack || err);
  process.exit(1);
});
