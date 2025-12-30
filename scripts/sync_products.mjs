#!/usr/bin/env node
/**
 * sync_from_csv.mjs
 *
 * Inputs:
 *  - CSV_FILE: local path to downloaded csv (preferred)
 *  - CSV_URL : remote url to download csv if CSV_FILE not provided
 *
 * Outputs:
 *  - products.json : active products only (source of truth for listing)
 *  - archive.json  : removed/offline products (for keeping individual pages alive)
 *
 * Behavior:
 *  - products.json becomes an exact mirror of "active" rows in CSV
 *  - anything not present in active set is moved/kept in archive.json
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import http from "http";
import https from "https";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// repo root assumed: scripts/ is at repoRoot/scripts
const REPO_ROOT = path.resolve(__dirname, "..");

const PRODUCTS_JSON = path.join(REPO_ROOT, "products.json");
const ARCHIVE_JSON = path.join(REPO_ROOT, "archive.json");

const CSV_FILE = (process.env.CSV_FILE || "").trim();
const CSV_URL = (process.env.CSV_URL || "").trim();

// ---------- helpers ----------
function nowISO() {
  return new Date().toISOString();
}

function safeUpper(s) {
  return (s || "").toString().trim().toUpperCase();
}

function normStr(s) {
  return (s ?? "").toString().trim();
}

function makeKey(market, asin) {
  return `${safeUpper(market)}::${normStr(asin).trim()}`;
}

function looksLikeHTML(text) {
  const t = (text || "").slice(0, 400).toLowerCase();
  return t.includes("<html") || t.includes("<!doctype") || t.includes("<body") || t.includes("</html>");
}

function headerContainsAsin(headerLine) {
  return headerLine.toLowerCase().includes("asin");
}

function detectDelimiter(headerLine) {
  // Most exports are comma. Sometimes semicolon.
  const comma = (headerLine.match(/,/g) || []).length;
  const semi = (headerLine.match(/;/g) || []).length;
  return semi > comma ? ";" : ",";
}

/**
 * Minimal CSV parser supporting quoted fields and delimiters.
 * Returns array of arrays.
 */
function parseCSV(text, delimiter = ",") {
  const rows = [];
  let row = [];
  let field = "";
  let i = 0;
  let inQuotes = false;

  while (i < text.length) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        // escaped quote
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += c;
      i += 1;
      continue;
    }

    if (c === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }

    if (c === delimiter) {
      row.push(field);
      field = "";
      i += 1;
      continue;
    }

    if (c === "\r") {
      // ignore; handle on \n
      i += 1;
      continue;
    }

    if (c === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      i += 1;
      continue;
    }

    field += c;
    i += 1;
  }

  // last field
  row.push(field);
  rows.push(row);

  // Remove trailing empty rows
  while (rows.length && rows[rows.length - 1].every((x) => normStr(x) === "")) rows.pop();

  return rows;
}

async function readJSONIfExists(filePath, fallback) {
  try {
    const buf = await fs.promises.readFile(filePath, "utf-8");
    return JSON.parse(buf);
  } catch (e) {
    return fallback;
  }
}

function normalizeHeader(h) {
  return normStr(h).toLowerCase().replace(/\s+/g, " ").trim();
}

function buildHeaderMap(headers) {
  const map = new Map();
  headers.forEach((h, idx) => {
    map.set(normalizeHeader(h), idx);
  });

  // helper for alternative names
  const alias = (name, alternatives) => {
    if (map.has(name)) return;
    for (const a of alternatives) {
      const k = normalizeHeader(a);
      if (map.has(k)) {
        map.set(name, map.get(k));
        return;
      }
    }
  };

  alias("market", ["Market", "country", "Country", "site", "Site"]);
  alias("asin", ["ASIN", "Asin"]);
  alias("title", ["Title", "name", "Name"]);
  alias("link", ["Link", "url", "URL"]);
  alias("image_url", ["image", "Image", "image url", "Image URL", "image_url"]);
  alias("remark", ["Remark", "note", "Note"]);
  alias("status", ["Status", "state", "State"]);
  alias("discount price", ["Discount Price", "discount", "Discount"]);
  alias("commission", ["Commission", "fee", "Fee"]);

  return map;
}

function getField(row, headerMap, key) {
  const idx = headerMap.get(key);
  if (idx === undefined) return "";
  return row[idx] ?? "";
}

function isOfflineByStatus(statusRaw) {
  const s = normStr(statusRaw).toLowerCase();
  if (!s) return false; // empty = treat active (you can change if needed)

  // common “inactive/offline” markers
  const offlineTokens = [
    "off",
    "offline",
    "inactive",
    "disabled",
    "down",
    "removed",
    "delete",
    "deleted",
    "0",
    "false",
    "no",
    "stop",
    "stopped",
  ];
  return offlineTokens.some((t) => s === t || s.includes(t));
}

async function fetchTextWithRetry(url, retry = 4) {
  const tryOnce = () =>
    new Promise((resolve, reject) => {
      const lib = url.startsWith("https") ? https : http;
      const req = lib.get(
        url,
        {
          headers: {
            "Cache-Control": "no-cache",
            Pragma: "no-cache",
            "User-Agent": "github-actions-sync/1.0",
          },
          timeout: 20000,
        },
        (res) => {
          if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            // follow redirect
            resolve(fetchTextWithRetry(res.headers.location, 1));
            req.destroy();
            return;
          }
          let data = "";
          res.setEncoding("utf8");
          res.on("data", (chunk) => (data += chunk));
          res.on("end", () => resolve(data));
        }
      );
      req.on("error", reject);
      req.on("timeout", () => {
        req.destroy(new Error("Request timeout"));
      });
    });

  let lastErr;
  for (let i = 0; i <= retry; i++) {
    try {
      return await tryOnce();
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, 1000 * (i + 1)));
    }
  }
  throw lastErr;
}

// ---------- main ----------
(async function main() {
  console.log(`[sync] start: ${nowISO()}`);

  // 1) Load previous files
  const prevProducts = await readJSONIfExists(PRODUCTS_JSON, []);
  const prevArchive = await readJSONIfExists(ARCHIVE_JSON, []);
  const archiveMap = new Map();
  for (const item of Array.isArray(prevArchive) ? prevArchive : []) {
    if (!item) continue;
    const key = makeKey(item.market, item.asin);
    if (!key.includes("::")) continue;
    archiveMap.set(key, item);
  }

  const prevActiveMap = new Map();
  for (const item of Array.isArray(prevProducts) ? prevProducts : []) {
    if (!item) continue;
    const key = makeKey(item.market, item.asin);
    prevActiveMap.set(key, item);
  }

  // 2) Read CSV
  let csvText = "";
  if (CSV_FILE) {
    const csvPath = path.isAbsolute(CSV_FILE) ? CSV_FILE : path.join(REPO_ROOT, CSV_FILE);
    console.log(`[sync] reading CSV_FILE: ${csvPath}`);
    csvText = await fs.promises.readFile(csvPath, "utf-8");
  } else if (CSV_URL) {
    const url = CSV_URL.includes("?") ? `${CSV_URL}&ts=${Date.now()}` : `${CSV_URL}?ts=${Date.now()}`;
    console.log(`[sync] fetching CSV_URL: ${url}`);
    csvText = await fetchTextWithRetry(url, 4);
  } else {
    console.error("[sync] ERROR: CSV_FILE or CSV_URL must be provided.");
    process.exit(1);
  }

  if (!csvText || normStr(csvText).length < 10) {
    console.error("[sync] ERROR: CSV content empty or too short.");
    process.exit(1);
  }

  if (looksLikeHTML(csvText)) {
    console.error("[sync] ERROR: CSV content looks like HTML (login/error page).");
    console.error("[sync] First 50 lines:");
    console.error(csvText.split("\n").slice(0, 50).join("\n"));
    process.exit(1);
  }

  const firstLine = csvText.split(/\n/)[0] || "";
  if (!headerContainsAsin(firstLine)) {
    console.error("[sync] ERROR: CSV header does not include 'asin'. Probably not the correct export.");
    console.error("[sync] Header line:");
    console.error(firstLine);
    process.exit(1);
  }

  const delimiter = detectDelimiter(firstLine);
  const grid = parseCSV(csvText, delimiter);

  if (grid.length < 2) {
    console.error("[sync] ERROR: CSV parsed but has no data rows.");
    process.exit(1);
  }

  const headers = grid[0].map((h) => normStr(h));
  const headerMap = buildHeaderMap(headers);

  // 3) Build new active list from CSV
  const newActiveMap = new Map(); // key -> product
  let totalRows = 0;
  let skipped = 0;
  let offlineCount = 0;

  for (let r = 1; r < grid.length; r++) {
    const row = grid[r];
    totalRows++;

    const market = safeUpper(getField(row, headerMap, "market"));
    const asin = normStr(getField(row, headerMap, "asin")).trim();
    if (!market || !asin) {
      skipped++;
      continue;
    }

    const statusRaw = getField(row, headerMap, "status");
    const offline = isOfflineByStatus(statusRaw);

    const item = {
      market,
      asin,
      title: normStr(getField(row, headerMap, "title")),
      link: normStr(getField(row, headerMap, "link")),
      image_url: normStr(getField(row, headerMap, "image_url")),
      remark: normStr(getField(row, headerMap, "remark")),
      discount_price: normStr(getField(row, headerMap, "discount price")),
      commission: normStr(getField(row, headerMap, "commission")),
      status: normStr(statusRaw),
      updated_at: nowISO(),
    };

    const key = makeKey(market, asin);

    if (offline) {
      offlineCount++;
      // put into archive (keep latest)
      const prev = archiveMap.get(key) || prevActiveMap.get(key);
      archiveMap.set(key, {
        ...(prev || {}),
        ...item,
        archived_reason: "status_offline",
        archived_at: nowISO(),
        last_seen_at: nowISO(),
      });
      continue;
    }

    // active
    newActiveMap.set(key, item);

    // also update archive's last_seen if it previously existed
    if (archiveMap.has(key)) {
      const prev = archiveMap.get(key);
      archiveMap.set(key, { ...prev, last_seen_at: nowISO() });
    }
  }

  // 4) Any previously active item not in newActiveMap => removed from CSV => archive it
  let removedFromActive = 0;
  for (const [key, prevItem] of prevActiveMap.entries()) {
    if (!newActiveMap.has(key)) {
      removedFromActive++;
      const prev = archiveMap.get(key) || prevItem;
      archiveMap.set(key, {
        ...(prev || {}),
        ...(prevItem || {}),
        archived_reason: "removed_from_csv",
        archived_at: nowISO(),
        last_seen_at: nowISO(),
      });
    }
  }

  // 5) Write outputs (stable ordering)
  const newProducts = Array.from(newActiveMap.values()).sort((a, b) => {
    if (a.market !== b.market) return a.market.localeCompare(b.market);
    return a.asin.localeCompare(b.asin);
  });

  const newArchive = Array.from(archiveMap.values()).sort((a, b) => {
    if (a.market !== b.market) return a.market.localeCompare(b.market);
    return a.asin.localeCompare(b.asin);
  });

  await fs.promises.writeFile(PRODUCTS_JSON, JSON.stringify(newProducts, null, 2) + "\n", "utf-8");
  await fs.promises.writeFile(ARCHIVE_JSON, JSON.stringify(newArchive, null, 2) + "\n", "utf-8");

  console.log(`[sync] rows total: ${totalRows}, skipped: ${skipped}, offline(status): ${offlineCount}`);
  console.log(`[sync] active products: ${newProducts.length}`);
  console.log(`[sync] removed from active -> archived: ${removedFromActive}`);
  console.log(`[sync] archive size: ${newArchive.length}`);
  console.log(`[sync] wrote: ${path.relative(REPO_ROOT, PRODUCTS_JSON)} / ${path.relative(REPO_ROOT, ARCHIVE_JSON)}`);
  console.log(`[sync] done: ${nowISO()}`);
})().catch((e) => {
  console.error("[sync] FATAL:", e?.stack || e);
  process.exit(1);
});
