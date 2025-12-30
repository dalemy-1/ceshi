name: Sync products.json + archive.json and generate /p pages (every 5 min)

on:
  workflow_dispatch:
  schedule:
    - cron: "*/5 * * * *"

permissions:
  contents: write

concurrency:
  group: sync-products
  cancel-in-progress: true

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: "20"

      # 1) 从 CSV_URL 重建 products.json + archive.json（你的脚本：scripts/sync_from_csv.mjs）
      - name: Sync from CSV -> products.json + archive.json
        env:
          CSV_URL: "http://154.48.226.95:5001/admin/Product/export_csv"
        run: |
          echo "[workflow] CSV_URL=$CSV_URL"
          node scripts/sync_from_csv.mjs

      # 2) 生成 /p/{market}/{asinKey}/index.html（URL 保持不变的静态独立页）
      - name: Generate product pages under /p
        env:
          SITE_ORIGIN: "https://ama.omino.top"
        run: |
          node tools/generate_product_pages.mjs

      # 3) 提交（仅当有变更）
      - name: Commit changes (only if changed)
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add -A

          if git diff --cached --quiet; then
            echo "No changes."
            exit 0
          fi

          git commit -m "chore: sync products + generate pages"
          git push
