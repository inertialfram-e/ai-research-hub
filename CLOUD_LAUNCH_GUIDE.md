# NexusAI Hub — Automated 24/7 Cloud Launch Guide

This guide walks through every step required to launch your personal AI research, tools, and systems tracker in the cloud for **$0/month**. Once deployed, a scheduled GitHub Actions worker polls your configured sources every 6 hours, enriches new items with practical takeaways via the [Gemini API](https://ai.google.dev/gemini-api/docs), and upserts them into [Supabase](https://supabase.com/docs/guides/getting-started/quickstarts/nextjs) so your hosted dashboard is always up to date.

## 1. Project Files Overview

Your `AI Hub` directory contains the following core files:

| File | Purpose |
| :--- | :--- |
| `schema.sql` | PostgreSQL schema for [Supabase](https://supabase.com/docs/guides/getting-started/quickstarts/nextjs) (`sources` and `items` tables, full-text `tsvector` indexes, views, RLS policies, and 11 pre-seeded AI feeds) |
| `ingest_ai_hub.py` | Zero-dependency Python ingestion and AI enrichment pipeline supporting [arXiv API](https://info.arxiv.org/help/api/user-manual.html), [Hugging Face Daily Papers](https://huggingface.co/papers), [GitHub REST API](https://docs.github.com/en/rest), [YouTube XML Feeds](https://developers.google.com/youtube/data/guides/push_notifications), [YouTube Data API v3](https://developers.google.com/youtube/data/reference/rest/v3/search/list), [Algolia HN Search API](https://hn.algolia.com/api), and RSS/Atom feeds |
| `ai_research_hub.html` & `index.html` | Single-file personal dashboard web app with two-way Supabase cloud sync (`index.html` is pre-created for root static hosting on Vercel or GitHub Pages) |
| `.github/workflows/ingest.yml` | Automated cron workflow using [GitHub Actions Workflow Syntax](https://docs.github.com/actions/using-workflows/workflow-syntax-for-github-actions) to run `ingest_ai_hub.py` every 6 hours |
| `.gitignore` | Protects local credential files (`Supabase DB Pass`, `.env*`), SQLite `.db` files, and Python cache directories from being committed to Git |

## 2. Step 1: Initialize Your Database in Supabase

1. **Create a new Supabase project:**
   - Sign in to [Supabase](https://supabase.com/docs/guides/getting-started/quickstarts/nextjs) and click **New Project**.
   - Choose an organization, name your project (e.g., `nexus-ai-hub`), set a database password, select the region closest to you, and wait ~60 seconds for provisioning to finish.
2. **Execute `schema.sql`:**
   - In the left sidebar of your Supabase dashboard, click **SQL Editor** and click **New query**.
   - Open your local `schema.sql` file, copy its entire contents, paste them into the SQL Editor, and click **Run**.
   - Verify under **Table Editor** that the `sources` table (pre-seeded with 11 active feeds) and the `items` table are now created.
3. **Copy your Project URL and API keys:**
   - Click the **Connect** button in the top header bar of your Supabase dashboard (or navigate to **Project Settings → Data API** and **API Keys**).
   - Your project credentials follow Supabase's modern API key format:
     - **Project URL (`SUPABASE_URL`):** `https://<your-project-ref>.supabase.co`
     - **Publishable Key (`SUPABASE_PUBLISHABLE_KEY`):** `sb_publishable_...` (entered once into the **Sources & DB** modal in your browser dashboard UI; saved in your browser's `localStorage` so it is never hardcoded in your public Git repo)
     - **Secret Key (`SUPABASE_SECRET_KEY` / `SUPABASE_SERVICE_ROLE_KEY`):** `sb_secret_...` (used exclusively by `ingest_ai_hub.py` inside GitHub Actions Secrets to upsert new items and update metrics; never expose `sb_secret_...` in browser code)

> **Security Note on Public Repositories:** `schema.sql` restricts the `anon` (`sb_publishable_...`) role strictly to read-only (`SELECT`) access, while `ingest_ai_hub.py` writes via `SUPABASE_SECRET_KEY` (`service_role`). None of your keys or project URLs are hardcoded in the repository files.

## 3. Step 2: Generate Your API Keys for Enrichment & Search

| Key Name | Required? | Where to Generate | What It Enables |
| :--- | :--- | :--- | :--- |
| `GEMINI_API_KEY` | Recommended | [Gemini API Docs / Google AI Studio](https://ai.google.dev/gemini-api/docs) | Synthesizes a 2-sentence summary, capability tags, and a concrete "How to apply this" takeaway for every new item using `gemini-2.5-flash` (falls back to keyword rules if omitted) |
| `YOUTUBE_API_KEY` | Optional | [YouTube Data API v3 Docs](https://developers.google.com/youtube/data/reference/rest/v3/search/list) (Google Cloud Console) | Enables broad keyword video discovery via the `youtube_api` driver (specific followed channels use zero-quota [YouTube XML Feeds](https://developers.google.com/youtube/data/guides/push_notifications) and require no key) |
| `GITHUB_TOKEN` | Automatic | Provided automatically by GitHub Actions as `${{ secrets.GITHUB_TOKEN }}` | Raises [GitHub REST API](https://docs.github.com/en/rest) rate limits to 5,000 requests/hour when polling trending AI repositories |

## 4. Step 3: Push Your Project to GitHub

1. **Create a GitHub repository:**
   - Create a new repository on GitHub (e.g., `ai-research-hub`). Keep it **Public** if you want unlimited free GitHub Actions minutes, or **Private** (GitHub's free tier includes 2,000 Actions minutes/month, and this script uses less than 15 minutes/month).
2. **Keep `index.html` in sync (if you edit `ai_research_hub.html`):**
   - `index.html` is already included at the project root so static hosts serve your dashboard at `/` automatically. If you customize `ai_research_hub.html` later, copy your changes over:
     ```bash
     cp ai_research_hub.html index.html
     ```
3. **Commit and push the files from your `AI Hub` directory:**
   - `.gitignore` automatically excludes `Supabase DB Pass`, `.env`, and local `*.db` files:
   ```bash
   git init
   git add .gitignore schema.sql ingest_ai_hub.py ai_research_hub.html index.html CLOUD_LAUNCH_GUIDE.md .github/workflows/ingest.yml
   git commit -m "Initial commit: Personal AI Research & Tools Hub"
   git branch -M main
   git remote add origin https://github.com/<your-username>/<your-repo>.git
   git push -u origin main
   ```

## 5. Step 4: Configure GitHub Actions Secrets & Run Your First Cloud Sync

1. **Add repository secrets:**
   - Open your GitHub repository in the browser and navigate to **Settings → Secrets and variables → Actions**.
   - Click **New repository secret** for each of the following:
     - `SUPABASE_URL`: `https://<your-project-ref>.supabase.co`
     - `SUPABASE_SECRET_KEY` (or `SUPABASE_SERVICE_ROLE_KEY`): Paste your `sb_secret_...` key.
     - `GEMINI_API_KEY`: Paste your Google AI Studio Gemini API key.
     - *(Optional)* `YOUTUBE_API_KEY`: Paste your YouTube Data API v3 key if you want keyword search in addition to channel RSS feeds.
2. **Trigger an immediate test run:**
   - Click the **Actions** tab at the top of your GitHub repository.
   - Select **Poll & Enrich AI Feeds** in the left sidebar (defined by `.github/workflows/ingest.yml`).
   - Click the **Run workflow** dropdown button on the right and click the green **Run workflow** button.
   - Watch the job complete (~20–40 seconds) and verify in your [Supabase](https://supabase.com/docs/guides/getting-started/quickstarts/nextjs) **Table Editor → `items`** table that fresh papers, GitHub tools, videos, and news stories have been populated.
   - From this point forward, [GitHub Actions Scheduled Workflows](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows) will execute the pipeline automatically every 6 hours (`0 */6 * * *`), enriching brand-new items via Gemini while updating engagement metrics (`stars`, `upvotes`, `points`, `views`) on existing items without overwriting your summaries or notes.

## 6. Step 5: Deploy the Dashboard Frontend & Connect Supabase

You can host the dashboard for free on either **Vercel** or **GitHub Pages**:

1. **Deploy with [Vercel Deployments](https://vercel.com/docs/deployments) (or GitHub Pages):**
   - **Vercel:** Sign in to Vercel, click **Add New → Project**, import your `ai-research-hub` GitHub repository, leave Framework Preset as **Other** (static), and click **Deploy**.
   - **GitHub Pages alternative:** In your GitHub repository, go to **Settings → Pages**, set **Source** to **Deploy from a branch**, select `main` and `/ (root)`, and click **Save**.
2. **Connect your live dashboard to Supabase:**
   - Open your live dashboard URL (or open `index.html` locally).
   - Click **Sources & DB** in the top-right navigation bar.
   - Scroll to **Optional Supabase Cloud Sync**, paste your `SUPABASE_URL` and your **`SUPABASE_PUBLISHABLE_KEY`** (`sb_publishable_...`), and click **Save & Sync Supabase** (these are stored only in your own browser's `localStorage`).
   - Your badge will switch to **Supabase Connected** and load all AI-enriched items and configured sources from your cloud database.

## 7. Adding New Sources Anytime (Zero Code Changes)

Because `ingest_ai_hub.py` dynamically reads all active rows from your `sources` table on every run, you can add new feeds at any time without editing Python code:

- **Via the Supabase Table Editor:** Insert a new row into the `sources` table with `name`, `category` (`paper`, `github`, `youtube`, `news`, or `project`), `driver` (`youtube_rss`, `github_api`, `arxiv_api`, `hf_papers_api`, `hn_algolia_api`, or `rss_feed`), and `feed_url_or_query`.
- **Via the Terminal CLI:** Run `ingest_ai_hub.py --add-source --only-add-source` locally with your `SUPABASE_URL` and `SUPABASE_SECRET_KEY` exported (or click **Copy CLI** next to any source in the **Sources & DB** modal):
  ```bash
  export SUPABASE_URL="https://<your-project-ref>.supabase.co"
  export SUPABASE_SECRET_KEY="<your-sb_secret-key>"

  python3 ingest_ai_hub.py --add-source --only-add-source \
    --name "AI Explained" \
    --category youtube \
    --driver youtube_rss \
    --query "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCNJ1Ymd5yFuUPtn21xtRbbw"
  ```
- **Finding a YouTube Channel's RSS URL:** Replace `<CHANNEL_ID>` in `https://www.youtube.com/xml/feeds/videos.xml?channel_id=<CHANNEL_ID>` with any channel's `UC...` ID (found in the channel's **About → Share channel → Copy channel ID** menu).
