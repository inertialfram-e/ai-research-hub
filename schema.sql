-- =============================================================================
-- Personal AI Research & Tools Hub — PostgreSQL / Supabase Schema
-- =============================================================================
-- Run this script in your Supabase SQL Editor (https://supabase.com/docs)
-- or any PostgreSQL 15+ instance to initialize the database.
-- =============================================================================

-- Enable UUID generation extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- -----------------------------------------------------------------------------
-- 1. ENUM Types for Strict Categorization & Workflow States
-- -----------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE content_category AS ENUM (
        'paper',    -- arXiv preprints, Hugging Face Daily Papers, conference papers
        'github',   -- Open-source AI tools, SDKs, MCP servers, agent frameworks
        'youtube',  -- AI channel uploads, paper walkthroughs, engineering talks
        'news',     -- AI lab announcements, technical blog posts, industry news
        'project'   -- Show HN launches, innovative demos, experimental AI apps
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE source_driver AS ENUM (
        'arxiv_api',      -- https://export.arxiv.org/api/query
        'hf_papers_api',  -- https://huggingface.co/api/daily_papers
        'github_api',     -- https://api.github.com/search/repositories
        'youtube_rss',    -- https://www.youtube.com/xml/feeds/videos.xml?channel_id=...
        'youtube_api',    -- https://www.googleapis.com/youtube/v3/search
        'hn_algolia_api', -- https://hn.algolia.com/api/v1/search_by_date
        'rss_feed'        -- Generic RSS 2.0 / Atom XML feed
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE workflow_status AS ENUM (
        'inbox',      -- Newly ingested, not yet triaged
        'to_test',    -- Queued to try out, implement, or read deeply
        'applied',    -- Integrated into personal workflow or notes completed
        'archived'    -- Read and filed away
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- -----------------------------------------------------------------------------
-- 2. Sources Table: Dynamic Registry of Feeds, Channels, and Queries
-- -----------------------------------------------------------------------------
-- Adding a new YouTube channel, RSS blog, or GitHub topic requires only
-- inserting a row here—no code changes needed in the ingestion pipeline.
CREATE TABLE IF NOT EXISTS sources (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    TEXT NOT NULL,
    category                content_category NOT NULL,
    driver                  source_driver NOT NULL,
    feed_url_or_query       TEXT NOT NULL UNIQUE,
    config                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    active                  BOOLEAN NOT NULL DEFAULT TRUE,
    fetch_interval_minutes  INTEGER NOT NULL DEFAULT 360,
    last_fetched_at         TIMESTAMPTZ,
    last_error              TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE sources IS 'Configurable registry of API queries, RSS feeds, and YouTube channels polled by the ingestion worker.';
COMMENT ON COLUMN sources.feed_url_or_query IS 'Full RSS URL, YouTube channel_id, arXiv search_query, GitHub query string, or Algolia HN query.';
COMMENT ON COLUMN sources.config IS 'Optional JSON parameters such as {"min_stars": 50, "max_results": 25, "days_back": 7, "min_points": 40}.';

-- -----------------------------------------------------------------------------
-- 3. Items Table: Unified Store for Papers, Repos, Videos, News & Projects
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS items (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           UUID REFERENCES sources(id) ON DELETE SET NULL,
    external_id         TEXT NOT NULL UNIQUE, -- e.g. 'arxiv:2610.14224', 'gh:123456', 'yt:dQw4w9WgXcQ'
    category            content_category NOT NULL,
    title               TEXT NOT NULL,
    url                 TEXT NOT NULL,
    authors_or_creator  TEXT,
    published_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_summary         TEXT,
    ai_summary          TEXT,                 -- 2-sentence Gemini synthesis
    practical_takeaway  TEXT,                 -- Actionable "How to use this to elevate your workflow"
    tags                TEXT[] NOT NULL DEFAULT '{}',
    metrics             JSONB NOT NULL DEFAULT '{}'::jsonb, -- e.g. {"stars": 420, "upvotes": 85, "points": 140, "comments": 32}
    status              workflow_status NOT NULL DEFAULT 'inbox',
    is_bookmarked       BOOLEAN NOT NULL DEFAULT FALSE,
    is_read             BOOLEAN NOT NULL DEFAULT FALSE,
    personal_notes      TEXT NOT NULL DEFAULT '',
    search_vector       TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(practical_takeaway, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(ai_summary, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(raw_summary, '')), 'C')
    ) STORED,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE items IS 'Deduplicated, AI-enriched items across research papers, GitHub tools, YouTube videos, news, and projects.';
COMMENT ON COLUMN items.external_id IS 'Globally unique source prefix + upstream identifier to guarantee idempotent upserts.';
COMMENT ON COLUMN items.practical_takeaway IS 'AI-generated guidance on how to apply the paper, tool, or technique in practice.';

-- -----------------------------------------------------------------------------
-- 4. Indexes for Fast Filtering, Tag Search, and Full-Text Querying
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_items_category_published
    ON items (category, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_items_status_bookmarked
    ON items (status, is_bookmarked)
    WHERE is_bookmarked = TRUE OR status = 'to_test';

CREATE INDEX IF NOT EXISTS idx_items_tags_gin
    ON items USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_items_metrics_gin
    ON items USING GIN (metrics);

CREATE INDEX IF NOT EXISTS idx_items_search_vector_gin
    ON items USING GIN (search_vector);

CREATE INDEX IF NOT EXISTS idx_sources_active
    ON sources (active, category);

-- -----------------------------------------------------------------------------
-- 5. Automatic updated_at Timestamp Trigger
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sources_updated_at ON sources;
CREATE TRIGGER trg_sources_updated_at
    BEFORE UPDATE ON sources
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS trg_items_updated_at ON items;
CREATE TRIGGER trg_items_updated_at
    BEFORE UPDATE ON items
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- -----------------------------------------------------------------------------
-- 6. Row-Level Security (RLS) Setup for Personal Dashboard
-- -----------------------------------------------------------------------------
-- Security Model for Public GitHub Repository:
-- 1. The background ingestion worker (ingest_ai_hub.py) uses SUPABASE_SECRET_KEY
--    (service_role), which automatically bypasses RLS to insert & update items.
-- 2. The public publishable key (anon) is restricted strictly to SELECT (read-only)
--    so nobody viewing your public GitHub repo can modify or delete your database.
-- 3. Write access from the browser requires an `authenticated` role (or your
--    secret key locally).
ALTER TABLE sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE items ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow public read on sources" ON sources;
CREATE POLICY "Allow public read on sources"
    ON sources FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS "Allow public read on items" ON items;
CREATE POLICY "Allow public read on items"
    ON items FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS "Allow authenticated full access on sources" ON sources;
DROP POLICY IF EXISTS "Allow dashboard write access on sources" ON sources;
CREATE POLICY "Allow authenticated full access on sources"
    ON sources FOR ALL TO authenticated USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow authenticated full access on items" ON items;
DROP POLICY IF EXISTS "Allow dashboard write access on items" ON items;
CREATE POLICY "Allow authenticated full access on items"
    ON items FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- -----------------------------------------------------------------------------
-- 7. Helper Views for Dashboard Tabs
-- -----------------------------------------------------------------------------
-- View 1: "To Test & Bookmarked" Action Queue
CREATE OR REPLACE VIEW v_action_queue AS
SELECT
    i.id,
    i.category,
    i.title,
    i.url,
    i.authors_or_creator,
    i.published_at,
    i.ai_summary,
    i.practical_takeaway,
    i.tags,
    i.metrics,
    i.status,
    i.is_bookmarked,
    i.personal_notes,
    s.name AS source_name
FROM items i
LEFT JOIN sources s ON i.source_id = s.id
WHERE i.status = 'to_test' OR i.is_bookmarked = TRUE
ORDER BY i.updated_at DESC;

-- View 2: High-Signal 7-Day Digest Across All Tracks
CREATE OR REPLACE VIEW v_weekly_highlights AS
SELECT
    i.id,
    i.category,
    i.title,
    i.url,
    i.authors_or_creator,
    i.published_at,
    i.ai_summary,
    i.practical_takeaway,
    i.tags,
    i.metrics,
    i.status,
    i.is_bookmarked,
    s.name AS source_name
FROM items i
LEFT JOIN sources s ON i.source_id = s.id
WHERE i.published_at >= NOW() - INTERVAL '7 days'
ORDER BY
    COALESCE((i.metrics->>'upvotes')::int, 0) +
    COALESCE((i.metrics->>'stars')::int, 0) +
    COALESCE((i.metrics->>'points')::int, 0) DESC,
    i.published_at DESC;

-- -----------------------------------------------------------------------------
-- 8. Seed Default Sources Across All 4 Requested Tracks (30+ High-Signal Feeds)
-- -----------------------------------------------------------------------------
INSERT INTO sources (name, category, driver, feed_url_or_query, config)
VALUES
    -- =========================================================================
    -- Track 1: AI Research Papers (Hugging Face + arXiv Core AI Categories)
    -- =========================================================================
    (
        'Hugging Face Daily Papers',
        'paper',
        'hf_papers_api',
        'https://huggingface.co/api/daily_papers',
        '{"max_results": 40, "min_upvotes": 2}'::jsonb
    ),
    (
        'arXiv Artificial Intelligence & Machine Learning (cs.AI, cs.LG)',
        'paper',
        'arxiv_api',
        'cat:cs.AI OR cat:cs.LG',
        '{"max_results": 45}'::jsonb
    ),
    (
        'arXiv Computation & Language / LLMs (cs.CL)',
        'paper',
        'arxiv_api',
        'cat:cs.CL',
        '{"max_results": 40}'::jsonb
    ),
    (
        'arXiv Computer Vision & Multimodal Systems (cs.CV)',
        'paper',
        'arxiv_api',
        'cat:cs.CV',
        '{"max_results": 35}'::jsonb
    ),
    (
        'arXiv Multi-Agent Systems, Robotics & Software Engineering (cs.MA, cs.RO, cs.SE)',
        'paper',
        'arxiv_api',
        'cat:cs.MA OR cat:cs.RO OR cat:cs.SE',
        '{"max_results": 35}'::jsonb
    ),

    -- =========================================================================
    -- Track 2: New & Trending AI Tools on GitHub
    -- =========================================================================
    (
        'GitHub Trending AI Agents & Autonomous Workflows',
        'github',
        'github_api',
        'topic:ai-agents',
        '{"days_back": 30, "min_stars": 15, "max_results": 30}'::jsonb
    ),
    (
        'GitHub Model Context Protocol (MCP) Servers & Tools',
        'github',
        'github_api',
        'topic:mcp',
        '{"days_back": 45, "min_stars": 10, "max_results": 30}'::jsonb
    ),
    (
        'GitHub LLM Tools, RAG & Vector Databases',
        'github',
        'github_api',
        'topic:rag',
        '{"days_back": 30, "min_stars": 15, "max_results": 25}'::jsonb
    ),
    (
        'GitHub Large Language Models & Local Inference',
        'github',
        'github_api',
        'topic:large-language-models',
        '{"days_back": 30, "min_stars": 20, "max_results": 25}'::jsonb
    ),
    (
        'GitHub AI Coding Assistants & Developer Tools',
        'github',
        'github_api',
        ' "claude code" OR "mcp server" OR "coding agent" in:name,description',
        '{"days_back": 21, "min_stars": 15, "max_results": 25}'::jsonb
    ),
    (
        'GitHub Active Breakout AI Repositories (Updated Recently)',
        'github',
        'github_api',
        'topic:artificial-intelligence',
        '{"days_back": 14, "min_stars": 25, "max_results": 25}'::jsonb
    ),

    -- =========================================================================
    -- Track 3: YouTube AI Channels & Video Breakdowns (Zero-Quota XML + API)
    -- =========================================================================
    (
        'Two Minute Papers (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCbfYPyITQ-7l4upoX8nvctg',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Andrej Karpathy (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCXUPKJO5MZQN11PqgIvyuvQ',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Yannic Kilcher (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCZHmQk67mSJgfCCTn7xBfew',
        '{"max_results": 15}'::jsonb
    ),
    (
        'AI Explained (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCNJ1Ymd5yFuUPtn21xtRbbw',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Matthew Berman (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCawZsQWqfGSbCI5yjkdVkTA',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Sam Witteveen — AI & LLM Engineering (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC55ODQSvARtgSyc8ThfiepQ',
        '{"max_results": 15}'::jsonb
    ),
    (
        'AI Jason (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCrXSVX9a1mj8l0CMLwKgMVw',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Cole Medin — AI Agents & Local AI (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCMwVTLZIRRUyyVrkjDpn4pA',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Fireship (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCsBjURrPoezykLs9EqgamOA',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Latent Space Podcast — AI Engineers (YouTube RSS)',
        'youtube',
        'youtube_rss',
        'https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCGnM7bwQv91M20cec9e5VCg',
        '{"max_results": 15}'::jsonb
    ),
    (
        'YouTube AI Engineering & Paper Breakdowns Search',
        'youtube',
        'youtube_api',
        'AI agents OR LLM engineering OR MCP server OR arXiv paper explained',
        '{"max_results": 25, "days_back": 14}'::jsonb
    ),

    -- =========================================================================
    -- Track 4: AI Lab Blogs, Engineering News, Reddit & Show HN Projects
    -- =========================================================================
    (
        'Hacker News High-Signal AI & LLM Stories',
        'news',
        'hn_algolia_api',
        'LLM OR "AI agent" OR Anthropic OR OpenAI OR Gemini OR DeepMind OR Qwen OR Llama',
        '{"tags": "story", "min_points": 20, "max_results": 35}'::jsonb
    ),
    (
        'Hacker News AI Engineering, RAG & MCP Discussions',
        'news',
        'hn_algolia_api',
        'MCP OR RAG OR vLLM OR Ollama OR "Claude Code" OR Cursor OR Fine-tuning',
        '{"tags": "story", "min_points": 15, "max_results": 30}'::jsonb
    ),
    (
        'Show HN: Innovative AI Projects & Launches',
        'project',
        'hn_algolia_api',
        'Show HN AI OR LLM OR Agent OR MCP OR GPT OR RAG',
        '{"tags": "show_hn", "min_points": 5, "max_results": 35}'::jsonb
    ),
    (
        'OpenAI Official News & Product Releases (RSS)',
        'news',
        'rss_feed',
        'https://openai.com/news/rss.xml',
        '{"max_results": 20}'::jsonb
    ),
    (
        'Hugging Face Official Blog (RSS)',
        'news',
        'rss_feed',
        'https://huggingface.co/blog/feed.xml',
        '{"max_results": 20}'::jsonb
    ),
    (
        'Google AI & DeepMind Research Blog (RSS)',
        'news',
        'rss_feed',
        'https://blog.google/technology/ai/rss/',
        '{"max_results": 20}'::jsonb
    ),
    (
        'Simon Willison Weblog (AI & LLM Engineering Atom)',
        'news',
        'rss_feed',
        'https://simonwillison.net/atom/everything/',
        '{"max_results": 20}'::jsonb
    ),
    (
        'Latent Space — The AI Engineer Substack (RSS)',
        'news',
        'rss_feed',
        'https://www.latent.space/feed',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Ahead of AI — Sebastian Raschka (LLM Research RSS)',
        'news',
        'rss_feed',
        'https://magazine.sebastianraschka.com/feed',
        '{"max_results": 15}'::jsonb
    ),
    (
        'Lilian Weng — OpenAI / AI Research Deep Dives (RSS)',
        'news',
        'rss_feed',
        'https://lilianweng.github.io/index.xml',
        '{"max_results": 15}'::jsonb
    ),
    (
        'LangChain Engineering & Agent Workflows Blog (RSS)',
        'news',
        'rss_feed',
        'https://blog.langchain.dev/rss/',
        '{"max_results": 15}'::jsonb
    ),
    (
        'r/LocalLLaMA — Open-Weights Models & Local AI Setups (RSS)',
        'project',
        'rss_feed',
        'https://www.reddit.com/r/LocalLLaMA/top/.rss?t=week',
        '{"max_results": 25}'::jsonb
    ),
    (
        'r/MachineLearning — Research Discussions & Implementations (RSS)',
        'news',
        'rss_feed',
        'https://www.reddit.com/r/MachineLearning/hot/.rss',
        '{"max_results": 20}'::jsonb
    )
ON CONFLICT (feed_url_or_query) DO UPDATE SET
    config = EXCLUDED.config,
    name = EXCLUDED.name,
    active = TRUE;

