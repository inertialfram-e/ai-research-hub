#!/usr/bin/env python3
"""
Personal AI Research & Tools Hub — Automated Ingestion & Enrichment Pipeline
=============================================================================
Fetches new items across 4 tracks:
  1. Research Papers (arXiv API + Hugging Face Daily Papers API)
  2. GitHub AI Tools (GitHub REST Search API sorted by star velocity)
  3. YouTube Videos (Zero-quota YouTube XML channel feeds + YouTube Data API v3)
  4. AI News & Innovative Projects (Algolia Hacker News Search API + RSS/Atom feeds)

Enriches unseen items with Gemini API ("How to use this" practical takeaways + tags)
and upserts them to Supabase (PostgreSQL) or a local SQLite database for instant
zero-setup local usage.

Environment Variables (all optional when running with --sqlite):
  SUPABASE_URL              Your Supabase project URL (https://<ref>.supabase.co)
  SUPABASE_SERVICE_ROLE_KEY Your Supabase service_role secret key
  GEMINI_API_KEY            Google AI Studio key for AI summaries & takeaways
  GITHUB_TOKEN              GitHub PAT (increases rate limit to 5,000 req/hr)
  YOUTUBE_API_KEY           YouTube Data API v3 key (only for keyword search driver)

Usage Examples:
  # Run with local SQLite database (zero cloud setup required):
  python3 ingest_ai_hub.py --sqlite ai_hub.db

  # Run against Supabase in production (e.g., via GitHub Actions cron):
  python3 ingest_ai_hub.py

  # Add a new YouTube channel, RSS feed, or GitHub topic dynamically:
  python3 ingest_ai_hub.py --sqlite ai_hub.db --add-source \
      --name "AI Explained" --category youtube --driver youtube_rss \
      --query "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCNJ1Ymd5yFuUPtn21xtRbbw"
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

# Optional feedparser import (falls back to built-in XML parser if not installed)
try:
    import feedparser  # type: ignore
except ImportError:
    feedparser = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ai_hub_ingest")

USER_AGENT = "PersonalAIHub/1.0 (+https://github.com/personal-ai-hub)"


# =============================================================================
# Data Models
# =============================================================================
@dataclass
class SourceConfig:
    id: str
    name: str
    category: str          # 'paper' | 'github' | 'youtube' | 'news' | 'project'
    driver: str            # 'arxiv_api' | 'hf_papers_api' | 'github_api' | 'youtube_rss' | 'youtube_api' | 'hn_algolia_api' | 'rss_feed'
    feed_url_or_query: str
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HubItem:
    external_id: str
    source_id: Optional[str]
    category: str
    title: str
    url: str
    authors_or_creator: str
    published_at: str
    raw_summary: str
    ai_summary: str = ""
    practical_takeaway: str = ""
    tags: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


DEFAULT_SOURCES: List[SourceConfig] = [
    # Track 1: AI Research Papers
    SourceConfig(
        id="src-hf-papers",
        name="Hugging Face Daily Papers",
        category="paper",
        driver="hf_papers_api",
        feed_url_or_query="https://huggingface.co/api/daily_papers",
        config={"max_results": 40, "min_upvotes": 2},
    ),
    SourceConfig(
        id="src-arxiv-ai-lg",
        name="arXiv Artificial Intelligence & Machine Learning (cs.AI, cs.LG)",
        category="paper",
        driver="arxiv_api",
        feed_url_or_query="cat:cs.AI OR cat:cs.LG",
        config={"max_results": 45},
    ),
    SourceConfig(
        id="src-arxiv-cl",
        name="arXiv Computation & Language / LLMs (cs.CL)",
        category="paper",
        driver="arxiv_api",
        feed_url_or_query="cat:cs.CL",
        config={"max_results": 40},
    ),
    SourceConfig(
        id="src-arxiv-cv",
        name="arXiv Computer Vision & Multimodal Systems (cs.CV)",
        category="paper",
        driver="arxiv_api",
        feed_url_or_query="cat:cs.CV",
        config={"max_results": 35},
    ),
    SourceConfig(
        id="src-arxiv-ma-ro-se",
        name="arXiv Multi-Agent Systems, Robotics & Software Engineering (cs.MA, cs.RO, cs.SE)",
        category="paper",
        driver="arxiv_api",
        feed_url_or_query="cat:cs.MA OR cat:cs.RO OR cat:cs.SE",
        config={"max_results": 35},
    ),
    # Track 2: GitHub AI Tools & Repositories
    SourceConfig(
        id="src-gh-agents",
        name="GitHub Trending AI Agents & Autonomous Workflows",
        category="github",
        driver="github_api",
        feed_url_or_query="topic:ai-agents",
        config={"days_back": 30, "min_stars": 15, "max_results": 30},
    ),
    SourceConfig(
        id="src-gh-mcp",
        name="GitHub Model Context Protocol (MCP) Servers & Tools",
        category="github",
        driver="github_api",
        feed_url_or_query="topic:mcp",
        config={"days_back": 45, "min_stars": 10, "max_results": 30},
    ),
    SourceConfig(
        id="src-gh-rag",
        name="GitHub LLM Tools, RAG & Vector Databases",
        category="github",
        driver="github_api",
        feed_url_or_query="topic:rag",
        config={"days_back": 30, "min_stars": 15, "max_results": 25},
    ),
    SourceConfig(
        id="src-gh-llm",
        name="GitHub Large Language Models & Local Inference",
        category="github",
        driver="github_api",
        feed_url_or_query="topic:large-language-models",
        config={"days_back": 30, "min_stars": 20, "max_results": 25},
    ),
    SourceConfig(
        id="src-gh-coding",
        name="GitHub AI Coding Assistants & Developer Tools",
        category="github",
        driver="github_api",
        feed_url_or_query='"claude code" OR "mcp server" OR "coding agent" in:name,description',
        config={"days_back": 21, "min_stars": 15, "max_results": 25},
    ),
    SourceConfig(
        id="src-gh-breakout",
        name="GitHub Active Breakout AI Repositories",
        category="github",
        driver="github_api",
        feed_url_or_query="topic:artificial-intelligence",
        config={"days_back": 14, "min_stars": 25, "max_results": 25},
    ),
    # Track 3: YouTube AI Channels & Video Breakdowns
    SourceConfig(
        id="src-yt-twomin",
        name="Two Minute Papers (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCbfYPyITQ-7l4upoX8nvctg",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-karpathy",
        name="Andrej Karpathy (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCXUPKJO5MZQN11PqgIvyuvQ",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-kilcher",
        name="Yannic Kilcher (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCZHmQk67mSJgfCCTn7xBfew",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-aiexplained",
        name="AI Explained (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCNJ1Ymd5yFuUPtn21xtRbbw",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-berman",
        name="Matthew Berman (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCawZsQWqfGSbCI5yjkdVkTA",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-witteveen",
        name="Sam Witteveen — AI & LLM Engineering (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC55ODQSvARtgSyc8ThfiepQ",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-aijason",
        name="AI Jason (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCrXSVX9a1mj8l0CMLwKgMVw",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-colemedin",
        name="Cole Medin — AI Agents & Local AI (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCMwVTLZIRRUyyVrkjDpn4pA",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-fireship",
        name="Fireship (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCsBjURrPoezykLs9EqgamOA",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-latentspace",
        name="Latent Space Podcast — AI Engineers (YouTube RSS)",
        category="youtube",
        driver="youtube_rss",
        feed_url_or_query="https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCGnM7bwQv91M20cec9e5VCg",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-yt-search",
        name="YouTube AI Engineering & Paper Breakdowns Search",
        category="youtube",
        driver="youtube_api",
        feed_url_or_query="AI agents OR LLM engineering OR MCP server OR arXiv paper explained",
        config={"max_results": 25, "days_back": 14},
    ),
    # Track 4: AI Lab Blogs, Engineering News, Reddit & Show HN Projects
    SourceConfig(
        id="src-hn-news",
        name="Hacker News High-Signal AI & LLM Stories",
        category="news",
        driver="hn_algolia_api",
        feed_url_or_query='LLM OR "AI agent" OR Anthropic OR OpenAI OR Gemini OR DeepMind OR Qwen OR Llama',
        config={"tags": "story", "min_points": 20, "max_results": 35},
    ),
    SourceConfig(
        id="src-hn-eng",
        name="Hacker News AI Engineering, RAG & MCP Discussions",
        category="news",
        driver="hn_algolia_api",
        feed_url_or_query='MCP OR RAG OR vLLM OR Ollama OR "Claude Code" OR Cursor OR Fine-tuning',
        config={"tags": "story", "min_points": 15, "max_results": 30},
    ),
    SourceConfig(
        id="src-hn-show",
        name="Show HN: Innovative AI Projects & Launches",
        category="project",
        driver="hn_algolia_api",
        feed_url_or_query="Show HN AI OR LLM OR Agent OR MCP OR GPT OR RAG",
        config={"tags": "show_hn", "min_points": 5, "max_results": 35},
    ),
    SourceConfig(
        id="src-rss-openai",
        name="OpenAI Official News & Product Releases (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://openai.com/news/rss.xml",
        config={"max_results": 20},
    ),
    SourceConfig(
        id="src-rss-hfblog",
        name="Hugging Face Official Blog (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://huggingface.co/blog/feed.xml",
        config={"max_results": 20},
    ),
    SourceConfig(
        id="src-rss-googleai",
        name="Google AI & DeepMind Research Blog (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://blog.google/technology/ai/rss/",
        config={"max_results": 20},
    ),
    SourceConfig(
        id="src-rss-simonw",
        name="Simon Willison Weblog (AI & LLM Engineering Atom)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://simonwillison.net/atom/everything/",
        config={"max_results": 20},
    ),
    SourceConfig(
        id="src-rss-latentspace",
        name="Latent Space — The AI Engineer Substack (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://www.latent.space/feed",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-rss-raschka",
        name="Ahead of AI — Sebastian Raschka (LLM Research RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://magazine.sebastianraschka.com/feed",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-rss-lilianweng",
        name="Lilian Weng — OpenAI / AI Research Deep Dives (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://lilianweng.github.io/index.xml",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-rss-langchain",
        name="LangChain Engineering & Agent Workflows Blog (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://blog.langchain.dev/rss/",
        config={"max_results": 15},
    ),
    SourceConfig(
        id="src-rss-localllama",
        name="r/LocalLLaMA — Open-Weights Models & Local AI Setups (RSS)",
        category="project",
        driver="rss_feed",
        feed_url_or_query="https://www.reddit.com/r/LocalLLaMA/top/.rss?t=week",
        config={"max_results": 25},
    ),
    SourceConfig(
        id="src-rss-mlreddit",
        name="r/MachineLearning — Research Discussions & Implementations (RSS)",
        category="news",
        driver="rss_feed",
        feed_url_or_query="https://www.reddit.com/r/MachineLearning/hot/.rss",
        config={"max_results": 20},
    ),
]


# =============================================================================
# HTTP Helper (Zero External Dependencies Required)
# =============================================================================
def http_request(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Any] = None,
    timeout: int = 25,
) -> bytes:
    req_headers = {"User-Agent": USER_AGENT}
    if headers:
        req_headers.update(headers)
    data = None
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")

    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def clean_text(text: Optional[str], max_len: int = 1200) -> str:
    if not text:
        return ""
    stripped = re.sub(r"<[^>]+>", " ", text)
    collapsed = re.sub(r"\s+", " ", stripped).strip()
    return collapsed[:max_len]


# =============================================================================
# Source Fetchers (4 Core Tracks)
# =============================================================================
def fetch_arxiv_papers(source: SourceConfig) -> List[HubItem]:
    """Queries the official arXiv Atom API (https://info.arxiv.org/help/api/user-manual.html)."""
    max_results = int(source.config.get("max_results", 25))
    params = urllib.parse.urlencode(
        {
            "search_query": source.feed_url_or_query,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "start": 0,
            "max_results": max_results,
        }
    )
    url = f"https://export.arxiv.org/api/query?{params}"
    raw_xml = http_request(url)
    root = ET.fromstring(raw_xml)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    items: List[HubItem] = []
    for entry in root.findall("atom:entry", ns):
        id_url = entry.findtext("atom:id", default="", namespaces=ns).strip()
        arxiv_id = id_url.split("/abs/")[-1].split("v")[0] if "/abs/" in id_url else id_url
        title = clean_text(entry.findtext("atom:title", default="", namespaces=ns), 300)
        summary = clean_text(entry.findtext("atom:summary", default="", namespaces=ns), 1500)
        published = entry.findtext("atom:published", default="", namespaces=ns).strip()
        authors = [
            a.findtext("atom:name", default="", namespaces=ns).strip()
            for a in entry.findall("atom:author", ns)
        ]
        author_str = ", ".join(authors[:4]) + (" et al." if len(authors) > 4 else "")

        if not arxiv_id or not title:
            continue

        items.append(
            HubItem(
                external_id=f"arxiv:{arxiv_id}",
                source_id=source.id,
                category=source.category,
                title=title,
                url=f"https://arxiv.org/abs/{arxiv_id}",
                authors_or_creator=author_str,
                published_at=published or dt.datetime.now(dt.timezone.utc).isoformat(),
                raw_summary=summary,
                metrics={"source": "arXiv"},
            )
        )
    return items


def fetch_hf_daily_papers(source: SourceConfig) -> List[HubItem]:
    """Fetches community-curated AI papers from Hugging Face Daily Papers."""
    max_results = int(source.config.get("max_results", 35))
    min_upvotes = int(source.config.get("min_upvotes", 2))
    payload = json.loads(http_request(source.feed_url_or_query).decode("utf-8"))

    items: List[HubItem] = []
    for record in payload[:max_results]:
        paper = record.get("paper", record)
        arxiv_id = paper.get("id", "")
        upvotes = int(paper.get("upvotes", record.get("upvotes", 0)))
        if not arxiv_id or upvotes < min_upvotes:
            continue

        title = clean_text(paper.get("title") or record.get("title"), 300)
        summary = clean_text(paper.get("summary") or record.get("summary"), 1500)
        published = paper.get("publishedAt") or dt.datetime.now(dt.timezone.utc).isoformat()
        authors = [a.get("name", "") for a in paper.get("authors", []) if isinstance(a, dict)]
        author_str = ", ".join(authors[:4]) + (" et al." if len(authors) > 4 else "")

        items.append(
            HubItem(
                external_id=f"arxiv:{arxiv_id}",
                source_id=source.id,
                category="paper",
                title=title,
                url=f"https://huggingface.co/papers/{arxiv_id}",
                authors_or_creator=author_str,
                published_at=published,
                raw_summary=summary,
                metrics={
                    "upvotes": upvotes,
                    "arxiv_url": f"https://arxiv.org/abs/{arxiv_id}",
                },
            )
        )
    return items


def fetch_github_repos(source: SourceConfig) -> List[HubItem]:
    """Queries GitHub REST Search API for fast-growing AI repositories and tools."""
    days_back = int(source.config.get("days_back", 21))
    min_stars = int(source.config.get("min_stars", 15))
    max_results = int(source.config.get("max_results", 25))
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back)).strftime("%Y-%m-%d")

    raw_q = source.feed_url_or_query.strip()
    # Avoid wrapping simple topic queries in outer parentheses when OR is present,
    # and use pushed:>cutoff when multiple OR topics are used so active repos aren't dropped.
    if " OR " in raw_q and "topic:" in raw_q:
        primary_topic = raw_q.split(" OR ")[0].strip()
        full_query = f"{primary_topic} pushed:>{cutoff} stars:>={min_stars}"
    else:
        full_query = f"{raw_q} created:>{cutoff} stars:>={min_stars}"

    params = urllib.parse.urlencode(
        {"q": full_query, "sort": "stars", "order": "desc", "per_page": max_results}
    )
    url = f"https://api.github.com/search/repositories?{params}"

    headers = {"Accept": "application/vnd.github+json"}
    gh_token = os.environ.get("GITHUB_TOKEN")
    if gh_token:
        headers["Authorization"] = f"Bearer {gh_token}"

    payload = json.loads(http_request(url, headers=headers).decode("utf-8"))
    items: List[HubItem] = []

    for repo in payload.get("items", []):
        repo_id = repo.get("id")
        full_name = repo.get("full_name", "")
        description = clean_text(repo.get("description") or "", 1000)
        topics = repo.get("topics") or []
        language = repo.get("language") or "Unknown"

        items.append(
            HubItem(
                external_id=f"gh:{repo_id}",
                source_id=source.id,
                category="github",
                title=full_name,
                url=repo.get("html_url", ""),
                authors_or_creator=repo.get("owner", {}).get("login", ""),
                published_at=repo.get("created_at") or dt.datetime.now(dt.timezone.utc).isoformat(),
                raw_summary=f"[{language}] {description} Topics: {', '.join(topics[:6])}",
                tags=[t.lower() for t in topics[:4]],
                metrics={
                    "stars": repo.get("stargazers_count", 0),
                    "forks": repo.get("forks_count", 0),
                    "open_issues": repo.get("open_issues_count", 0),
                    "language": language,
                },
            )
        )
    return items


def fetch_youtube_rss(source: SourceConfig) -> List[HubItem]:
    """Polls zero-quota native YouTube XML channel feeds."""
    max_results = int(source.config.get("max_results", 10))
    raw_xml = http_request(source.feed_url_or_query)
    root = ET.fromstring(raw_xml)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }

    channel_title = root.findtext("atom:title", default=source.name, namespaces=ns)
    items: List[HubItem] = []

    for entry in root.findall("atom:entry", ns)[:max_results]:
        video_id = entry.findtext("yt:videoId", default="", namespaces=ns).strip()
        title = clean_text(entry.findtext("atom:title", default="", namespaces=ns), 300)
        published = entry.findtext("atom:published", default="", namespaces=ns).strip()
        media_group = entry.find("media:group", ns)
        description = ""
        views = 0
        if media_group is not None:
            description = clean_text(
                media_group.findtext("media:description", default="", namespaces=ns), 1200
            )
            community = media_group.find("media:community", ns)
            if community is not None:
                stats = community.find("media:statistics", ns)
                if stats is not None and stats.attrib.get("views"):
                    views = int(stats.attrib["views"])

        if not video_id:
            continue

        items.append(
            HubItem(
                external_id=f"yt:{video_id}",
                source_id=source.id,
                category="youtube",
                title=title,
                url=f"https://www.youtube.com/watch?v={video_id}",
                authors_or_creator=channel_title,
                published_at=published or dt.datetime.now(dt.timezone.utc).isoformat(),
                raw_summary=description,
                metrics={
                    "views": views,
                    "thumbnail": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                },
            )
        )
    return items


def fetch_youtube_api(source: SourceConfig) -> List[HubItem]:
    """Queries YouTube Data API v3 /search/list when YOUTUBE_API_KEY is configured."""
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        logger.info("Skipping YouTube API search '%s' (YOUTUBE_API_KEY not set).", source.name)
        return []

    max_results = int(source.config.get("max_results", 15))
    days_back = int(source.config.get("days_back", 7))
    published_after = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = urllib.parse.urlencode(
        {
            "part": "snippet",
            "q": source.feed_url_or_query,
            "type": "video",
            "order": "relevance",
            "publishedAfter": published_after,
            "maxResults": max_results,
            "key": api_key,
        }
    )
    url = f"https://www.googleapis.com/youtube/v3/search?{params}"
    payload = json.loads(http_request(url).decode("utf-8"))

    items: List[HubItem] = []
    for entry in payload.get("items", []):
        video_id = entry.get("id", {}).get("videoId")
        snippet = entry.get("snippet", {})
        if not video_id:
            continue

        items.append(
            HubItem(
                external_id=f"yt:{video_id}",
                source_id=source.id,
                category="youtube",
                title=clean_text(snippet.get("title"), 300),
                url=f"https://www.youtube.com/watch?v={video_id}",
                authors_or_creator=snippet.get("channelTitle", ""),
                published_at=snippet.get("publishedAt")
                or dt.datetime.now(dt.timezone.utc).isoformat(),
                raw_summary=clean_text(snippet.get("description"), 1000),
                metrics={"thumbnail": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"},
            )
        )
    return items


def fetch_hacker_news(source: SourceConfig) -> List[HubItem]:
    """Queries Algolia Hacker News Search API (https://hn.algolia.com/api)."""
    max_results = int(source.config.get("max_results", 20))
    min_points = int(source.config.get("min_points", 30))
    tags = source.config.get("tags", "story")

    params = urllib.parse.urlencode(
        {
            "query": source.feed_url_or_query,
            "tags": tags,
            "numericFilters": f"points>={min_points}",
            "hitsPerPage": max_results,
        }
    )
    url = f"https://hn.algolia.com/api/v1/search_by_date?{params}"
    payload = json.loads(http_request(url).decode("utf-8"))

    items: List[HubItem] = []
    for hit in payload.get("hits", []):
        object_id = hit.get("objectID")
        title = clean_text(hit.get("title"), 300)
        story_url = hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}"
        story_text = clean_text(hit.get("story_text") or "", 1000)

        if not object_id or not title:
            continue

        items.append(
            HubItem(
                external_id=f"hn:{object_id}",
                source_id=source.id,
                category=source.category,
                title=title,
                url=story_url,
                authors_or_creator=hit.get("author", "hn_user"),
                published_at=hit.get("created_at")
                or dt.datetime.now(dt.timezone.utc).isoformat(),
                raw_summary=story_text
                or f"Hacker News discussion: https://news.ycombinator.com/item?id={object_id}",
                metrics={
                    "points": hit.get("points", 0),
                    "comments": hit.get("num_comments", 0),
                    "hn_url": f"https://news.ycombinator.com/item?id={object_id}",
                },
            )
        )
    return items


def fetch_rss_feed(source: SourceConfig) -> List[HubItem]:
    """Parses any standard RSS 2.0 or Atom XML feed."""
    max_results = int(source.config.get("max_results", 15))
    raw_bytes = http_request(source.feed_url_or_query)

    if feedparser is not None:
        parsed = feedparser.parse(raw_bytes)
        items: List[HubItem] = []
        for entry in parsed.entries[:max_results]:
            link = getattr(entry, "link", "")
            guid = getattr(entry, "id", link)
            title = clean_text(getattr(entry, "title", ""), 300)
            summary = clean_text(
                getattr(entry, "summary", getattr(entry, "description", "")), 1200
            )
            published = getattr(
                entry,
                "published",
                getattr(entry, "updated", dt.datetime.now(dt.timezone.utc).isoformat()),
            )
            author = getattr(entry, "author", source.name)
            if not link or not title:
                continue
            items.append(
                HubItem(
                    external_id=f"rss:{guid}",
                    source_id=source.id,
                    category=source.category,
                    title=title,
                    url=link,
                    authors_or_creator=author,
                    published_at=published,
                    raw_summary=summary,
                )
            )
        return items

    # Built-in ElementTree fallback for RSS 2.0 / Atom feeds
    root = ET.fromstring(raw_bytes)
    items = []
    # Check Atom vs RSS 2.0
    if root.tag.endswith("feed"):
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns)[:max_results]:
            title = clean_text(entry.findtext("atom:title", default="", namespaces=ns), 300)
            link_el = entry.find("atom:link", ns)
            link = link_el.attrib.get("href", "") if link_el is not None else ""
            guid = entry.findtext("atom:id", default=link, namespaces=ns).strip()
            summary = clean_text(
                entry.findtext("atom:summary", default="", namespaces=ns)
                or entry.findtext("atom:content", default="", namespaces=ns),
                1200,
            )
            published = entry.findtext("atom:updated", default="", namespaces=ns).strip()
            if title and link:
                items.append(
                    HubItem(
                        external_id=f"rss:{guid}",
                        source_id=source.id,
                        category=source.category,
                        title=title,
                        url=link,
                        authors_or_creator=source.name,
                        published_at=published or dt.datetime.now(dt.timezone.utc).isoformat(),
                        raw_summary=summary,
                    )
                )
    else:
        for item_el in root.findall(".//item")[:max_results]:
            title = clean_text(item_el.findtext("title", default=""), 300)
            link = item_el.findtext("link", default="").strip()
            guid = item_el.findtext("guid", default=link).strip()
            summary = clean_text(item_el.findtext("description", default=""), 1200)
            published = item_el.findtext("pubDate", default="").strip()
            if title and link:
                items.append(
                    HubItem(
                        external_id=f"rss:{guid}",
                        source_id=source.id,
                        category=source.category,
                        title=title,
                        url=link,
                        authors_or_creator=source.name,
                        published_at=published or dt.datetime.now(dt.timezone.utc).isoformat(),
                        raw_summary=summary,
                    )
                )
    return items


DRIVER_DISPATCH = {
    "arxiv_api": fetch_arxiv_papers,
    "hf_papers_api": fetch_hf_daily_papers,
    "github_api": fetch_github_repos,
    "youtube_rss": fetch_youtube_rss,
    "youtube_api": fetch_youtube_api,
    "hn_algolia_api": fetch_hacker_news,
    "rss_feed": fetch_rss_feed,
}


# =============================================================================
# AI Triage & Practical Takeaway Enricher (Gemini API + Deterministic Fallback)
# =============================================================================
KEYWORD_TAG_RULES = {
    "agents": ["agent", "agentic", "multi-agent", "autonomous", "tool use"],
    "mcp": ["mcp", "model context protocol"],
    "rag": ["rag", "retrieval", "vector", "embedding", "chunking"],
    "fine-tuning": ["fine-tun", "lora", "qlora", "rlhf", "dpo", "grpo", "post-training"],
    "coding": ["code", "coding", "swe-bench", "compiler", "ide", "cursor", "copilot"],
    "multimodal": ["vision", "VLM", "audio", "speech", "video", "image", "multimodal"],
    "local-ai": ["ollama", "gguf", "llama.cpp", "quantiz", "edge", "on-device"],
    "prompting": ["prompt", "chain-of-thought", "reasoning", "eval", "benchmark"],
}


def fallback_enrich(item: HubItem) -> None:
    """Deterministic keyword tagger and summary fallback when GEMINI_API_KEY is absent."""
    corpus = f"{item.title} {item.raw_summary}".lower()
    matched_tags = list(item.tags)
    for tag, keywords in KEYWORD_TAG_RULES.items():
        if any(kw.lower() in corpus for kw in keywords) and tag not in matched_tags:
            matched_tags.append(tag)
    if not matched_tags:
        matched_tags.append(item.category)
    item.tags = matched_tags[:5]

    if not item.ai_summary:
        item.ai_summary = (
            item.raw_summary[:280] + ("..." if len(item.raw_summary) > 280 else "")
            if item.raw_summary
            else f"New {item.category} entry from {item.authors_or_creator}."
        )
    if not item.practical_takeaway:
        action_by_category = {
            "paper": "Review the method and evaluation benchmarks to see if the technique improves your prompting, RAG, or model selection.",
            "github": "Inspect the repository README and quickstart to evaluate integrating this tool or SDK into your local AI workflow.",
            "youtube": "Watch the walkthrough for architectural patterns and practical engineering intuition.",
            "news": "Check the announcement details for API updates, pricing shifts, or new model capabilities you can adopt.",
            "project": "Explore the live demo or architecture to spark ideas for your own AI workflows and prototypes.",
        }
        item.practical_takeaway = action_by_category.get(
            item.category, "Review item for workflow improvements."
        )


def enrich_batch_with_gemini(items: List[HubItem], api_key: Optional[str], chunk_size: int = 12) -> None:
    """Enriches new items in batches of 12 per Gemini API call to stay well within free-tier RPM limits."""
    if not items:
        return
    if not api_key:
        for item in items:
            fallback_enrich(item)
        return

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.5-flash:generateContent?key={urllib.parse.quote(api_key)}"
    )

    for start_idx in range(0, len(items), chunk_size):
        chunk = items[start_idx : start_idx + chunk_size]
        items_payload = [
            {
                "index": idx,
                "category": it.category,
                "title": it.title,
                "creator": it.authors_or_creator,
                "description": it.raw_summary[:600],
            }
            for idx, it in enumerate(chunk)
        ]

        prompt = (
            "You are an expert AI research and engineering assistant curating a personal AI hub.\n"
            "Analyze each item in the input JSON array and return a JSON array of objects where each object has:\n"
            '  - "index": The integer index matching the input item.\n'
            '  - "ai_summary": A clear, jargon-free 2-sentence summary of what is new.\n'
            '  - "practical_takeaway": 1 concrete sentence explaining how an AI practitioner can use or apply this to elevate their workflow.\n'
            '  - "tags": An array of 2 to 4 lowercase capability tags (e.g., ["agents", "mcp", "rag", "fine-tuning", "coding", "multimodal", "local-ai", "prompting"]).\n\n'
            f"Input Items JSON:\n{json.dumps(items_payload, ensure_ascii=False)}\n"
        )

        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2},
        }

        try:
            raw_resp = http_request(url, method="POST", json_body=body, timeout=35)
            parsed_resp = json.loads(raw_resp.decode("utf-8"))
            text_out = parsed_resp["candidates"][0]["content"]["parts"][0]["text"]
            data = json.loads(text_out)
            if isinstance(data, dict):
                data = data.get("items") or data.get("results") or []

            enriched_by_idx: Dict[int, Dict[str, Any]] = {}
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict) and "index" in entry:
                        try:
                            enriched_by_idx[int(entry["index"])] = entry
                        except (ValueError, TypeError):
                            pass

            for idx, item in enumerate(chunk):
                res = enriched_by_idx.get(idx)
                if res:
                    item.ai_summary = clean_text(res.get("ai_summary"), 500)
                    item.practical_takeaway = clean_text(res.get("practical_takeaway"), 400)
                    tags = res.get("tags") or []
                    if isinstance(tags, list):
                        item.tags = [str(t).lower().strip() for t in tags[:5] if str(t).strip()]
                if not item.ai_summary or not item.practical_takeaway or not item.tags:
                    fallback_enrich(item)

        except Exception as exc:
            logger.warning(
                "Gemini batch enrichment fallback for chunk %d..%d: %s",
                start_idx,
                start_idx + len(chunk),
                exc,
            )
            for item in chunk:
                fallback_enrich(item)


# =============================================================================
# Storage Backends: Supabase REST (PostgREST) & Local SQLite Backend
# =============================================================================
class SQLiteStorage:
    """Zero-dependency local SQLite storage mirroring the Supabase schema."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sources (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    driver TEXT NOT NULL,
                    feed_url_or_query TEXT NOT NULL UNIQUE,
                    config TEXT NOT NULL DEFAULT '{}',
                    active INTEGER NOT NULL DEFAULT 1,
                    last_fetched_at TEXT,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS items (
                    external_id TEXT PRIMARY KEY,
                    source_id TEXT,
                    category TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    authors_or_creator TEXT,
                    published_at TEXT NOT NULL,
                    raw_summary TEXT,
                    ai_summary TEXT,
                    practical_takeaway TEXT,
                    tags TEXT NOT NULL DEFAULT '[]',
                    metrics TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'inbox',
                    is_bookmarked INTEGER NOT NULL DEFAULT 0,
                    is_read INTEGER NOT NULL DEFAULT 0,
                    personal_notes TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                """
            )
        self.ensure_default_sources()

    def ensure_default_sources(self) -> None:
        with self.conn:
            for src in DEFAULT_SOURCES:
                self.conn.execute(
                    """
                    INSERT INTO sources (id, name, category, driver, feed_url_or_query, config, active)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(feed_url_or_query) DO UPDATE SET
                        name = excluded.name,
                        config = excluded.config
                    """,
                    (
                        src.id,
                        src.name,
                        src.category,
                        src.driver,
                        src.feed_url_or_query,
                        json.dumps(src.config),
                    ),
                )

    def get_active_sources(self, category_filter: Optional[str] = None) -> List[SourceConfig]:
        query = "SELECT * FROM sources WHERE active = 1"
        params: Tuple[Any, ...] = ()
        if category_filter:
            query += " AND category = ?"
            params = (category_filter,)
        rows = self.conn.execute(query, params).fetchall()
        return [
            SourceConfig(
                id=row["id"],
                name=row["name"],
                category=row["category"],
                driver=row["driver"],
                feed_url_or_query=row["feed_url_or_query"],
                config=json.loads(row["config"] or "{}"),
            )
            for row in rows
        ]

    def add_source(self, src: SourceConfig) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO sources (id, name, category, driver, feed_url_or_query, config, active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    src.id,
                    src.name,
                    src.category,
                    src.driver,
                    src.feed_url_or_query,
                    json.dumps(src.config),
                ),
            )

    def get_existing_external_ids(self, external_ids: List[str]) -> Set[str]:
        if not external_ids:
            return set()
        placeholders = ",".join("?" for _ in external_ids)
        rows = self.conn.execute(
            f"SELECT external_id FROM items WHERE external_id IN ({placeholders})",
            tuple(external_ids),
        ).fetchall()
        return {row["external_id"] for row in rows}

    def item_exists(self, external_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM items WHERE external_id = ?", (external_id,)
        ).fetchone()
        return row is not None

    def upsert_items(
        self, new_items: List[HubItem], existing_items: Optional[List[HubItem]] = None
    ) -> int:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        all_items = list(new_items) + list(existing_items or [])
        with self.conn:
            for item in all_items:
                self.conn.execute(
                    """
                    INSERT INTO items (
                        external_id, source_id, category, title, url,
                        authors_or_creator, published_at, raw_summary,
                        ai_summary, practical_takeaway, tags, metrics, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(external_id) DO UPDATE SET
                        metrics = excluded.metrics,
                        updated_at = excluded.updated_at
                    """,
                    (
                        item.external_id,
                        item.source_id,
                        item.category,
                        item.title,
                        item.url,
                        item.authors_or_creator,
                        item.published_at,
                        item.raw_summary,
                        item.ai_summary,
                        item.practical_takeaway,
                        json.dumps(item.tags),
                        json.dumps(item.metrics),
                        now,
                    ),
                )
        return len(all_items)

    def mark_source_status(self, source_id: str, error: Optional[str] = None) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                "UPDATE sources SET last_fetched_at = ?, last_error = ? WHERE id = ?",
                (now, error, source_id),
            )


class SupabaseRestStorage:
    """Interacts directly with Supabase PostgREST endpoints using standard HTTPS."""

    def __init__(self, supabase_url: str, service_key: str) -> None:
        self.base_url = supabase_url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": service_key,
            "Content-Type": "application/json",
        }
        # Legacy Supabase keys are JWTs (eyJ...) and use Authorization: Bearer.
        # Modern Supabase API keys (sb_secret_... / sb_publishable_...) are opaque
        # tokens passed strictly via the `apikey` header.
        if not service_key.startswith("sb_"):
            self.headers["Authorization"] = f"Bearer {service_key}"

    def ensure_default_sources(self) -> None:
        """Upserts all DEFAULT_SOURCES into Supabase so new feeds added in git automatically take effect."""
        url = f"{self.base_url}/sources?on_conflict=feed_url_or_query"
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates"}
        payload = [
            {
                "name": src.name,
                "category": src.category,
                "driver": src.driver,
                "feed_url_or_query": src.feed_url_or_query,
                "config": src.config,
                "active": True,
            }
            for src in DEFAULT_SOURCES
        ]
        try:
            http_request(url, method="POST", headers=headers, json_body=payload)
        except Exception as exc:
            logger.warning("Could not sync default sources to Supabase: %s", exc)

    def get_active_sources(self, category_filter: Optional[str] = None) -> List[SourceConfig]:
        url = f"{self.base_url}/sources?active=eq.true&select=*"
        if category_filter:
            url += f"&category=eq.{urllib.parse.quote(category_filter)}"
        rows = json.loads(http_request(url, headers=self.headers).decode("utf-8"))
        return [
            SourceConfig(
                id=row["id"],
                name=row["name"],
                category=row["category"],
                driver=row["driver"],
                feed_url_or_query=row["feed_url_or_query"],
                config=row.get("config") or {},
            )
            for row in rows
        ]

    def add_source(self, src: SourceConfig) -> None:
        url = f"{self.base_url}/sources?on_conflict=feed_url_or_query"
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates"}
        body = {
            "name": src.name,
            "category": src.category,
            "driver": src.driver,
            "feed_url_or_query": src.feed_url_or_query,
            "config": src.config,
            "active": True,
        }
        http_request(url, method="POST", headers=headers, json_body=body)

    def get_existing_external_ids(self, external_ids: List[str]) -> Set[str]:
        if not external_ids:
            return set()
        quoted_ids = ",".join(
            '"' + eid.replace('\\', '\\\\').replace('"', '\\"') + '"'
            for eid in external_ids
        )
        in_param = urllib.parse.quote(f"({quoted_ids})", safe="(),\"")
        url = f"{self.base_url}/items?external_id=in.{in_param}&select=external_id"
        try:
            rows = json.loads(http_request(url, headers=self.headers).decode("utf-8"))
            return {row["external_id"] for row in rows if "external_id" in row}
        except Exception:
            return {eid for eid in external_ids if self.item_exists(eid)}

    def item_exists(self, external_id: str) -> bool:
        encoded = urllib.parse.quote(external_id, safe="")
        url = f"{self.base_url}/items?external_id=eq.{encoded}&select=external_id"
        rows = json.loads(http_request(url, headers=self.headers).decode("utf-8"))
        return len(rows) > 0

    def upsert_items(
        self, new_items: List[HubItem], existing_items: Optional[List[HubItem]] = None
    ) -> int:
        count = 0
        if new_items:
            url = f"{self.base_url}/items?on_conflict=external_id"
            headers = {**self.headers, "Prefer": "resolution=merge-duplicates"}
            payload = [
                {
                    "external_id": item.external_id,
                    "source_id": item.source_id,
                    "category": item.category,
                    "title": item.title,
                    "url": item.url,
                    "authors_or_creator": item.authors_or_creator,
                    "published_at": item.published_at,
                    "raw_summary": item.raw_summary,
                    "ai_summary": item.ai_summary,
                    "practical_takeaway": item.practical_takeaway,
                    "tags": item.tags,
                    "metrics": item.metrics,
                }
                for item in new_items
            ]
            http_request(url, method="POST", headers=headers, json_body=payload)
            count += len(payload)

        if existing_items:
            # Update only fresh metrics on already-enriched items so existing Gemini
            # summaries, practical takeaways, tags, and user workflow status are preserved.
            for item in existing_items:
                encoded = urllib.parse.quote(item.external_id, safe="")
                patch_url = f"{self.base_url}/items?external_id=eq.{encoded}"
                http_request(
                    patch_url,
                    method="PATCH",
                    headers=self.headers,
                    json_body={"metrics": item.metrics},
                )
                count += 1

        return count

    def mark_source_status(self, source_id: str, error: Optional[str] = None) -> None:
        url = f"{self.base_url}/sources?id=eq.{urllib.parse.quote(source_id)}"
        body = {
            "last_fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "last_error": error,
        }
        http_request(url, method="PATCH", headers=self.headers, json_body=body)


# =============================================================================
# Main Ingestion Orchestrator
# =============================================================================
def run_pipeline(args: argparse.Namespace) -> int:
    gemini_key = os.environ.get("GEMINI_API_KEY")
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_SECRET_KEY")
    )

    storage: Any
    if args.sqlite:
        logger.info("Using local SQLite database: %s", args.sqlite)
        storage = SQLiteStorage(args.sqlite)
    elif supabase_url and supabase_key:
        logger.info("Connecting to Supabase REST API: %s", supabase_url)
        storage = SupabaseRestStorage(supabase_url, supabase_key)
        if not args.dry_run:
            storage.ensure_default_sources()
    elif os.environ.get("GITHUB_ACTIONS") == "true" and not args.dry_run:
        logger.error(
            "Running inside GitHub Actions without SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY. "
            "Please configure repository secrets under Settings -> Secrets and variables -> Actions."
        )
        return 1
    else:
        default_sqlite = "ai_hub.db"
        logger.info(
            "SUPABASE_URL not set; defaulting to local SQLite database '%s'.",
            default_sqlite,
        )
        storage = SQLiteStorage(default_sqlite)

    # Handle dynamic source addition from CLI
    if args.add_source:
        if not (args.name and args.category and args.driver and args.query):
            logger.error(
                "--add-source requires --name, --category, --driver, and --query."
            )
            return 1
        new_src = SourceConfig(
            id=f"src-custom-{int(dt.datetime.now().timestamp())}",
            name=args.name,
            category=args.category,
            driver=args.driver,
            feed_url_or_query=args.query,
            config=json.loads(args.config_json or "{}"),
        )
        storage.add_source(new_src)
        logger.info(
            "Added new source '%s' (%s / %s) -> %s",
            new_src.name,
            new_src.category,
            new_src.driver,
            new_src.feed_url_or_query,
        )
        if args.only_add_source:
            return 0

    sources = storage.get_active_sources(category_filter=args.category)
    logger.info("Loaded %d active source(s) to poll.", len(sources))

    total_ingested = 0
    all_collected: List[HubItem] = []

    for src in sources:
        fetcher = DRIVER_DISPATCH.get(src.driver)
        if not fetcher:
            logger.warning("Unknown driver '%s' for source '%s'", src.driver, src.name)
            continue

        logger.info("Polling [%s] %s (%s)...", src.category.upper(), src.name, src.driver)
        try:
            fetched_items = fetcher(src)
            if args.limit:
                fetched_items = fetched_items[: args.limit]

            # Deduplicate within the same source batch by external_id
            deduped_items: List[HubItem] = []
            seen_in_source: Set[str] = set()
            for item in fetched_items:
                if item.external_id not in seen_in_source:
                    seen_in_source.add(item.external_id)
                    deduped_items.append(item)
            fetched_items = deduped_items

            existing_ids = storage.get_existing_external_ids(
                [item.external_id for item in fetched_items]
            )
            new_batch: List[HubItem] = []
            existing_batch: List[HubItem] = []

            for item in fetched_items:
                if item.external_id not in existing_ids:
                    new_batch.append(item)
                else:
                    fallback_enrich(item)
                    existing_batch.append(item)

            if new_batch:
                enrich_batch_with_gemini(new_batch, gemini_key)

            if args.dry_run:
                all_collected.extend(new_batch + existing_batch)
                logger.info(
                    "Dry-run: fetched %d item(s) (%d new) from '%s'.",
                    len(fetched_items),
                    len(new_batch),
                    src.name,
                )
            else:
                upserted = storage.upsert_items(new_batch, existing_batch)
                storage.mark_source_status(src.id, error=None)
                total_ingested += upserted
                logger.info(
                    "Processed %d item(s) (%d new, %d updated) from '%s'.",
                    upserted,
                    len(new_batch),
                    len(existing_batch),
                    src.name,
                )

        except Exception as exc:
            logger.error("Error polling '%s': %s", src.name, exc)
            if not args.dry_run:
                storage.mark_source_status(src.id, error=str(exc)[:500])

    if args.dry_run and args.export_json:
        with open(args.export_json, "w", encoding="utf-8") as f:
            json.dump([asdict(i) for i in all_collected], f, indent=2)
        logger.info("Exported %d items to %s", len(all_collected), args.export_json)

    logger.info("Ingestion run complete. Total items processed: %d", total_ingested)
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Automated Ingestion & AI Enrichment Worker for Personal AI Hub"
    )
    parser.add_argument(
        "--sqlite",
        metavar="DB_PATH",
        help="Run using a local SQLite database file instead of Supabase.",
    )
    parser.add_argument(
        "--category",
        choices=["paper", "github", "youtube", "news", "project"],
        help="Poll only sources matching a specific category.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap maximum items processed per source.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and enrich without writing to the database.",
    )
    parser.add_argument(
        "--export-json",
        metavar="FILE_PATH",
        help="When combined with --dry-run, write fetched items to a JSON file.",
    )
    # Dynamic source management flags
    parser.add_argument(
        "--add-source",
        action="store_true",
        help="Register a new feed, YouTube channel, or API query in the database.",
    )
    parser.add_argument(
        "--only-add-source",
        action="store_true",
        help="Exit immediately after registering the new source without polling.",
    )
    parser.add_argument("--name", help="Display name for the new source.")
    parser.add_argument(
        "--driver",
        choices=list(DRIVER_DISPATCH.keys()),
        help="Ingestion driver for the new source.",
    )
    parser.add_argument("--query", help="Feed URL, channel RSS URL, or search query string.")
    parser.add_argument(
        "--config-json",
        default="{}",
        help='Optional JSON config string (e.g. \'{"min_stars": 50, "max_results": 15}\').',
    )
    return parser


if __name__ == "__main__":
    cli_parser = build_arg_parser()
    sys.exit(run_pipeline(cli_parser.parse_args()))
