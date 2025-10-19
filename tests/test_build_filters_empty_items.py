import pytest
from pathlib import Path
from ednews import build


def test_build_filters_empty_items(tmp_path, monkeypatch):
    # Prepare templates
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()
    (templates_dir / "index.rss.jinja2").write_text("{% for a in articles %}ITEM: {{ a.title }}\n{% endfor %}", encoding="utf-8")
    (templates_dir / "articles.rss.jinja2").write_text("{% for a in articles %}A: {{ a.title }}\n{% endfor %}", encoding="utf-8")
    (templates_dir / "headlines.rss.jinja2").write_text("{% for a in articles %}H: {{ a.title }}\n{% endfor %}", encoding="utf-8")

    monkeypatch.setattr(build, "TEMPLATES_DIR", templates_dir)

    # No static needed
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    monkeypatch.setattr(build, "STATIC_DIR", static_dir)

    # Minimal planet
    planet = tmp_path / "research.json"
    planet.write_text('{"title": "X", "feeds": {}}', encoding="utf-8")
    monkeypatch.setattr(build, "PLANET_FILE", planet)

    # Prepare DB file path but we won't create a DB; build should handle missing DB
    monkeypatch.setattr(build, "DB_FILE", Path(tmp_path / "no_db.sqlite"))

    # Provide context with empty/meaningless items
    ctx = {
        "title": "X",
        "feeds": [],
        "articles": [
            {"title": "", "link": "", "content": ""},  # empty
            {"title": None, "link": None, "content": None},  # empty
            {"title": "Real Article", "link": "http://example.com", "content": ""},
        ],
        "news_headlines": [
            {"title": "", "link": "", "text": ""},
            {"title": "Headline", "link": "http://example.org", "text": "Some text"},
        ],
    }

    # Directly test the helper
    assert build.item_has_content({"title": "x"})
    assert build.item_has_content({"link": "http://example.com"})
    assert not build.item_has_content({"title": "", "link": "", "content": ""})

    # Now render the articles template with filtered items to ensure filtering removes empties
    articles_limit = getattr(build.config, 'ARTICLES_DEFAULT_LIMIT', 20)
    articles_items = [a for a in (ctx.get("articles") or []) if build.item_has_content(a)][:articles_limit]

    env = __import__('jinja2').Environment(loader=__import__('jinja2').FileSystemLoader(str(templates_dir)))
    tpl_articles = env.get_template('articles.rss.jinja2')
    rendered = tpl_articles.render({'articles': articles_items})
    assert 'Real Article' in rendered

    # Render headlines similarly
    headlines_items = [
        h for h in [{
            "title": hh.get("title") or "Untitled",
            "link": hh.get("link") or "",
            "content": hh.get("text") or "",
            "abstract": None,
            "published": hh.get("published") or None,
        } for hh in ctx.get("news_headlines")]
        if build.item_has_content(h)
    ]
    tpl_head = env.get_template('headlines.rss.jinja2')
    rendered_h = tpl_head.render({'articles': headlines_items})
    assert 'Headline' in rendered_h
