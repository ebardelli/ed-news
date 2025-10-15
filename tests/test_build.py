from pathlib import Path
import sqlite3
import pytest

from ednews import build


def test_read_planet_parses_sections(tmp_path):
    p = tmp_path / "planet.ini"
    p.write_text(
        """
[feed1]
title = Feed One
link = http://example.com
feed = http://example.com/feed

[feed2]
title = Feed Two
link = http://example.org
feed = http://example.org/feed
""",
        encoding="utf-8",
    )

    result = build.read_planet(p)
    assert result["title"] == "Latest Research Articles in Education"
    assert isinstance(result["feeds"], list)
    ids = {f["id"] for f in result["feeds"]}
    assert "feed1" in ids and "feed2" in ids


def test_read_planet_missing(tmp_path):
    p = tmp_path / "planet.ini"
    p.write_text("title = Example")
    res = build.read_planet(p)
    assert isinstance(res, dict)
    assert "title" in res


def test_render_templates_writes_output(tmp_path, monkeypatch):
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()
    (templates_dir / "index.html.jinja2").write_text("Hello {{ title }}", encoding="utf-8")

    monkeypatch.setattr(build, "TEMPLATES_DIR", templates_dir)

    out = tmp_path / "out"
    out.mkdir()
    build.render_templates({"title": "X"}, out)
    assert (out / "index.html").exists()
    assert "Hello X" in (out / "index.html").read_text(encoding="utf-8")


def test_copy_static_copies_directory(tmp_path, monkeypatch):
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "css.txt").write_text("body {}", encoding="utf-8")

    monkeypatch.setattr(build, "STATIC_DIR", static_dir)
    out = tmp_path / "out"
    out.mkdir()
    build.copy_static(out)
    assert (out / "static" / "css.txt").exists()


def test_build_smoke(tmp_path, monkeypatch):
    planet = tmp_path / "planet.ini"
    planet.write_text("[feed1]\ntitle = F\n", encoding="utf-8")

    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()
    (templates_dir / "index.html.jinja2").write_text("Site: {{ title }}", encoding="utf-8")

    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "a.txt").write_text("x", encoding="utf-8")

    monkeypatch.setattr(build, "TEMPLATES_DIR", templates_dir)
    monkeypatch.setattr(build, "STATIC_DIR", static_dir)
    monkeypatch.setattr(build, "PLANET_FILE", planet)
    monkeypatch.setattr(build, "DB_FILE", Path(tmp_path / "no_db.sqlite"))

    out = tmp_path / "site"
    build.build(out_dir=out)

    assert (out / "index.html").exists()
    assert (out / "static" / "a.txt").exists()


def test_read_articles_empty_view(tmp_path):
    db = tmp_path / "ednews.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, doi TEXT)")
    conn.commit()
    conn.close()

    res = build.read_articles(db)
    assert isinstance(res, list)
