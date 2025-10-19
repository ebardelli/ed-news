import sys
import importlib


def run_args(argv, monkeypatch, tui):
    monkeypatch.setattr(sys, 'argv', argv)
    try:
        tui.run()
    except SystemExit:
        # argparse may call SystemExit for -h
        pass


def test_build_subcommand_sets_out_dir(monkeypatch):
    called = {}

    def fake_build(args):
        called['out_dir'] = getattr(args, 'out_dir', None)

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_build', fake_build)

    run_args(['ednews', 'build', '--out-dir', 'out_folder'], monkeypatch, tui)
    assert called.get('out_dir') == 'out_folder'


def test_embed_subcommand_defaults_and_flags(monkeypatch):
    called = {}

    def fake_embed(args):
        called['model'] = getattr(args, 'model', None)
        called['batch_size'] = getattr(args, 'batch_size', None)
        called['articles'] = getattr(args, 'articles', False)
        called['headlines'] = getattr(args, 'headlines', False)

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_embed', fake_embed)

    # default call (no flags) should set defaults
    run_args(['ednews', 'embed'], monkeypatch, tui)
    assert called['model'] is None
    assert called['batch_size'] == 64

    # pass flags
    run_args(['ednews', 'embed', '--model', 'm1', '--batch-size', '16', '--articles'], monkeypatch, tui)
    assert called['model'] == 'm1'
    assert called['batch_size'] == 16
    assert called['articles'] is True


def test_issn_lookup_parsing(monkeypatch):
    called = {}

    def fake_issn(args):
        called['per_journal'] = getattr(args, 'per_journal', None)
        called['timeout'] = getattr(args, 'timeout', None)
        called['delay'] = getattr(args, 'delay', None)

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_issn_lookup', fake_issn)

    run_args(['ednews', 'issn-lookup', '--per-journal', '5', '--timeout', '1.5', '--delay', '0.01'], monkeypatch, tui)
    assert called['per_journal'] == 5
    assert abs(called['timeout'] - 1.5) < 1e-6
    assert abs(called['delay'] - 0.01) < 1e-6


def test_headlines_flags(monkeypatch):
    called = {}

    def fake_headlines(args):
        called['out'] = getattr(args, 'out', None)
        called['no_persist'] = getattr(args, 'no_persist', False)

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_headlines', fake_headlines)

    run_args(['ednews', 'headlines', '--out', 'news.json', '--no-persist'], monkeypatch, tui)
    assert called['out'] == 'news.json'
    assert called['no_persist'] is True


def test_enrich_crossref_flags(monkeypatch):
    called = {}

    def fake_enrich(args):
        called['batch_size'] = getattr(args, 'batch_size', None)
        called['delay'] = getattr(args, 'delay', None)

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_enrich_crossref', fake_enrich)

    run_args(['ednews', 'enrich-crossref', '--batch-size', '10', '--delay', '0.2'], monkeypatch, tui)
    assert called['batch_size'] == 10
    assert abs(called['delay'] - 0.2) < 1e-6


def test_manage_db_run_all_dry_run_and_older_than(monkeypatch):
    called = {}

    def fake_run_all(args):
        called['dry_run'] = getattr(args, 'dry_run', False)
        called['older_than_days'] = getattr(args, 'older_than_days', None)

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_manage_db_run_all', fake_run_all)

    run_args(['ednews', 'manage-db', 'run-all', '--dry-run', '--older-than-days', '7'], monkeypatch, tui)
    assert called['dry_run'] is True
    assert called['older_than_days'] == 7


def test_help_exits_cleanly(monkeypatch):
    # Ensure -h triggers SystemExit but does not raise other exceptions
    tui = importlib.import_module('ednews.cli')
    try:
        run_args(['ednews', '-h'], monkeypatch, tui)
    except Exception as e:
        raise AssertionError(f"help raised unexpected exception: {e}")
