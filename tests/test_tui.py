import sys
from types import SimpleNamespace
import importlib


def run_with_argv(argv, monkeypatch, tui_module=None):
    """Helper: import tui module and run with argv replaced.

    If tui_module is provided, it will be used instead of importing/reloading.
    """
    monkeypatch.setattr(sys, 'argv', argv)
    if tui_module is None:
        tui = importlib.import_module('ednews.cli')
    else:
        tui = tui_module
    # call run and return the loaded module for inspection
    try:
        tui.run()
    except SystemExit:
        # argparse may call sys.exit on -h; ignore
        pass
    return tui


def test_fetch_subcommand_invokes_fetch(monkeypatch):
    called = {}

    def fake_cmd_fetch(args):
        called['fetch'] = True

    # import module and monkeypatch handler, then run
    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_fetch', fake_cmd_fetch)

    run_with_argv(['ednews', 'fetch'], monkeypatch, tui_module=tui)
    assert called.get('fetch', False) is True


def test_manage_db_subcommand_invokes_cleanup(monkeypatch):
    called = {}

    def fake_cleanup(args):
        called['cleanup'] = args

    tui = importlib.import_module('ednews.cli')
    monkeypatch.setattr(tui, 'cmd_manage_db_cleanup', fake_cleanup)

    run_with_argv(['ednews', 'manage-db', 'cleanup-empty-articles', '--dry-run'], monkeypatch, tui_module=tui)
    assert 'cleanup' in called
    # ensure dry-run flag made it through
    assert getattr(called['cleanup'], 'dry_run', False) is True
