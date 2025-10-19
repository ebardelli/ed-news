#!/usr/bin/env python3
"""Thin wrapper entrypoint for ed-news CLI.

Delegates the full CLI implementation to `ednews.cli.run()` so the
top-level module remains small and import-safe.
"""
from ednews.cli import run


def main():
    run()


if __name__ == "__main__":
    main()
