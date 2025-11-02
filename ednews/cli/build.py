from pathlib import Path
from typing import Any
from ednews import build as build_mod


def cmd_build(args: Any) -> None:
    """Render the static site into the output directory.

    Args:
        args: argparse namespace with optional .out_dir attribute.
    """
    out_dir = Path(args.out_dir) if args.out_dir else Path("build")
    build_mod.build(out_dir)
