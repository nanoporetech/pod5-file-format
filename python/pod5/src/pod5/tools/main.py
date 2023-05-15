"""Main entry point for pod5 tools"""
import argparse
from typing import Any

from pod5 import __version__
from pod5.tools.parsers import (
    SubcommandHelpFormatter,
    prepare_pod5_convert,
    prepare_pod5_inspect_argparser,
    prepare_pod5_merge_argparser,
    prepare_pod5_repack_argparser,
    prepare_pod5_subset_argparser,
    prepare_pod5_filter_argparser,
    prepare_pod5_recover_argparser,
    prepare_pod5_update_argparser,
    prepare_pod5_view_argparser,
    run_tool,
)


def main() -> Any:
    """
    The core pod5 tools function which assembles the argparser and executes the required
    pod5 tool.
    """
    desc = (
        "**********      POD5 Tools      **********\n\n"
        "Tools for inspecting, converting, subsetting and formatting POD5 files"
    )

    parser = argparse.ArgumentParser(
        prog="pod5",
        description=desc,
        epilog="Example: pod5 convert fast5 input.fast5 --output output.pod5",
        formatter_class=SubcommandHelpFormatter,
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="Pod5 version: {}".format(__version__),
        help="Show pod5 version and exit.",
    )
    parser.set_defaults(func=lambda **_: parser.print_help())

    root = parser.add_subparsers(title="sub-commands")

    # add sub-parsers to the root argparser
    prepare_pod5_convert(root)
    prepare_pod5_inspect_argparser(root)
    prepare_pod5_merge_argparser(root)
    prepare_pod5_repack_argparser(root)
    prepare_pod5_subset_argparser(root)
    prepare_pod5_filter_argparser(root)
    prepare_pod5_recover_argparser(root)
    prepare_pod5_update_argparser(root)
    prepare_pod5_view_argparser(root)

    # Run the tool
    return run_tool(parser)


if __name__ == "__main__":
    main()
