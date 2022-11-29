"""
Parsers for pod5 tools.

Each parser should have set_defaults(func=tool) called where tool is the core function
of a pod5 tool. These are commonly tool_pod5 (e.g. subset_pod5, merge_pod5)
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Optional

from pod5.signal_tools import DEFAULT_SIGNAL_CHUNK_SIZE


class SubcommandHelpFormatter(
    argparse.RawDescriptionHelpFormatter,
    argparse.ArgumentDefaultsHelpFormatter,
):
    """
    Helper function to prettier print subcommand help. This removes some
    extra lines of output when a final command parser is not selected.
    """

    def _format_action(self, action):
        parts = super(SubcommandHelpFormatter, self)._format_action(action)
        if action.nargs == argparse.PARSER:
            parts = "\n".join(parts.split("\n")[1:])
        return parts


def run_tool(parser: argparse.ArgumentParser) -> Any:
    """Run the tool prepared by an argparser"""
    kwargs = vars(parser.parse_args())
    tool_func = kwargs.pop("func")
    try:
        return tool_func(**kwargs)
    except Exception as exc:
        if os.getenv("POD5_DEBUG", 0):
            raise exc
        print(f"\nPOD5 has encountered an error: '{exc}'", file=sys.stderr)
        print("\nFor detailed information set POD5_DEBUG=1'", file=sys.stderr)
        exit(1)


#
# CONVERT - fast5
#
def pod5_convert_from_fast5_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """
    Create an argument parser for the pod5 convert-from-fast5 tool
    """

    _desc = "Convert a fast5 file into a pod5 file"

    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(
            name="fast5",
            aliases=["from_fast5"],
            description=_desc,
            formatter_class=SubcommandHelpFormatter,
        )

    parser.add_argument(
        "inputs", type=Path, nargs="+", help="Input path for fast5 file"
    )
    parser.add_argument(
        "output",
        type=Path,
        help="Output path for the pod5 file(s). This can be an existing "
        "directory (creating 'output.pod5' within it) or a new named file path. "
        "A directory must be given for --output-one-to-one.",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        default=False,
        action="store_true",
        help="Search for input files recursively",
    )
    parser.add_argument(
        "-t",
        "--threads",
        default=10,
        type=int,
        help="Set the number of threads to use [default: 10]",
    )
    parser.add_argument(
        "-O",
        "--output-one-to-one",
        type=Path,
        default=None,
        help="Output files are written 1:1 to inputs. 1:1 output files are "
        "written to the output directory in a new directory structure relative to the "
        "directory path provided to this argument. This directory path must be a "
        "relative parent of all inputs.",
    )
    parser.add_argument(
        "-f",
        "--force-overwrite",
        action="store_true",
        help="Overwrite destination files",
    )
    parser.add_argument(
        "--signal-chunk-size",
        default=DEFAULT_SIGNAL_CHUNK_SIZE,
        help="Chunk size to use for signal data set",
        type=int,
    )

    def run(**kwargs):
        from pod5.tools.pod5_convert_from_fast5 import convert_from_fast5

        return convert_from_fast5(**kwargs)

    parser.set_defaults(func=run)

    return parser


#
# CONVERT - to_fast5
#
def pod5_convert_to_fast5_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """
    Create an argument parser for the pod5 convert-to-fast5 tool
    """
    _desc = "Convert pod5 file(s) into fast5 file(s)"
    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(
            name="to_fast5",
            description=_desc,
            formatter_class=SubcommandHelpFormatter,
        )

    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "-r",
        "--recursive",
        default=False,
        action="store_true",
        help="Search for input files recursively",
    )
    parser.add_argument(
        "-t",
        "--threads",
        default=10,
        type=int,
        help="How many file writers to keep active [default: 10]",
    )
    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )
    parser.add_argument(
        "--file-read-count",
        default=4000,
        type=int,
        help="Number of reads to write per file",
    )

    def run(**kwargs):
        from pod5.tools.pod5_convert_to_fast5 import convert_to_fast5

        return convert_to_fast5(**kwargs)

    parser.set_defaults(func=run)

    return parser


#
# CONVERT - root
#
def prepare_pod5_convert(parent: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Create an argument paraser for the pod5 convert entry point"""

    _desc = "File conversion tools"

    convert_parser = parent.add_parser(
        name="convert",
        description=_desc,
        epilog="Example: pod5 convert fast5 input.fast5 output.pod5",
        formatter_class=SubcommandHelpFormatter,
    )
    convert_parser.set_defaults(func=lambda x: convert_parser.print_help())

    sub_convert_parser = convert_parser.add_subparsers(title="conversion type")

    pod5_convert_from_fast5_argparser(sub_convert_parser)
    pod5_convert_to_fast5_argparser(sub_convert_parser)

    return convert_parser


#
# Inspect
#
def prepare_pod5_inspect_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 inspect tool"""

    _desc = "Inspect the contents of a pod5 file"
    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(
            name="inspect",
            description=_desc,
            epilog="Example: pod5 inspect reads input.pod5",
        )

    def run(**kwargs):
        from pod5.tools.pod5_inspect import inspect_pod5

        return inspect_pod5(**kwargs)

    subparser = parser.add_subparsers(title="command", dest="command")
    summary_parser = subparser.add_parser(
        "summary",
        description="Print a summary of the contents of pod5 files",
        epilog="Example: pod5 inspect summary input.pod5",
    )
    summary_parser.add_argument("input_files", type=Path, nargs="+")
    summary_parser.set_defaults(func=run)

    reads_parser = subparser.add_parser(
        "reads",
        description="Print read information on all reads as a csv table",
        epilog="Example: pod5 inspect reads input.pod5",
    )
    reads_parser.add_argument("input_files", type=Path, nargs="+")
    reads_parser.set_defaults(func=run)

    read_parser = subparser.add_parser(
        "read",
        description="Print detailed read information for a named read id",
        epilog="Example: pod5 inspect read input.pod5 0000173c-bf67-44e7-9a9c-1ad0bc728e74",
    )
    read_parser.add_argument("input_files", type=Path, nargs=1)
    read_parser.add_argument("read_id", type=str)
    read_parser.set_defaults(func=run)

    debug_parser = subparser.add_parser(
        "debug",
        description="Print debugging information",
        epilog="Example: pod5 inspect debug input.pod5",
    )
    debug_parser.add_argument("input_files", type=Path, nargs=1)
    debug_parser.set_defaults(func=run)

    return parser


#
# MERGE
#
def prepare_pod5_merge_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 merge tool"""

    _desc = "Merge multiple pod5 files"

    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(
            name="merge",
            description=_desc,
            formatter_class=SubcommandHelpFormatter,
            epilog="Example: pod5 merge inputs/*.pod5 merged.pod5",
        )

    # Core arguments
    parser.add_argument(
        "inputs",
        type=Path,
        nargs="+",
        help="Pod5 filepaths to use as inputs",
    )
    parser.add_argument(
        "output",
        type=Path,
        help="Output filepath",
    )
    parser.add_argument(
        "-f",
        "--force_overwrite",
        action="store_true",
        help="Overwrite destination file if it already exists",
    )
    parser.add_argument(
        "-D",
        "--duplicate_ok",
        action="store_true",
        help="Allow duplicate read_ids",
    )

    def run(**kwargs):
        from pod5.tools.pod5_merge import merge_pod5

        return merge_pod5(**kwargs)

    parser.set_defaults(func=run)
    return parser


#
# Repack
#
def prepare_pod5_repack_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 repack tool"""

    _desc = "Repack a pod5 files into a single output"
    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(
            name="repack",
            description=_desc,
            epilog="Example: pod5 repack inputs/*.pod5 repacked/",
        )

    parser.add_argument(
        "inputs", type=Path, nargs="+", help="Input pod5 file(s) to repack"
    )
    parser.add_argument("output", type=Path, help="Output path for pod5 files")

    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )

    def run(**kwargs):
        from pod5.tools.pod5_repack import repack_pod5

        return repack_pod5(**kwargs)

    parser.set_defaults(func=run)
    return parser


#
# Subset
#
def prepare_pod5_subset_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 subset tool"""

    _desc = (
        "Given one or more pod5 input files, take subsets of reads "
        "into one or more pod5 output files by a user-supplied mapping."
    )
    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(
            name="subset",
            description=_desc,
            epilog="Example: pod5 subset inputs.pod5 --output subset_mux/ "
            "--summary summary.tsv --columns mux",
        )

    # Core arguments
    parser.add_argument(
        "inputs", type=Path, nargs="+", help="Pod5 filepaths to use as inputs"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path.cwd(),
        help="Destination directory to write outputs [cwd]",
    )
    parser.add_argument(
        "-f",
        "--force_overwrite",
        action="store_true",
        help="Overwrite destination files",
    )

    mapping_group = parser.add_argument_group("direct mapping")
    mapping_exclusive = mapping_group.add_mutually_exclusive_group(required=False)
    mapping_exclusive.add_argument(
        "--csv",
        type=Path,
        help="CSV file mapping output filename to read ids",
    )
    mapping_exclusive.add_argument(
        "--json", type=Path, help="JSON mapping output filename to array of read ids."
    )
    summary_group = parser.add_argument_group("sequencing summary mapping")
    summary_group.add_argument(
        "-s", "--summary", type=Path, help="Sequencing summary path"
    )
    summary_group.add_argument(
        "-r",
        "--read_id_column",
        type=str,
        default="read_id",
        help="Name of the read_id column in the summary. ['read_id']",
    )
    summary_group.add_argument(
        "-c",
        "--columns",
        type=str,
        nargs="+",
        help="Names of --summary columns to subset on",
    )
    summary_group.add_argument(
        "--template",
        type=str,
        default=None,
        help="template string to generate output filenames "
        '(e.g. "mux-{mux}_barcode-{barcode}.pod5"). '
        "default is to concatenate all columns to values as shown in the example.",
    )
    summary_group.add_argument(
        "-T",
        "--ignore_incomplete_template",
        action="store_true",
        default=None,
        help="Suppress the exception raised if the --template string does not contain "
        "every --subset_columns key",
    )

    content_group = parser.add_argument_group("content settings")
    content_group.add_argument(
        "-M",
        "--missing_ok",
        action="store_true",
        help="Allow missing read_ids",
    )
    content_group.add_argument(
        "-D",
        "--duplicate_ok",
        action="store_true",
        help="Allow duplicate read_ids",
    )

    def run(**kwargs):
        from pod5.tools.pod5_subset import subset_pod5

        return subset_pod5(**kwargs)

    parser.set_defaults(func=run)

    return parser


#
# Update
#
def prepare_pod5_update_argparser(
    parent: Optional[argparse._SubParsersAction] = None,
) -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 update tool"""

    _desc = "Update a pod5 files to the latest available version"
    if parent is None:
        parser = argparse.ArgumentParser(description=_desc)
    else:
        parser = parent.add_parser(name="update", description=_desc)

    parser.add_argument(
        "inputs", type=Path, nargs="+", help="Input pod5 file(s) to update"
    )
    parser.add_argument("output", type=Path, help="Output path for pod5 files")

    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )

    def run(**kwargs) -> Any:
        from pod5.tools.pod5_update import update_pod5

        return update_pod5(**kwargs)

    parser.set_defaults(func=run)

    return parser
