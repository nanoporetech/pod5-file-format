from typing import Optional
import polars as pl

# Reserved column names used in polars dataframes
PL_DEST_FNAME = "__dest_fname"
PL_SRC_FNAME = "__src_fname"
PL_READ_ID = "__read_id"
PL_UUID_REGEX = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


def pl_format_read_id(read_id_col: pl.Expr) -> pl.Expr:
    """Format read ids to in UUID style"""
    read_id = read_id_col.bin.encode("hex")
    return pl.format(
        "{}-{}-{}-{}-{}",
        read_id.str.slice(0, 8),
        read_id.str.slice(8, 4),
        read_id.str.slice(12, 4),
        read_id.str.slice(16, 4),
        read_id.str.slice(20, 12),
    )


def pl_format_empty_string(expr: pl.Expr, subst: Optional[str]) -> pl.Expr:
    """Empty strings are read as a pair of double-quotes which need to be removed"""
    return pl.when(expr.str.lengths() == 0).then(pl.lit(subst)).otherwise(expr)
