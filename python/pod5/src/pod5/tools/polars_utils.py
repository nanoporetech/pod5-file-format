from typing import Optional
import polars as pl
import pyarrow as pa

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
    return pl.when(expr.str.len_bytes() == 0).then(pl.lit(subst)).otherwise(expr)


def pl_from_arrow(table: pa.Table, rechunk: bool) -> pl.DataFrame:
    """Workaround failure to read our arrow extension type"""
    # Based on https://github.com/pola-rs/polars/issues/20700

    def remove_pod5_metadata(field: pa.Field) -> pa.Field:
        metadata = field.metadata
        if metadata is not None and metadata.get(b"ARROW:extension:name") in [
            b"minknow.uuid",
            b"minknow.vbz",
        ]:
            del metadata[b"ARROW:extension:name"]
            field = field.remove_metadata().with_metadata(metadata)
        return field

    table = pa.Table.from_batches(
        table.to_batches(),
        schema=pa.schema([remove_pod5_metadata(field) for field in table.schema]),
    )
    return pl.from_arrow(table, rechunk=rechunk)


def pl_from_arrow_batch(record_batch: pa.RecordBatch, rechunk: bool) -> pl.DataFrame:
    """Workaround failure to read our arrow extension type"""
    table = pa.Table.from_batches([record_batch])
    return pl_from_arrow(table, rechunk=rechunk)
