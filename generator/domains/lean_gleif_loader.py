# lean_gleif_loader.py
# Usage:
#   df = read_gleif_reservoir("/data/lei_golden_copy.csv.gz",
#                             usecols=GLEIF_MIN_COLS, reservoir_size=250_000)
#   # or build a compact parquet once:
#   build_minimal_parquet("/data/lei_golden_copy.csv.gz", "gleif_minimal.parquet",
#                         usecols=GLEIF_MIN_COLS, chunksize=200_000)

from __future__ import annotations
import os, random, io, zipfile, gzip
from typing import List, Optional, Iterable
import pandas as pd

# Minimal column set you actually use downstream (tweak as needed)
GLEIF_MIN_COLS = [
    "LEI",
    "Entity.LegalName",
    "Entity.LegalAddress.AddressLine1","Entity.LegalAddress.AddressLine2","Entity.LegalAddress.AddressLine3",
    "Entity.LegalAddress.City","Entity.LegalAddress.Region","Entity.LegalAddress.PostalCode","Entity.LegalAddress.Country",
    "Entity.HeadquartersAddress.AddressLine1","Entity.HeadquartersAddress.AddressLine2","Entity.HeadquartersAddress.AddressLine3",
    "Entity.HeadquartersAddress.City","Entity.HeadquartersAddress.Region","Entity.HeadquartersAddress.PostalCode","Entity.HeadquartersAddress.Country",
]

def _open_csv_for_pandas(path: str) -> dict:
    """
    Returns kwargs for pandas.read_csv that handle .gz/.zip without loading to RAM.
    """
    if path.lower().endswith(".gz"):
        return {"filepath_or_buffer": path, "compression": "gzip"}
    if path.lower().endswith(".zip"):
        # Pandas can read a single-file zip by path directly.
        # If the zip has multiple CSVs, pandas reads the first one.
        return {"filepath_or_buffer": path, "compression": "zip"}
    return {"filepath_or_buffer": path}

def read_gleif_reservoir(
    path: str,
    usecols: Optional[List[str]] = None,
    reservoir_size: int = 250_000,
    chunksize: int = 200_000,
) -> pd.DataFrame:
    """
    Stream a huge GLEIF CSV/CSV.GZ/ZIP and keep only a uniform random sample
    of at most `reservoir_size` rows in memory. Returns a pandas DataFrame of the sample.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    # Make sure we only parse strings (no giant Python objects); pandas' native
    # 'string' dtype is more memory friendly than 'object' when combined with pyarrow backend.
    dtype = None  # let pandas read as object, we’ll convert at the end (fastest for wide text)
    read_kwargs = dict(_open_csv_for_pandas(path),
                       usecols=usecols,
                       dtype=dtype,
                       keep_default_na=False,
                       chunksize=chunksize,
                       low_memory=True,
                       memory_map=True)

    reservoir = []   # list[tuple]
    cols = None
    seen = 0

    for chunk in pd.read_csv(**read_kwargs):
        if cols is None:
            cols = list(chunk.columns)

        # Iterate as tuples (no per-row Series allocations)
        for row in chunk.itertuples(index=False, name=None):
            seen += 1
            if len(reservoir) < reservoir_size:
                reservoir.append(row)
            else:
                j = random.randint(1, seen)
                if j <= reservoir_size:
                    reservoir[j - 1] = row

    if not reservoir:
        return pd.DataFrame(columns=usecols or cols or [])

    out = pd.DataFrame.from_records(reservoir, columns=cols)

    # Downcast/optimize: use pandas' nullable string (pyarrow backend if available)
    try:
        # pandas >= 2 supports dtype_backend='pyarrow' for more compact memory
        out = out.convert_dtypes(dtype_backend="pyarrow")
    except Exception:
        # fallback: at least move to pandas' string dtype
        for c in out.columns:
            if out[c].dtype == object:
                out[c] = out[c].astype("string")

    return out


# ---------- Optional: one-off creation of a compact Parquet ----------
# Read once in chunks, write an on-disk parquet that loads much faster/lighter later.
def build_minimal_parquet(
    path: str,
    out_parquet: str,
    usecols: Optional[List[str]] = None,
    chunksize: int = 200_000,
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    read_kwargs = dict(_open_csv_for_pandas(path),
                       usecols=usecols,
                       dtype=None,
                       keep_default_na=False,
                       chunksize=chunksize,
                       low_memory=True,
                       memory_map=True)

    writer = None
    try:
        for chunk in pd.read_csv(**read_kwargs):
            # Convert chunk to Arrow table using types that compress well
            try:
                chunk = chunk.convert_dtypes(dtype_backend="pyarrow")
            except Exception:
                for c in chunk.columns:
                    if chunk[c].dtype == object:
                        chunk[c] = chunk[c].astype("string")
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_parquet, table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
