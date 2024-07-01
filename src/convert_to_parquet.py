import numbers
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_chunksize_by_max_ram_mb(file_path: Path, max_ram_mb_per_chunk: int) -> int:
    """Returns the amount of rows (chunksize) of a CSV that is approximately
    equivalent to the maximum RAM consumption defined.

    Args:
        file_path (Path): csv file path
        max_ram_mb_per_chunk (int): maximum consumption of RAM in mb

    Returns:
        int: chunksize
    """
    mb_size = Path(file_path).stat().st_size / (1024**2)
    num_lines = sum(1 for _ in open(file_path, encoding="utf-8"))
    rows_per_chunk = (
        int(max_ram_mb_per_chunk / mb_size * num_lines / 3.5 / 10000) * 10000
    )
    return rows_per_chunk


def auto_opt_pd_dtypes(df_: pd.DataFrame, inplace=False) -> Optional[pd.DataFrame]:
    """Automatically downcast Number dtypes for minimal possible,
    will not touch other (datetime, str, object, etc)
    Ref.: https://stackoverflow.com/a/67403354
    :param df_: dataframe
    :param inplace: if False, will return a copy of input dataset
    :return: `None` if `inplace=True` or dataframe if `inplace=False`

    Opportunities for Improvement
    Optimize Object column for categorical
    Ref.: https://github.com/safurrier/data_science_toolbox/blob/master/data_science_toolbox/pandas/optimization/dtypes.py#L56
    """

    df = df_ if inplace else df_.copy()

    for col in df.columns:
        # integers
        if issubclass(df[col].dtypes.type, numbers.Integral):
            # unsigned integers
            if df[col].min() >= 0:
                df[col] = pd.to_numeric(df[col], downcast="unsigned")
            # signed integers
            else:
                df[col] = pd.to_numeric(df[col], downcast="integer")
        # other real numbers
        elif issubclass(df[col].dtypes.type, numbers.Real):
            df[col] = pd.to_numeric(df[col], downcast="float")

    if not inplace:
        return df


def get_dtype_opt(csv_file_path, sep, chunksize, encoding):
    """
    Identifies the optimized data type of each column by analyzing
    the entire dataframe by chunks.
    Ref.: https://stackoverflow.com/a/15556579

    return: dtype dict to pass as dtype argument of pd.read_csv
    """

    list_chunk = pd.read_csv(
        csv_file_path,
        sep=sep,
        chunksize=chunksize,
        header=0,
        low_memory=True,
        encoding=encoding,
    )

    list_chunk_opt = []
    for chunk in list_chunk:
        chunk_opt = auto_opt_pd_dtypes(chunk, inplace=False)
        list_chunk_opt.append(chunk_opt.dtypes)

    df_dtypes = pd.DataFrame(list_chunk_opt)
    dict_dtypes = df_dtypes.apply(lambda x: np.result_type(*x), axis=0).to_dict()
    return dict_dtypes


def get_chunksize_opt(
    csv_file_path, sep, dtype, max_ram_mb_per_chunk, chunksize, encoding
):
    """After dtype optimization, analyzing only one data chunk,
    returns the amount of rows (chunksize) of a CSV that is
    approximately equivalent to the maximum RAM consumption.
    """

    for chunk in pd.read_csv(
        csv_file_path,
        sep=sep,
        dtype=dtype,
        chunksize=chunksize,
        low_memory=True,
        encoding=encoding,
    ):
        chunksize_opt = chunksize * (
            max_ram_mb_per_chunk / (chunk.memory_usage(deep=True).sum() / (1024**2))
        )
        break
    return int(chunksize_opt / 10_000) * 10_000


def write_parquet(csv_file_path, parquet_file_path, sep, dtype, chunksize, encoding):
    """Write Parquet file from a CSV with defined dtypes and
    by chunks for RAM optimization.
    """

    for i, chunk in enumerate(
        pd.read_csv(
            csv_file_path,
            sep=sep,
            dtype=dtype,
            chunksize=chunksize,
            low_memory=True,
            encoding=encoding,
            header=None,
            names=["station", "measure"],
        )
    ):
        if i == 0:
            # Guess the schema of the CSV file from the first chunk
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            # Open a Parquet file for writing
            parquet_writer = pq.ParquetWriter(
                parquet_file_path, parquet_schema, compression="gzip"
            )
        # Write CSV chunk to the parquet file
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)

    parquet_writer.close()


def convert_csv_to_parquet(
    csv_file_path,
    parquet_file_path,
    max_ram_mb_per_chunk,
    sep=",",
    encoding="utf-8",
):
    """Converts a CSV file to Parquet file, with maximum RAM consumption
    limit allowed and automatically optimizing the data types of each column.
    """

    chunksize = get_chunksize_by_max_ram_mb(csv_file_path, max_ram_mb_per_chunk)
    dict_dtypes_opt = get_dtype_opt(csv_file_path, sep, chunksize, encoding)
    chunksize_opt = get_chunksize_opt(
        csv_file_path,
        sep,
        dict_dtypes_opt,
        max_ram_mb_per_chunk,
        chunksize,
        encoding,
    )
    write_parquet(
        csv_file_path,
        parquet_file_path,
        sep,
        dict_dtypes_opt,
        chunksize_opt,
        encoding,
    )


if __name__ == "__main__":
    start_time = time.time()
    print("Converting the file into Parquet... this will take about 20 minutes")

    convert_csv_to_parquet(
        csv_file_path="./data/measurements.txt",
        parquet_file_path="./data/measurements.parquet",
        max_ram_mb_per_chunk=100,
        sep=";",
        encoding="utf-8",
    )

    end_time = time.time()
    elapsed_time = end_time - start_time

    print("File successfully written to data/measurements.parquet")
    print(f"Duration: {elapsed_time:.2f} seconds")

exit()
