import time

import duckdb


def create_duckdb():
    duckdb.sql(
        """
        SELECT station,
            MIN(measure) AS min_temperature,
            AVG(measure) AS mean_temperature,
            MAX(measure) AS max_temperature
        FROM read_parquet("data/measurements.parquet")
        GROUP BY station
        ORDER BY station
    """
    ).show()


if __name__ == "__main__":
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")
