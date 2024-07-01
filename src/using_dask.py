import dask
import dask.dataframe as dd


def create_dask_df():
    dask.config.set({"dataframe.query-planning": True})
    # Configuring Dask DataFrame to read the CSV file
    # Since the file doesn't have a header, we specify column names manually
    df = dd.read_csv(
        "data/measurements.txt", sep=";", header=None, names=["station", "measure"]
    )

    # Grouping by 'station' and calculating the maximum, minimum, and mean of 'measure'
    # Dask performs operations in a lazy form, so this part only defines the calculation
    grouped_df = (
        df.groupby("station")["measure"].agg(["max", "min", "mean"]).reset_index()
    )

    # Dask does not support direct sorting of grouped/derived DataFrames efficiently
    # But you can compute the result and then sort it if the resulting dataset is not too large
    # or if it is essential for the next processing step
    # Sorting will be performed after the .compute() call, if necessary

    return grouped_df


if __name__ == "__main__":
    import time

    start_time = time.time()
    df = create_dask_df()

    # The actual computation and sorting are done here
    result_df = df.compute().sort_values("station")
    took = time.time() - start_time

    print(result_df)
    print(f"Dask Took: {took:.2f} sec")
