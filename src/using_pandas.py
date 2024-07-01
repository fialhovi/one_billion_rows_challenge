import pandas as pd


def create_pandas_df(filepath, chunksize=100_000_000):
    chunk_iter = pd.read_csv(
        filepath,
        sep=";",
        header=None,
        names=["station", "measure"],
        chunksize=chunksize,
    )

    # Dictionary to accumulate results
    agg_dict = {}

    for chunk in chunk_iter:
        chunk["station"] = chunk["station"].astype(str)
        chunk["measure"] = chunk["measure"].astype(float)

        grouped_chunk = chunk.groupby("station").agg(
            max_measure=("measure", "max"),
            min_measure=("measure", "min"),
            mean_measure=("measure", "mean"),
            count=("measure", "count"),  # To help in calculating the overall mean
        )

        for station, row in grouped_chunk.iterrows():
            if station not in agg_dict:
                agg_dict[station] = row
            else:
                agg_dict[station]["max_measure"] = max(
                    agg_dict[station]["max_measure"], row["max_measure"]
                )
                agg_dict[station]["min_measure"] = min(
                    agg_dict[station]["min_measure"], row["min_measure"]
                )
                agg_dict[station]["mean_measure"] = (
                    agg_dict[station]["mean_measure"] * agg_dict[station]["count"]
                    + row["mean_measure"] * row["count"]
                ) / (agg_dict[station]["count"] + row["count"])
                agg_dict[station]["count"] += row["count"]

    # Converting the dictionary to a DataFrame
    result_df = (
        pd.DataFrame.from_dict(agg_dict, orient="index")
        .reset_index()
        .rename(columns={"index": "station"})
    )
    result_df.drop(columns="count", inplace=True)
    sorted_df = result_df.sort_values(by="station").reset_index(drop=True)

    return sorted_df


if __name__ == "__main__":
    import time

    start_time = time.time()
    df = create_pandas_df("data/measurements.txt", chunksize=100_000_000)
    took = time.time() - start_time
    print(df)
    print(f"Pandas Took: {took:.2f} sec")
