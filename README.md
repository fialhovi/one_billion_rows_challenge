# One Billion Rows Challenge with Python

![cover_img](./pic/one_billion_rows.png 'One Billion Rows Challenge')

## Introduction

The objective of this project is to demonstrate how to efficiently process a massive data file containing 1 billion rows (~16GB), specifically to calculate statistics (including aggregation and sorting, which are heavy operations) using Python.

This challenge was inspired by the [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc), originally proposed for Java, and adapted for Python by [Luciano Galvão](https://www.linkedin.com/in/lucianovasconcelosf/).

This project also addressed the challenges of converting a CSV file to Parquet (GZIP) with a size larger than the available RAM. As a result, it was possible to achieve a compression of the file from 16GB to 3GB, optimizing storage and file reading time.

The data file consists of temperature measurements from various weather stations. Each record follows the format <string: station name>;<double: measurement>, with the temperature presented to one decimal place.

Here are ten example lines from the file:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. Johns;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The challenge is to develop a Python program capable of reading this file and calculating the minimum, average (rounded to one decimal place), and maximum temperature for each station, displaying the results in a table sorted by station name. For example:

| station       | min_temperature | mean_temperature | max_temperature |
| ------------- | --------------- | ---------------- | --------------- |
| Abha          | -31.1           | 18.0             | 66.5            |
| Abidjan       | -25.9           | 26.0             | 74.6            |
| Abéché        | -19.8           | 29.4             | 79.9            |
| Accra         | -24.8           | 26.4             | 76.3            |
| Addis Ababa   | -31.8           | 16.0             | 63.9            |
| Adelaide      | -31.8           | 17.3             | 71.5            |
| Aden          | -19.6           | 29.1             | 78.3            |
| Ahvaz         | -24.0           | 25.4             | 72.6            |
| Albuquerque   | -35.0           | 14.0             | 61.9            |
| Alexandra     | -40.1           | 11.0             | 67.9            |
| ...           | ...             | ...              | ...             |
| Yangon        | -23.6           | 27.5             | 77.3            |
| Yaoundé       | -26.2           | 23.8             | 73.4            |
| Yellowknife   | -53.4           | -4.3             | 46.7            |
| Yerevan       | -38.6           | 12.4             | 62.8            |
| Yinchuan      | -45.2           | 9.0              | 56.9            |
| Zagreb        | -39.2           | 10.7             | 58.1            |
| Zanzibar City | -26.5           | 26.0             | 75.2            |
| Zürich        | -42.0           | 9.3              | 63.6            |
| Ürümqi        | -42.1           | 7.4              | 56.7            |
| İzmir         | -34.4           | 17.9             | 67.9            |

## Dependencies

To run the scripts of this project, you will need the following libraries:

- Polars: `0.20.3`
- DuckDB: `0.10.0`
- Dask[complete]: `^2024.2.0`

## Results

The tests were conducted on a laptop equipped with an Intel(R) Core(TM) i7-8550U CPU @ 1.80GHz 1.99 GHz processor and 16GB of RAM. The implementations used pure Python, Pandas, Dask, Polars, and DuckDB approaches. The execution time results for processing the 1 billion rows file are presented below:

| Implementation  | Time       |
| --------------- | ---------- |
| Python          | 57 min     |
| Python + Pandas | 496.31 sec |
| Python + Dask   | 342.87 sec |
| Python + Polars | 258.57 sec |
| Python + DuckDB | 237.98 sec |

### DuckDB: CSV vs. Parquet

| Implementation  | File Format          | Time       |
| --------------- | -------------------- | ---------- |
| Python + DuckDB | CSV - 16GB           | 237.98 sec |
| Python + DuckDB | Parquet (Gzip) - 3GB | 32.90 sec  |

## Conclusion

This challenge clearly highlighted the effectiveness of various Python libraries in handling large volumes of data. Traditional methods such as pure Python (57 minutes), and even Pandas (8 minutes), required a series of tactics to implement batch processing, while libraries like Dask, Polars, and DuckDB proved to be exceptionally effective, requiring fewer lines of code due to their inherent ability to distribute data in more efficient streaming batches. DuckDB stood out, achieving the shortest execution time thanks to its execution strategy and data processing capabilities.

It was also possible to see how the Parquet file format (Gzip compression) is more efficient in storage compared to CSV files, in addition to other advantages, such as higher processing efficiency, incorporating the data schema, and easy portability. Overall, Parquet format demonstrated very high performance in analytical queries and large-scale data processing operations.

These results emphasize the importance of selecting the appropriate tool for large-scale data analysis, demonstrating that Python, with the right libraries, is a powerful choice for tackling big data challenges.

## How to Run

To run this project and reproduce the results:

1. Clone this repository
2. Set the Python version using `pyenv local 3.12.1` (make sure you have pyenv installed)
3. Run `poetry env use 3.12.1`, `poetry install --no-root`, and `poetry lock --no-update` (make sure you have poetry installed)
4. Set the Python interpreter to the Poetry .venv
5. Run the command `python src/create_measurements.py` to generate the test file (CSV)
6. Be patient and go make a coffee, it will take about 15 minutes to generate the file
7. Run the command `python src/convert_to_parquet.py` to generate the Parquet file from the CSV, it will take about 20 minutes
8. Run the scripts `python src/using_python.py`, `python src/using_pandas.py`, `python src/using_dask.py`, `python src/using_polars.py`, `python src/using_duckdb.py`, and `python src/using_duckdb_parquet.py` through the terminal.

## References

TUTORIAL Apache Parquet em Python, Let's Data. Here: https://www.youtube.com/watch?v=BztuEQ9ojtc.

A Deep Dive into Parquet: The Data Format Engineers Need to Know, Airbyte Blog. Here: https://airbyte.com/data-engineering-resources/parquet-data-format.

Parquet Documentation, Compression section. Here: https://parquet.apache.org/docs/file-format/data-pages/compression/.
