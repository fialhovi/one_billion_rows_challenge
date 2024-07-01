import time
from collections import Counter, defaultdict
from csv import reader

from tqdm import tqdm

NUMERO_DE_LINHAS = 1_000_000_000


def processar_temperaturas(path_do_csv):
    # Using positive and negative infinity for comparison
    minimas = defaultdict(lambda: float("inf"))
    maximas = defaultdict(lambda: float("-inf"))
    somas = defaultdict(float)
    medicoes = Counter()

    with open(path_do_csv, "r", encoding="utf-8") as file:
        _reader = reader(file, delimiter=";")
        # Using tqdm directly in the iterator, this will show the percentage of completion
        for row in tqdm(_reader, total=NUMERO_DE_LINHAS, desc="Processando"):
            nome_da_station, temperatura = str(row[0]), float(row[1])
            medicoes.update([nome_da_station])
            minimas[nome_da_station] = min(minimas[nome_da_station], temperatura)
            maximas[nome_da_station] = max(maximas[nome_da_station], temperatura)
            somas[nome_da_station] += temperatura

    print("Data loaded. Calculating statistics...")

    # Calculating min, mean and max for each station
    results = {}
    for station, qtd_medicoes in medicoes.items():
        mean_temp = somas[station] / qtd_medicoes
        results[station] = (minimas[station], mean_temp, maximas[station])

    print("Statistics calculated. Sorting...")
    # Sorting results by station
    sorted_results = dict(sorted(results.items()))

    # Formatting the results
    formatted_results = {
        station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
        for station, (min_temp, mean_temp, max_temp) in sorted_results.items()
    }

    return formatted_results


if __name__ == "__main__":
    path_do_csv = "data/measurements.txt"

    print("Starting file processing.")
    start_time = time.time()

    resultados = processar_temperaturas(path_do_csv)

    end_time = time.time()

    for station, metrics in resultados.items():
        print(station, metrics, sep=": ")

    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds.")
