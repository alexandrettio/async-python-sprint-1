from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Analyze weather by cities.
    """
    queue = Queue()
    with ThreadPoolExecutor() as pool:
        fetching_pool_outputs = pool.map(DataFetchingTask, CITIES)

    for i in fetching_pool_outputs:
        DataCalculationTask(i.run(), queue).run()
    data, rating = DataAggregationTask(queue).run()
    analyzer = DataAnalyzingTask(data, rating, "csv")
    analyzer.run()


if __name__ == "__main__":
    forecast_weather()
