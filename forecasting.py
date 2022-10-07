import logging
# import subprocess
import multiprocessing

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES

# import threading

logging.basicConfig(
    filename="sprint1.log",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s",
    level="INFO",
)


def forecast_weather():
    """
    Analyze weather by cities.
    """
    yw_data = list()
    fetching_pool = multiprocessing.Pool(processes=3)
    pool_outputs = fetching_pool.imap_unordered(DataFetchingTask, CITIES)
    for i in pool_outputs:
        calculator = DataCalculationTask(i.run())
        yw_data.extend(calculator.run())
    data, rating = DataAggregationTask(yw_data).run()
    analyzer = DataAnalyzingTask(data, rating, "csv")
    analyzer.run()


if __name__ == "__main__":
    forecast_weather()
