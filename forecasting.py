import logging
from concurrent.futures import ThreadPoolExecutor

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES

logging.basicConfig(
    filename="sprint1.log",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s",
    level="INFO",
)


def forecast_weather():
    """
    Analyze weather by cities.
    """
    with ThreadPoolExecutor(max_workers=5) as pool:
        fetching_pool_outputs = pool.map(DataFetchingTask, CITIES)

    yw_data = list()
    for i in fetching_pool_outputs:
        calculator = DataCalculationTask(i.run())
        yw_data.extend(calculator.run())
    data, rating = DataAggregationTask(yw_data).run()
    analyzer = DataAnalyzingTask(data, rating, "csv")
    analyzer.run()


if __name__ == "__main__":
    forecast_weather()
