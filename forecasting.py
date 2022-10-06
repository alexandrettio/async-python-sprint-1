# import logging
# import threading
# import subprocess
# import multiprocessing

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask, DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    yw_data = list()
    for city in CITIES:
        fetcher = DataFetchingTask(city_name=city, yw_api=YandexWeatherAPI())
        calculator = DataCalculationTask(fetcher.run())
        yw_data.extend(calculator.run())
    aggregator = DataAggregationTask(yw_data, "csv")
    analyzer = DataAnalyzingTask(aggregator.run(), "csv")
    analyzer.run()


if __name__ == "__main__":
    forecast_weather()
