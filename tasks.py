import csv
import json
import threading
from collections import defaultdict
from multiprocessing import Process, Queue
from typing import Literal

from pydantic import ValidationError
from xlsxwriter import Workbook

from api_client import YandexWeatherAPI
from model import CityWeatherData, YWResponse
from utils import (AVG_STR, AVG_TMP_STR, BASE_CONDITIONS, HOURS_COUNT,
                   HOURS_END, HOURS_START, NO_CONDITIONS_STR, logger)

FileFormat = Literal["json", "csv", "xls"]


class PickleHackStub:
    def __getstate__(self):
        """called when pickling - this hack allows subprocesses to
        be spawned without the AuthenticationString raising an error"""
        state = self.__dict__.copy()
        conf = state["_config"]
        if "authkey" in conf:
            conf["authkey"] = bytes(conf["authkey"])
        return state

    def __setstate__(self, state):
        """for unpickling"""
        state["_config"]["authkey"] = None
        self.__dict__.update(state)


class DataFetchingTask:
    """Requests data from Yandex Weather by city and represents as YWResponse object."""

    def __init__(self, city_name: str):
        self.task_name = "DataFetchingTask"
        self.city_name = city_name
        self.yw_api = YandexWeatherAPI()

    def run(self) -> YWResponse:
        logger.info(f"{self.task_name} started for city {self.city_name}.")
        try:
            raw_response = self.yw_api.get_forecasting(self.city_name)
            parsed = YWResponse.parse_obj(raw_response)
            logger.info(
                f"{self.task_name} finished without errors for city {self.city_name}."
            )
            return parsed
        except ValidationError as e:
            logger.exception(
                f"{self.task_name} finished with error for city "
                f"{self.city_name} during parsing response: {e}"
            )
            raise e
        except Exception as e:  # Bad API: why raises common exception not customized?
            logger.exception(
                f"{self.task_name} finished with error for city: {self.city_name}. Got: {e}"
            )
            raise e


class DataCalculationTask(Process, PickleHackStub):
    """Calculates average temperature and count of hours without conditions in the city for dates.
    If calculation period is not completed, data will be passed and not take part in statistics.

    - day calculation period ??? from 9am to 7pm;
    """

    def __init__(self, response: YWResponse, queue: Queue):
        super().__init__()
        self.task_name = "DataCalculationTask"
        self.response = response
        self.queue = queue

    def run(self) -> None:
        logger.info(
            f"{self.task_name} started for city {self.response.geo_object.province.name}."
        )
        result = []
        for date_info in self.response.forecasts:
            temps = [
                h.temp for h in date_info.hours if HOURS_START <= h.hour <= HOURS_END
            ]
            without_conditions = [
                h.condition for h in date_info.hours
                if HOURS_START <= h.hour <= HOURS_END and h.condition not in BASE_CONDITIONS
            ]
            if (
                    len(temps) == HOURS_COUNT
            ):  # if API returns not full day, period average stats will be ruined
                data = CityWeatherData(
                    city=self.response.geo_object.province.name,
                    date=date_info.date,
                    average_temperature=sum(temps) / len(temps),
                    without_conditions_hours=len(without_conditions),
                )
                self.queue.put(data)
        logger.info(
            f"{self.task_name} finished for city {self.response.geo_object.province.name}. "
            f"Got {len(result)} date weather records."
        )


class DataAggregationTask(Process, PickleHackStub):
    """Aggregates data for different days, scores absolute city attraction."""

    def __init__(self, queue: Queue):
        super().__init__()
        self.task_name = "DataAggregationTask"
        self.queue = queue

    def group_by_city(self) -> dict[str, dict[str, dict[str, float]]]:
        grouped_data = dict()

        while not self.queue.empty():
            item = self.queue.get()

            if item.city not in grouped_data:
                grouped_data[item.city] = defaultdict(dict)
                grouped_data[item.city]["sum"][AVG_TMP_STR] = 0
                grouped_data[item.city]["sum"][NO_CONDITIONS_STR] = 0

            date = item.date
            grouped_data[item.city][date][AVG_TMP_STR] = int(item.average_temperature)
            grouped_data[item.city][date][NO_CONDITIONS_STR] = int(
                item.without_conditions_hours
            )

            grouped_data[item.city]["sum"][AVG_TMP_STR] += item.average_temperature
            grouped_data[item.city]["sum"][
                NO_CONDITIONS_STR
            ] += item.without_conditions_hours
        return grouped_data

    @staticmethod
    def count_points(info: dict[str, float]) -> int:
        return int(info[AVG_TMP_STR] * 100 + info[NO_CONDITIONS_STR])

    def count_average_and_rating(
            self, data: dict[str, dict[str, dict]]
    ) -> tuple[dict[str, dict], dict[int, list[str]]]:
        rating = defaultdict(list)
        for city, info in data.items():
            data[city][AVG_STR][AVG_TMP_STR] = (
                    data[city]["sum"][AVG_TMP_STR] / HOURS_COUNT
            )
            data[city][AVG_STR][NO_CONDITIONS_STR] = (
                    data[city]["sum"][NO_CONDITIONS_STR] / HOURS_COUNT
            )
            data[city].pop("sum")

            points = self.count_points(data[city][AVG_STR])
            rating[points].append(city)
        return data, rating

    def run(self) -> tuple[dict[str, dict], dict[int, list[str]]]:
        return self.count_average_and_rating(self.group_by_city())


class DataAnalyzingTask(threading.Thread):
    """Aggregates city attraction rating and write it down in the file with selected format."""

    def __init__(
            self,
            data: dict[str, dict],
            rating: dict[int, list[str]],
            file_format: FileFormat,
    ):
        super().__init__()
        self.task_name = "DataAnalyzingTask"
        self.data = data
        self.rating = rating
        self.format = file_format

    def group_table_ordered_by_points(self) -> list[dict]:
        result = list()
        index = 1
        for points in sorted(self.rating, reverse=True):
            for city_name in self.rating[points]:
                avgs = dict()
                conditions = dict()
                for k, v in self.data[city_name].items():
                    avgs[k] = round(v[AVG_TMP_STR], 1)
                    conditions[k] = round(v[NO_CONDITIONS_STR], 1)

                # 2 lines for one city as in example.
                result.append(
                    {
                        "??????????/????????": city_name,
                        "": AVG_TMP_STR,
                        **avgs,
                        "??????????????": index,
                    }
                )
                result.append(
                    {
                        "??????????/????????": None,
                        "": NO_CONDITIONS_STR,
                        **conditions,
                        "??????????????": None,
                    }
                )

            if index == 1:
                if len(self.rating[points]) == 1:
                    logger.info(f"Best city for vacation is {self.rating[points][0]}!")
                else:
                    logger.info(f"Best cities for vacation are {', '.join(self.rating[points])}")
            index += 1
        return result

    @staticmethod
    def write_csv(data: list[dict], fieldnames: list[str]):
        logger.info(f"write_csv started.")
        with open("report.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"write_csv finished.")

    @staticmethod
    def write_json(data: list[dict]):
        logger.info(f"write_json started.")
        with open("report.json", "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"write_json finished.")

    @staticmethod
    def write_xls(data: list[dict], fieldnames: list[str]):
        logger.info(f"write_xls started.")
        with Workbook("report.xlsx") as workbook:
            worksheet = workbook.add_worksheet()
            worksheet.write_row(row=0, col=0, data=fieldnames)
            for index, item in enumerate(data):
                row = map(lambda field_id: item.get(field_id, ""), fieldnames)
                worksheet.write_row(row=index + 1, col=0, data=row)
        logger.info(f"write_xls finished.")

    def run(self):
        logger.info(f"{self.task_name} started. File format is {self.format}.")
        data = self.group_table_ordered_by_points()
        fieldnames = list(data[0].keys())
        if self.format == "csv":
            self.write_csv(data, fieldnames)
        elif self.format == "json":
            self.write_json(data)
        elif self.format == "xls":
            self.write_xls(data, fieldnames)
        logger.info(f"{self.task_name} finished.")
