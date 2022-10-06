import csv
import logging
from typing import List, Literal, Tuple, Dict

from pydantic import ValidationError

from api_client import YandexWeatherAPI
from model import YWResponse, CityWeatherData
from utils import BASE_CONDITIONS, AVG_TMP_STR, NO_CONDITIONS_STR

logging.basicConfig(
    filename="sprint1.log",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s",
    level="INFO"
)

FileFormat = Literal["json", "csv", "xls"]


class DataFetchingTask:
    """Выкачивание данных от Яндекс.Погоды по названию города и преобразует в объект YWResponse."""

    def __init__(self, city_name: str, yw_api: YandexWeatherAPI):
        self.task_name = "DataFetchingTask"
        self.city_name = city_name
        self.yw_api = yw_api

    def run(self) -> YWResponse:
        logging.info(f"{self.task_name} started for city {self.city_name}.")

        try:
            raw_response = self.yw_api.get_forecasting(self.city_name)
            parsed = YWResponse.parse_obj(raw_response)
            logging.info(f"{self.task_name} finished without errors for city {self.city_name}.")
            return parsed
        except ValidationError as e:
            logging.exception(f"{self.task_name} finished with error for city "
                              f"{self.city_name} during parsing response: {e}")
            raise e
        except Exception as e:  # Bad API: why raises common exception not customized?
            logging.exception(f"{self.task_name} finished with error for city: {self.city_name}. Got: {e}")
            raise e


class DataCalculationTask:
    """Выдает сренюю температуру и количество часов без осадков в городе за определенные даты.

    - период вычислений в течение дня — с 9 до 19 часов;
    - средняя температура рассчитывается за указанный промежуток времени;
    - сумма времени (часов), когда погода без осадков (без дождя, снега, града или грозы),
    рассчитывается за указанный промежуток времени;
    """

    def __init__(self, response: YWResponse):
        self.task_name = "DataCalculationTask"
        self.response = response

    def run(self) -> List[CityWeatherData]:
        logging.info(f"{self.task_name} started for city {self.response.geo_object.province.name}.")

        result = []
        for date_info in self.response.forecasts:
            temps = [h.temp for h in date_info.hours if 9 <= h.hour <= 19]
            without_conditions = [h.condition for h in date_info.hours
                                  if 9 <= h.hour <= 19 and h.condition not in BASE_CONDITIONS]
            if len(temps) == 11:  # if API returns not full day, period average stats will be ruined
                data = CityWeatherData(
                    city=self.response.geo_object.province.name,
                    date=date_info.date,
                    average_temperature=sum(temps) / len(temps),
                    without_conditions_hours=len(without_conditions)
                )
                result.append(data)
        logging.info(f"{self.task_name} finished for city {self.response.geo_object.province.name}. "
                     f"Got {len(result)} date weather records.")
        return result


class DataAggregationTask:
    """Группирует данные и создает таблицу с агрегированными данными."""

    def __init__(self, data: List[CityWeatherData], file_format: FileFormat):
        self.task_name = "DataAggregationTask"
        self.data = data
        self.format = file_format

    @staticmethod
    def init_grouped_data_dict():
        return {
            "sum": 0,
            "count": 0
        }

    def group_by_city(self) -> Dict[Tuple[str, str], Dict[str, float]]:
        grouped_data = dict()

        for item in self.data:
            if (item.city, AVG_TMP_STR) not in grouped_data:
                grouped_data[(item.city, AVG_TMP_STR)] = self.init_grouped_data_dict()
                grouped_data[(item.city, NO_CONDITIONS_STR)] = self.init_grouped_data_dict()

            date = item.date
            grouped_data[(item.city, AVG_TMP_STR)][date] = int(item.average_temperature)
            grouped_data[(item.city, AVG_TMP_STR)]["sum"] += item.average_temperature
            grouped_data[(item.city, AVG_TMP_STR)]["count"] += 1

            grouped_data[(item.city, NO_CONDITIONS_STR)][date] = int(item.without_conditions_hours)
            grouped_data[(item.city, NO_CONDITIONS_STR)]["sum"] += item.without_conditions_hours
            grouped_data[(item.city, NO_CONDITIONS_STR)]["count"] += 1

        return grouped_data

    def group_for_output(self) -> List[Dict]:
        data = self.group_by_city()
        output_data = list()
        for item, value_dict in data.items():
            value_dict["Среднее"] = round(value_dict.pop("sum") / value_dict.pop("count"), 1)
            if item[1] == AVG_TMP_STR:
                conditions_info = data[(item[0], NO_CONDITIONS_STR)]
                conditions_points = conditions_info["sum"] / conditions_info["count"]
                value_dict["points"] = int(value_dict["Среднее"] * 100 + conditions_points)
            else:
                temperature_info = data[(item[0], AVG_TMP_STR)]
                value_dict["points"] = int(temperature_info["Среднее"] * 100 + value_dict["Среднее"])
            output_data.append(
                {
                    "Город / день": item[0] if item[1] == AVG_TMP_STR else "",
                    "": item[1],
                    **value_dict
                }
            )
        return output_data

    def run(self):
        return self.group_for_output()


class DataAnalyzingTask:
    """
    Составляет рейтинг привлекательности городов и создает файл

    - Наиболее благоприятным городом считать тот, в котором средняя температура за всё время была самой высокой,
    а количество времени без осадков — максимальным. Если таких городов более одного, то выводить все.
    """

    def __init__(self, data: List[Dict], file_format: FileFormat):
        self.task_name = "DataAnalyzingTask"
        self.data = data
        self.format = file_format

    def get_scores(self):
        pass
        return self.data

    def sort_by_scores(self):
        pass

    def run(self):
        logging.info(f"{self.task_name} started. File format is {self.format}.")
        if self.format == "csv":
            with open("report.csv", "w") as f:
                data = self.get_scores()
                fieldnames = data[0].keys()
                writer = csv.DictWriter(f, fieldnames)
                writer.writeheader()
                writer.writerows(data)
        logging.info(f"{self.task_name} finished.")
