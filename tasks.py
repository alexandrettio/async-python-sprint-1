import csv
import logging
from typing import List, Literal, Tuple, Dict

from pydantic import ValidationError

from api_client import YandexWeatherAPI
from model import YWResponse, CityWeatherData

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s'
)

FileFormat = Literal["json", "csv", "xls"]


class DataFetchingTask:
    """Выкачивание данных от Яндекс.Погоды по названию города."""

    def __init__(self, city_name: str, yw_api: YandexWeatherAPI):
        self.city_name = city_name
        self.yw_api = yw_api

    def run(self) -> YWResponse:
        # ??? logging.info('Начинаю сбор сырых данных из URL')
        try:
            raw_response = self.yw_api.get_forecasting(self.city_name)
            return YWResponse.parse_obj(raw_response)
        except ValidationError as e:
            logging.exception(f"Error during parsing response: {e.message}")
            raise e
        except Exception as e:  # Bad API: why raises common exception not customized?
            logging.exception(f"processed city: {self.city}. Got: {e.message}")
            raise e


class DataCalculationTask:
    """Выдает сренюю температуру и количество часов без осадков в городе за определенные даты.

    - период вычислений в течение дня — с 9 до 19 часов;
    - средняя температура рассчитывается за указанный промежуток времени;
    - сумма времени (часов), когда погода без осадков (без дождя, снега, града или грозы),
    рассчитывается за указанный промежуток времени;
    """

    def __init__(self, response: YWResponse):
        self.response = response

    def run(self) -> List[CityWeatherData]:
        result = []
        base_conditions = ("rain", "snow", "hail", "thunderstorm")
        for date_info in self.response.forecasts:
            temps = [h.temp for h in date_info.hours if 9 >= h.hour >= 19]
            data = CityWeatherData(
                city=self.response.geo_object.province.name,
                date=date_info.date,
                average_temperature=sum(temps) / len(temps),
                without_conditions_hours=len([h.condition for h in date_info.hours
                                              if 9 >= h.hour >= 19 and h.condition not in base_conditions])
            )
            result.append(data)
        return result


class DataAggregationTask:
    def __init__(self, data: List[CityWeatherData], file_format: FileFormat):
        self.data = data
        self.format = file_format

    def group_by_city(self) -> Dict[Tuple[str, str], Dict[str, float]]:
        grouped_data = dict()

        for item in self.data:
            if (item.city, "Температура, среднее") not in grouped_data:
                grouped_data[(item.city, "Температура, среднее")] = dict()
                grouped_data[(item.city, "Температура, среднее")]["sum"] = 0
                grouped_data[(item.city, "Температура, среднее")]["count"] = 0
                grouped_data[(item.city, "Без осадков, часов")] = dict()
                grouped_data[(item.city, "Без осадков, часов")]["sum"] = 0
                grouped_data[(item.city, "Без осадков, часов")]["count"] = 0

            grouped_data[(item.city, "Температура, среднее")][item.date] = item.average_temperature
            grouped_data[(item.city, "Температура, среднее")]["sum"] += item.average_temperature
            grouped_data[(item.city, "Температура, среднее")]["count"] += 1

            grouped_data[(item.city, "Без осадков, часов")][item.date] = item.without_conditions_hours
            grouped_data[(item.city, "Без осадков, часов")]["sum"] += item.without_conditions_hours
            grouped_data[(item.city, "Без осадков, часов")]["count"] += 1

        return grouped_data

    def group_for_output(self) -> Tuple[List[Dict], Tuple]:
        data = self.group_by_city()
        output_data = list()
        fieldnames = tuple()
        for item, value_dict in data.items():
            value_dict["Среднее"] = value_dict.pop("sum") / value_dict.pop("count")
            fieldnames = ("Город / день", "", *value_dict.keys())
            output_data.append(
                {
                    "Город / день": item[0],
                    "": item[1],
                    **value_dict
                }
            )
        return output_data, fieldnames

    def run(self):
        if self.format == "csv":
            with open("report.csv", "w") as f:
                data, fields = self.group_for_output()
                writer = csv.DictWriter(f, fields)
                writer.writeheader()
                writer.writerows(data)


class DataAnalyzingTask:
    def __init__(self):
        pass

    def run(self):
        pass
