import logging
from typing import List

from pydantic import ValidationError

from api_client import YandexWeatherAPI
from model import YWResponse, CityWeatherData

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s'
)


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
                is_with_conditions=len([h.condition for h in date_info.hours
                                        if 9 >= h.hour >= 19 and h.condition not in base_conditions])
            )
            result.append(data)
        return result


class DataAggregationTask:
    def __init__(self):
        pass

    def run(self):
        pass


class DataAnalyzingTask:
    def __init__(self):
        pass

    def run(self):
        pass
