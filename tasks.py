import logging

from pydantic import ValidationError

from api_client import YandexWeatherAPI
from model import YWResponse

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s'
)


class DataFetchingTask:
    def __init__(self, city_name: str, yw_api: YandexWeatherAPI):
        self.city_name = city_name
        self.yw_api = yw_api

    def run(self) -> YWResponse:
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
    def __init__(self, response: YWResponse):
        pass

    def run(self):
        pass


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
