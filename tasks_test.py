from api_client import YandexWeatherAPI
from tasks import DataFetchingTask


def test_data_fetching_task_success():
    task = DataFetchingTask(city_name="MOSCOW", yw_api=YandexWeatherAPI())
    response = task.run()
    assert response.geo_object.province.name == "Moscow"
    assert len(response.forecasts) == 5
    assert response.forecasts[0].date == "2022-05-26"
    assert len(response.forecasts[0].hours) == 24


def test_data_fetching_task_unknown_city():
    city_name = "Not MOSCOW"
    task = DataFetchingTask(city_name="Not MOSCOW", yw_api=YandexWeatherAPI())
    try:
        task.run()
    except Exception as e:
        assert str(e) == "Please check that city {} exists".format(city_name)
