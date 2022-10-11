from multiprocessing import Queue

from tasks import DataFetchingTask, DataCalculationTask


def test_data_fetching_task_success():
    task = DataFetchingTask(city_name="MOSCOW")
    response = task.run()
    assert response.geo_object.province.name == "Moscow"
    assert len(response.forecasts) == 5
    assert response.forecasts[0].date == "2022-05-26"
    assert len(response.forecasts[0].hours) == 24


def test_data_fetching_task_unknown_city():
    city_name = "Not MOSCOW"
    task = DataFetchingTask(city_name="Not MOSCOW")
    try:
        task.run()
    except Exception as e:
        assert str(e) == "Please check that city {} exists".format(city_name)


def test_data_calculation_task_success():
    fetch_response = DataFetchingTask(city_name="MOSCOW").run()
    queue = Queue()
    task = DataCalculationTask(fetch_response, queue)
    task.run()
    assert not queue.empty()
    item = queue.get()
    assert item.city == "Moscow"
    assert item.date == "2022-05-26"
    assert round(item.average_temperature, 1) == 17.7
    assert item.without_conditions_hours == 11
