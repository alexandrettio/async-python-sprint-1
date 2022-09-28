def check_python_version():
    from utils import check_python_version

    check_python_version()


def check_api():
    from api_client import YandexWeatherAPI

    CITY_NAME_FOR_TEST = "MOSCOW"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)
    attr = resp.get("info")
    print(attr)


if __name__ == "__main__":
    check_python_version()
    check_api()
