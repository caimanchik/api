import csv
import datetime
import json
from typing import Generator, List

import requests as requests


class ApiHH:

    @staticmethod
    def create_csv():
        """
        Метод для создания csv файла с вакансиями за прошедший день
        :return:
        """
        with open('vacancies.csv', 'w', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            for row in ApiHH.__get_parsed_rows():
                writer.writerow(row)

    @staticmethod
    def __get_parsed_rows() -> Generator[List[str]]:
        """
        Возвращает строки для csv файла
        :return: Строки для csv файла
        """
        delta = datetime.timedelta(1)
        end = datetime.datetime.now()
        start = end - delta

        hours = [str(x) if x >= 10 else f'0{x}' for x in range(24)] + ['00']
        days = [start.day if start.day >= 10 else f'0{start.day}'] * 24 + [end.day if end.day >= 10 else f'0{end.day}']

        for i in range(len(hours) - 1):
            for row in ApiHH.__get__rows_for_day(hours[i], hours[i + 1], days[i], days[i + 1]):
                yield row

    @staticmethod
    def __get__rows_for_day(hour_from: str, hour_to: str, day1: str, day2: str) -> Generator[List[str]]:
        """
        Возвращает строки csv файла для переданного диапазона
        :param hour_from: Начальгный час
        :param hour_to: Конечный час
        :param day1: Начальный день
        :param day2: Конечный день
        :return: Строки для csv файла
        """
        now_page = ApiHH.__get_vacancies_per_page(0, hour_from, hour_to, day1, day2)
        js_obj = json.loads(now_page)

        count = int(js_obj['pages'])

        for row in ApiHH.__get_rows_from_json(js_obj):
            yield row

        for i in range(1, count):
            now_page = ApiHH.__get_vacancies_per_page(i, hour_from, hour_to, day1, day2)
            js_obj = json.loads(now_page)

            for row in ApiHH.__get_rows_from_json(js_obj):
                yield row

    @staticmethod
    def __get_vacancies_per_page(page: int, hour_from: str, hour_to: str, day1: str, day2: str):
        """
        Возваращает тело запроса с сайта HH
        :param page: Страница
        :param hour_from: Начальгный час
        :param hour_to: Конечный час
        :param day1: Начальный день
        :param day2: Конечный день
        :return: Тело запроса
        """
        url = "https://api.hh.ru/vacancies"

        params = {
            'page': page,
            'per_page': 100,
            'specialization': 1,
            'date_from': f'2022-12-{day1}T{hour_from}:00:00',
            'date_to': f'2022-12-{day2}T{hour_to}:00:00',
        }

        req = requests.get(url, params)
        data = req.content.decode()
        req.close()

        return data

    @staticmethod
    def __get_rows_from_json(js_obj) -> Generator[List[str]]:
        """
        Парсит полученный запросом json
        :param js_obj: Объект вакансии в формате json
        :return: Строка для csv файла
        """
        for item in js_obj['items']:
            salary = ApiHH.__get_salary(item['salary'])
            yield [item['name']] + salary + [item['area']['name'], item['published_at']]

    @staticmethod
    def __get_salary(salary) -> List[str]:
        """
        Возвращает зарплату для csv файла
        :param salary: Объект salary из запроса
        :return: Массив с полями зарплаты
        """
        if salary is None:
            return [''] * 3

        return [
            salary['from'] if salary['from'] is not None else '',
            salary['to'] if salary['to'] is not None else '',
            salary['currency'] if salary['currency'] is not None else '',
        ]


def main():
    """
    Метод для запуска программы
    :return:
    """
    ApiHH.create_csv()


if __name__ == '__main__':
    """
    Основная идиома питона
    """
    main()
