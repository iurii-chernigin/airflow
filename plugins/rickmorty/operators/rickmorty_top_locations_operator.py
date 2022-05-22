"""
    Here will be custom RickMortyTopLocationsOperator that will be inherited from BaserOperator
    The operator will be counting TOP N locations base on API: https://rickandmortyapi.com/documentation/#location
    Method: get_resident_count_on_page()

"""
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

import requests
import logging

class RickMortyTopLocationsOperator(BaseOperator):
    
    template_fields = ('top_count',)
    ui_color = "#e0ffff"

    def __init__(self, top_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_count = top_count

    def get_pages_count(self, api_url: str) -> int:
        """
        Get count of pages in API
        :param api_url
        :return page_count
        """
        response = requests.get(api_url)
        if response.status_code == 200:
            logging.info(f'The request to {api_url} was successful')
            page_count = response.json().get('info').get('pages')
            logging.info(f'Number of pages is {page_count}')
            return page_count
        else:
            raise AirflowException(f'Failure attempt to make a get request to API URL {api_url}')


    def get_locations_counters(self, results_json: list) -> list:
        pass

    def get_results_by_page(self, page) -> list:
        pass

    def execute():
        pass
