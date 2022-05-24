"""
    Here will be custom RickMortyTopLocationsOperator that will be inherited from BaserOperator
    The operator will be counting TOP N locations base on API: https://rickandmortyapi.com/documentation/#location
    Method: get_resident_count_on_page()

"""
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

import requests
import logging
import csv

class RickMortyTopLocationsOperator(BaseOperator):
    
    template_fields = ('top_count', 'result_path',)
    ui_color = "#e0ffff"
    api_base_url = 'https://rickandmortyapi.com/api/location'

    def __init__(self, top_count: int = 3, result_csv_path: str = '/tmp/rickmorty_top_locations.csv', **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_count = top_count
        self.result_csv_path = result_csv_path


    def run_request(self, api_url: str) -> dict:
        """
        Get count of pages in API
        :param api_url
        :return page_count
        """
        response = requests.get(api_url)
        if response.status_code == 200:
            logging.info(f'The request to {api_url} is successful')
            response_json = response.json()
            return {
                'locations': response_json.get('results'), 
                'next': response_json.get('info').get('next') 
            }
        else:
            raise AirflowException(f'Failure attempt to make a get request to API URL {self.api_base_url}')


    def execute(self):
        
        location_counters = []
        request_result = None
        result_csv_path = self.result_csv_path

        while True: 
            if request_result is None:
                request_result = self.run_request(self.api_base_url)
            else: 
                request_result = self.run_request(request_result['next'])
            for location in request_result['locations']:
                location_counters.append({
                    'id': location.get('id'),
                    'residents': len(location.get('residents'))
                })
            if request_result['next'] is None:
                break

        location_counters = sorted(location_counters, key=lambda location: location['residents'], reverse=True)
        location_counters = location_counters[0:self.top_count]
        logging.info(f'TOP-{self.top_count} locations by residents: {location_counters}')

        with open(result_csv_path, 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'residents'])
            for location in location_counters:
                writer.writerow([location['id'], location['residents']])







            
            


