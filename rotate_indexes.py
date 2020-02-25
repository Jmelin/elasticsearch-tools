#!/usr/bin/python

import json
import requests
from datetime import datetime, date
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

elasticsearch_url = '10.96.1.91'
days_on_hot = 7
days_on_warm = days_on_hot + 10000
days_on_cold = days_on_warm + 10000

def get_all_indexes():
    indices_url = 'https://' + elasticsearch_url + ':9200/_cat/indices/fortisiem-event*?h=index&format=json'
    r = requests.get(indices_url, auth=('admin', 'admin'), verify=False)
    data = r.json()
    return data

# def set_lifecycle_policies(indexes):
#      for i in indexes:
#         index_name = i['index']
#         index_endpoint = 'https://' + elasticsearch_url + ':9200/' + index_name + '?include_type_name=false'
#         r = requests.get(index_endpoint, auth=('admin', 'admin'), verify=False)
#         data = r.json()

def rotate_indexes(indexes):
    today = date.today()
    currentday = today.strftime('%Y.%m.%d')
    dateformat = '%Y.%m.%d'

    for i in indexes:
        index_name = i['index']

        index_endpoint = 'https://' + elasticsearch_url + ':9200/' + index_name + '?include_type_name=false'
        r = requests.get(index_endpoint, auth=('admin', 'admin'), verify=False)
        data = r.json()
        index_node_requirement = data[index_name]['settings']['index']['routing']['allocation']['require']['box_type']
        index_creation_date = data[index_name]['settings']['index']['creation_date']
        parsed_creation_date = datetime.fromtimestamp(int(index_creation_date)/1000).strftime('%Y.%m.%d')

        a = datetime.strptime(parsed_creation_date,dateformat)
        b = datetime.strptime(currentday,dateformat)
        days_between = b - a

        try:
            if index_node_requirement == 'hot':
                if days_between.days >= days_on_hot:
                    indices_settings_url = 'https://' + elasticsearch_url + ':9200/' + index_name + '/_settings'
                    data = '{"index.routing.allocation.require.box_type": "warm"}'
                    r = requests.put(indices_settings_url, headers=headers, data=data, auth=('admin', 'admin'), verify=False)
                    if r.status_code is 200:
                        print('{} - {} is moving to warm nodes').format(r.text, index_name)
                    else:
                        print('{} - {}').format(r.json, r.text)
            elif index_node_requirement == 'warm':
                if days_between.days >= days_on_warm:
                    indices_settings_url = 'https://' + elasticsearch_url + ':9200/' + index_name + '/_settings'
                    data = '{"index.routing.allocation.require.box_type": "cold"}'
                    r = requests.put(indices_settings_url, headers=headers, data=data, auth=('admin', 'admin'), verify=False)

                    ## TODO: Long term storage here
                    # need to shrink and or merge indexes

                    if r.status_code is 200:
                        print('{} - {} is moving to cold nodes').format(r.text, index_name)
                    else:
                        print('{} - {}').format(r.json, r.text)
            elif index_node_requirement == 'cold':
                ## TODO: Delete indexes here
                pass
        except KeyError:
            print('issue')

all_indexes = get_all_indexes()
rotate_indexes(all_indexes)