#!/usr/bin/python

## TODO : Allow the ability to exclude indices from being rotated or delete

import json
import requests
from datetime import datetime, date
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

elasticsearch_url = '10.96.1.91:9202'
days_on_hot = 7 #7 days
days_on_warm = days_on_hot + 13 #20 days
days_on_cold = days_on_warm + 12 #32 days

#days_on_hot = 3 #3 days
#days_on_warm = days_on_hot + 2 #5 days
#days_on_cold = days_on_warm + 2 #7 days

indexfilter = '*'

def get_all_indexes(index_filter):
    indices_url = 'https://' + elasticsearch_url + '/_cat/indices/'+ index_filter +'?h=index&format=json'
    r = requests.get(indices_url, auth=('admin', 'admin'), verify=False)
    data = r.json()
    return data

def add_hot_requirement(index):
    indices_settings_url = 'https://' + elasticsearch_url + '/' + index + '/_settings'
    data = '{"index.routing.allocation.require.box_type": "hot"}'
    r = requests.put(indices_settings_url, headers=headers, data=data, auth=('admin', 'admin'), verify=False)
    if r.status_code is 200:
        print('{} - {} is moving to hot nodes').format(r.text, index)
    else:
        print('{} - {}').format(r.json, r.text)

def rotate_indexes(indexes):
    today = date.today()
    currentday = today.strftime('%Y.%m.%d')
    dateformat = '%Y.%m.%d'
    index_node_requirement = ''

    for i in indexes:
        index_name = i['index']

        if index_name == '.kibana':
            print('This is {}. Should Skip.').format(index_name)
            add_hot_requirement(index_name)
            continue

        index_endpoint = 'https://' + elasticsearch_url + '/' + index_name + '?include_type_name=false'
        r = requests.get(index_endpoint, auth=('admin', 'admin'), verify=False)
        data = r.json()
        try:
            index_node_requirement = data[index_name]['settings']['index']['routing']['allocation']['require']['box_type']
        except KeyError:
            add_hot_requirement(index_name)
        index_creation_date = data[index_name]['settings']['index']['creation_date']
        parsed_creation_date = datetime.fromtimestamp(int(index_creation_date)/1000).strftime('%Y.%m.%d')

        a = datetime.strptime(parsed_creation_date,dateformat)
        b = datetime.strptime(currentday,dateformat)
        days_between = b - a

        try:
            if index_node_requirement == 'hot':
                if days_between.days >= days_on_hot:
                    indices_settings_url = 'https://' + elasticsearch_url + '/' + index_name + '/_settings'
                    data = '{"index.routing.allocation.require.box_type": "warm"}'
                    r = requests.put(indices_settings_url, headers=headers, data=data, auth=('admin', 'admin'), verify=False)
                    if r.status_code is 200:
                        print('{} - {} is moving to warm nodes').format(r.text, index_name)
                    else:
                        print('{} - {}').format(r.json, r.text)
            elif index_node_requirement == 'warm':
                if days_between.days >= days_on_warm:
                    indices_settings_url = 'https://' + elasticsearch_url + '/' + index_name + '/_settings'
                    data = '{"index.routing.allocation.require.box_type": "cold"}'
                    r = requests.put(indices_settings_url, headers=headers, data=data, auth=('admin', 'admin'), verify=False)

                    ## TODO: Long term storage here
                    # need to shrink and or merge indexes

                    if r.status_code is 200:
                        print('{} - {} is moving to cold nodes').format(r.text, index_name)
                    else:
                        print('{} - {}').format(r.json, r.text)
            elif index_node_requirement == 'cold':
                if days_between.days >= days_on_cold:
                    indice_delete_url = 'https://' + elasticsearch_url + '/' + index_name + ''
                    r = requests.delete(indice_delete_url,headers=headers, data=data, auth=('admin', 'admin'), verify=False)

                    if r.status_code is 200:
                        print('{} - {} is being deleted').format(r.text, index_name)
                    else:
                        print('{} - {}').format(r.json, r.text)
        except KeyError:
            print('issue')

all_indexes = get_all_indexes(indexfilter)
rotate_indexes(all_indexes)
