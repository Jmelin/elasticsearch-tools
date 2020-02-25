#!/usr/bin/python

import argparse
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, date
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-a','--action', choices=['health', 'allocation', 'nodes', 'shards', 'quickstatus'], help='Get the health of the Elasticsearch Cluster', required=True)
parser.add_argument('-f','--filter', help='Filter search down. Use with shards')
args = parser.parse_args()

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

elasticsearch_url = '10.96.1.91'

def get_health():
    health_url = 'https://' + elasticsearch_url + ':9200/_cat/health?format=json'
    r = requests.get(health_url, auth=('admin', 'admin'), verify=False)
    data = json.dumps(r.json(), indent=4)
    print data

def get_allocation():
    allocation_url = 'https://' + elasticsearch_url + ':9200/_cat/allocation?v'
    r = requests.get(allocation_url, auth=('admin', 'admin'), verify=False)
    data = r.text
    print data

def get_nodes():
    nodes_url = 'https://' + elasticsearch_url + ':9200/_cat/nodes?v'
    r = requests.get(nodes_url, auth=('admin', 'admin'), verify=False)
    data = r.text
    print data

def list_shards(argfilter):
    if argfilter:
        shards_url = 'https://' + elasticsearch_url + ':9200/_cat/shards/'+ argfilter +''
    elif argfilter == "all":
        shards_url = 'https://' + elasticsearch_url + ':9200/_cat/shards'
    r = requests.get(shards_url, auth=('admin', 'admin'), verify=False)
    data = r.text
    print data

if args.action == "health":
    get_health()

if args.action == "allocation":
    get_allocation()

if args.action == "nodes":
    get_nodes()

if args.action == "shards":
    if args.filter:
        list_shards(args.filter)
    else:
        list_shards('*')

if args.action == "quickstatus":
    get_health()
    get_allocation()
    get_nodes()
