# -*- coding: utf-8 -*-
import configparser
import os
import json
import subprocess


def exec(url, method, api_key):
    cmd = 'curl -s -X %s -u "apikey":%s %s' % (method, api_key, url)
    print(cmd)
    proc_stdout = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read().decode("utf-8")
    # print(proc_stdout)
    return json.loads(proc_stdout)


def get_collection_details(url, environment_id, collection_id, version, api_key):
    collection_url = url % (environment_id, collection_id, "", version)
    return exec("\"" + collection_url + "\"", "GET", api_key)


def get_document_list(url, environment_id, collection_id, version, api_key, count):
    query_url = url % (environment_id, collection_id, "/query", version)
    query_str = "&return=id&count=%s" % count
    return exec("\"" + query_url + query_str + "\"", "GET", api_key)


def write_file(path, document_list):
    with open(path, mode='w') as f:
        for document in document_list["results"]:
            document_id = document["id"]
            f.write(document_id + '\n')


def delete_document(url, environment_id, collection_id, version, api_key, document_id):
    document_url = url % (environment_id, collection_id, "/documents/" + document_id, version)
    ret = exec("\"" + document_url + "\"", "DELETE", api_key)
    print(ret)


def add_documents(path, url, environment_id, collection_id, version, api_key):
    files = os.listdir(path)

    add_documents_url = url % (environment_id, collection_id, "/documents", version)
    option_file = " -F file=@%s"

    for file in files:
        ret = exec("\"" + add_documents_url + "\"" + option_file % (path + file), "POST", api_key)


config = configparser.ConfigParser()
config.read('watson.ini')

url = config.get('discovery', 'url')
api_key = config.get('discovery', 'api_key')
environment_id = config.get('discovery', 'environment_id')
collection_id = config.get('discovery', 'collection_id')
version = config.get('discovery', 'version')

url += "environments/%s/collections/%s%s?version=%s"

path = "c:/dev/watson_doc/"

collection_details = get_collection_details(url, environment_id, collection_id, version, api_key)

document_count_available = collection_details["document_counts"]["available"]

print("document count: available(%s), processing(%s), failed(%s)" % (
    collection_details["document_counts"]["available"], collection_details["document_counts"]["processing"],
    collection_details["document_counts"]["failed"]))

document_list = get_document_list(url, environment_id, collection_id, version, api_key, document_count_available)

write_file("document_id.txt", document_list)

# for document in document_list["results"]:
#     document_id = document["id"]
#     ret = delete_document(url, environment_id, collection_id, version, api_key, document_id)
#
# add_documents(path, url, environment_id, collection_id, version, api_key)
