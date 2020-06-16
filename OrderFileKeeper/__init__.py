import logging
import azure.functions as func
import json, requests, os
from pydocumentdb import document_client

def main(event: func.EventHubEvent):

    cosmosdb_order_masterKey = os.environ.get('cosmosdb_order_masterKey')
    cosmosdb_order_host = os.environ.get('cosmosdb_order_host')
    cosmosdb_order_databaseId = os.environ.get('cosmosdb_order_databaseId')
    cosmosdb_order_collectionId = os.environ.get('cosmosdb_order_collectionId')    

    event_body = event.get_body().decode('utf-8')
    logging.info(event_body)

    combineOrderContent = os.environ.get('combineOrderContent')    

    response = requests.post(combineOrderContent, data = event_body)
    doc_json = response.json()

    client = document_client.DocumentClient(cosmosdb_order_host, {'masterKey': cosmosdb_order_masterKey})

    for sale in doc_json:

        dbLink = 'dbs/' + cosmosdb_order_databaseId
        collLink = dbLink + '/colls/' + cosmosdb_order_collectionId

        sale['salesNumber'] = sale['headers']['salesNumber']

        client.CreateDocument(collLink, sale)


