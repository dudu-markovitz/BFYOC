import logging
import azure.functions as func
import json, requests, os
from pydocumentdb import document_client


def main(event: func.EventHubEvent):

    cosmosdb_pos_masterKey = os.environ.get('cosmosdb_pos_masterKey')
    cosmosdb_pos_host = os.environ.get('cosmosdb_pos_host')
    cosmosdb_pos_databaseId = os.environ.get('cosmosdb_pos_databaseId')
    cosmosdb_pos_collectionId = os.environ.get('cosmosdb_pos_collectionId')    

    dbLink = 'dbs/' + cosmosdb_pos_databaseId
    collLink = dbLink + '/colls/' + cosmosdb_pos_collectionId

    client = document_client.DocumentClient(cosmosdb_pos_host, {'masterKey': cosmosdb_pos_masterKey})

    for e in event:

        event_str = e.get_body().decode('utf-8')
        logging.info(event_str)      

        event_json = json.loads(event_str)

        event_json['id'] = event_json['header']['salesNumber']

        client.CreateDocument(collLink, event_json)
