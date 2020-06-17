import logging
import azure.functions as func
import json, requests, os
from pydocumentdb import document_client
from azure.servicebus import TopicClient, Message

def main(event: func.EventHubEvent):

    cosmosdb_pos_masterKey = os.environ.get('cosmosdb_pos_masterKey')
    cosmosdb_pos_host = os.environ.get('cosmosdb_pos_host')
    cosmosdb_pos_databaseId = os.environ.get('cosmosdb_pos_databaseId')
    cosmosdb_pos_collectionId = os.environ.get('cosmosdb_pos_collectionId')    

    servicebus_pos_conn_str = os.environ.get('servicebus_pos_conn_str')
    servicebus_pos_topic_name = os.environ.get('servicebus_pos_topic_name')


    dbLink = 'dbs/' + cosmosdb_pos_databaseId
    collLink = dbLink + '/colls/' + cosmosdb_pos_collectionId

    client = document_client.DocumentClient(cosmosdb_pos_host, {'masterKey': cosmosdb_pos_masterKey})

    event_str = event.get_body().decode('utf-8')
    logging.info(event_str)      

    event_json = json.loads(event_str)

    for e in event_json:
        e['id'] = e['header']['salesNumber']        
        client.CreateDocument(collLink, e)

        totalCost = float(e["header"]["totalCost"]) 

        receipt = {
            "totalItems": len(e["details"]),
            "totalCost": totalCost,
            "salesNumber": e["header"]["salesNumber"],
            "salesDate": e["header"]["dateTime"],
            "storeLocation": e["header"]["locationId"],
            "receiptUrl": e["header"]["receiptUrl"]
        }


        tc = TopicClient.from_connection_string(servicebus_pos_conn_str, servicebus_pos_topic_name)
        msg = Message(json.dumps(receipt).encode('utf8'), ContentType='application/json;charset=utf-8')
        msg.user_properties = {'totalCost': totalCost}
        tc.send(msg)

