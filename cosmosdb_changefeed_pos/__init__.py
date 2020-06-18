import logging
import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
import os, json

def main(documents: func.DocumentList) -> str:

    eventhub_ns_sap_sl = os.environ.get('eventhub_ns_sap_sl')
    eventhub_cosmosdb_change_feed = os.environ.get('eventhub_cosmosdb_change_feed')

    producer = EventHubProducerClient.from_connection_string(conn_str = eventhub_ns_sap_sl, eventhub_name = eventhub_cosmosdb_change_feed)
    event_data_batch = producer.create_batch()

    for doc in documents:
        doc["source_collection"] = "pos"
        event_data_batch.add(EventData(json.dumps(doc)))     

    try: 
        producer.send_batch(event_data_batch)
    finally:
        producer.close()
