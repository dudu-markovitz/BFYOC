import logging
import azure.functions as func
import os, json, re
from azure.storage.blob import ContainerClient
from azure.eventhub import EventHubProducerClient, EventData

order_file_type = {
     'orderheaderdetails': 'orderHeaderDetailsCSVUrl'
    ,'orderlineitems': 'orderLineItemsCSVUrl'
    ,'productinformation': 'productInformationCSVUrl'
    }

def main(event: func.EventHubEvent):

    storage_orders = os.environ.get('storage_orders')
    storage_orders_container = os.environ.get('storage_orders_container')
    eventhub_ns_sap_sl = os.environ.get('eventhub_ns_sap_sl')
    eventhub_order_combine_files = os.environ.get('eventhub_order_combine_files')
    
    container = ContainerClient.from_connection_string(conn_str = storage_orders, container_name = storage_orders_container)

    event_body = event.get_body().decode('utf-8')
    
    logging.info(event_body)

    event_json = json.loads(event_body)

    for e in event_json:

        url = e["data"]["url"]
        url_dirname = os.path.dirname(url)
        url_basename = os.path.basename(url)
        order_id = re.findall('\d+', url_basename)[0]

        blobs = list(container.list_blobs(name_starts_with = order_id))
        
        blob_ts = {b['name'] : (b['last_modified'], b['etag'])  for b in blobs}
        blob_ts_max = max(blob_ts.values())
        
        doc = dict()

        if len(blobs) == 3 and blob_ts[url_basename] == blob_ts_max:
            for b in blobs:
                blob_name = b['name']
                file_type = order_file_type.get(re.findall('(?<=-)\w+(?=\.)', blob_name)[0].lower())

                doc[file_type] = f'{url_dirname}/{blob_name}'
            
            doc_json = json.dumps(doc)
            
            producer = EventHubProducerClient.from_connection_string(conn_str = eventhub_ns_sap_sl, eventhub_name = eventhub_order_combine_files)

            try: 
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData(doc_json))         
                producer.send_batch(event_data_batch)
            finally:
                producer.close()

