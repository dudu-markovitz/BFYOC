import logging
import azure.functions as func
import os, json
from azure.storage import blob

def main(message: func.ServiceBusMessage):

    message_content_type = message.content_type
    message_body = message.get_body().decode("utf-8")

    logging_info = {
        "Message Content Type" : message_content_type,
        "Message Body" : message_body
        }

    logging.info(json.dumps(logging_info))

    storage_receipt_conn_str = os.environ.get('storage_receipt_conn_str')
    storage_receipt_lt100_container = os.environ.get('storage_receipt_lt100_container')


    receipt_json = json.loads(message_body)

    receipt_lt100_json = {
        "Store": receipt["storeLocation"],
        "SalesNumber": receipt["salesNumber"],
        "TotalCost": receipt["totalCost"],
        "Items": receipt["totalItems"],
        "SalesDate": receipt["salesDate"]
    }


    blob_name = receipt_json["salesNumber"] + '.json'

    blob_service = blob.BlobClient.from_connection_string(storage_receipt_conn_str, storage_receipt_lt100_container, blob_name)
    blob_service.upload_blob(json.dumps(receipt_lt100_json))
