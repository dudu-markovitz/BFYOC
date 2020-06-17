import logging
import azure.functions as func
import os, json
from azure.storage import blob
import requests, base64

def main(message: func.ServiceBusMessage):

    message_content_type = message.content_type
    message_body = message.get_body().decode("utf-8")

    logging_info = {
        "Message Content Type" : message_content_type,
        "Message Body" : message_body
        }

    logging.info(json.dumps(logging_info))

    storage_receipt_conn_str = os.environ.get('storage_receipt_conn_str')
    storage_receipt_ge100_container = os.environ.get('storage_receipt_ge100_container')


    receipt_json = json.loads(message_body)
    
    receiptUrl = receipt_json["receiptUrl"]

    ReceiptImage = None
    
    if receiptUrl is not None:
        response = requests.get(receiptUrl)
        ReceiptImage = base64.b64encode(response.content).decode('utf-8')

    receipt_ge100_json = {
        "Store": receipt_json["storeLocation"],
        "SalesNumber": receipt_json["salesNumber"],
        "TotalCost": receipt_json["totalCost"],
        "Items": receipt_json["totalItems"],
        "SalesDate": receipt_json["salesDate"],
        "ReceiptImage": ReceiptImage
    }


    blob_name = receipt_json["salesNumber"] + '.json'

    blob_service = blob.BlobClient.from_connection_string(storage_receipt_conn_str, storage_receipt_ge100_container, blob_name)
    blob_service.upload_blob(json.dumps(receipt_ge100_json), overwrite = True)
