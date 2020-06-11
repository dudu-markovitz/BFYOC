import logging
import azure.functions as func
import uuid, datetime, json, requests

from pydocumentdb import document_client, documents
import os
# comment
def get_rating_db_client():
    masterKey = os.environ.get('cosmos_db_rating_master_key')
    host = os.environ.get('cosmos_db_rating_host')
    return document_client.DocumentClient(host, {'masterKey': masterKey})

def get_rating_db_collLink():
    databaseId = os.environ.get('cosmos_db_rating_databaseId')
    collectionId = os.environ.get('cosmos_db_rating_collectionId')
    return f'dbs/{databaseId}/colls/{collectionId}'

def validate_productId(productId):
    url_GetProduct = os.environ.get('url_GetProduct')
    response = requests.get(url_GetProduct, params = {'productId': productId})
    return (response.status_code, response.text)
    
def validate_userId(userId):
    url_GetUser = os.environ.get('url_GetUser')
    response = requests.get(url_GetUser, params = {'userId': userId})
    return (response.status_code, response.text)

def validate_rating(rating):
    is_valid = isinstance(rating, int) and rating >= 0 and rating <= 5
    return (is_valid, '' if is_valid else 'validate rating: rating must be an integer between 0 to 5') 

def get_id():
    return str(uuid.uuid4())

def get_timestamp():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat(' ')


def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('OH Serverless CreateRating')

    req_body = req.get_json()
    userId = req_body.get('userId')
    productId = req_body.get('productId')
    rating = req_body.get('rating')

    if None in (userId, productId, rating):
        return func.HttpResponse('must supply userId, productId & rating', status_code = 400)

    status_code, text = validate_productId(productId)
    if status_code != 200:
        return func.HttpResponse(f'validate productId: {text}', status_code = status_code)

    status_code, text = validate_userId(userId)
    if status_code != 200:
        return func.HttpResponse(f'validate userId: {text}', status_code = status_code)

    is_valid, text = validate_rating(rating)
    if not is_valid:
        return func.HttpResponse(text ,status_code = 400)

    req_body['id'] = get_id()
    req_body['timestamp'] = get_timestamp()

    collLink = get_rating_db_collLink()
    client = get_rating_db_client()

    try:
        client.CreateDocument(collLink, req_body)
        return func.HttpResponse(json.dumps(req_body))
    except:
        return func.HttpResponse('cannot connect to rating db', status_code = 400)

 
