import logging
import azure.functions as func

from pydocumentdb import document_client, documents
import json, os

def get_rating_db_client():
    masterKey = os.environ.get('cosmos_db_rating_master_key')
    host = os.environ.get('cosmos_db_rating_host')

    return document_client.DocumentClient(host, {'masterKey': masterKey})


def get_rating_db_collLink():
    databaseId = os.environ.get('cosmos_db_rating_databaseId')
    collectionId = os.environ.get('cosmos_db_rating_collectionId')
    return f'dbs/{databaseId}/colls/{collectionId}'



def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('OH Serverless GetRating')

    
    ratingId = req.params.get('ratingId')
    if ratingId is None:
        return func.HttpResponse('must supply ratingId', status_code = 400)

    collLink = get_rating_db_collLink()
    client = get_rating_db_client()

    querystr = '''
    select  c.userId, c.productId, c.locationName, c.rating, c.userNotes, c.id, c.timestamp 
    from    c
    where   c.id = @ratingId
    '''

    parameters = [{"name": "@ratingId", "value": ratingId}]
    query = {"query":querystr,"parameters":parameters}

    query_results = client.QueryDocuments(collLink, query, options = {'enableCrossPartitionQuery': True}, partition_key = 'userId')

    try:
        results = list(query_results)        
        if len(results) == 0:
            return func.HttpResponse('no rating found', status_code = 404)        
        return func.HttpResponse(json.dumps(results[0]))
    except:
        return func.HttpResponse('cannot connect to rating db', status_code = 400)
