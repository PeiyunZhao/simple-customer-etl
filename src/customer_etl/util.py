import pandas as pd
import numpy as np
from loguru import logger as log
import sqlalchemy
import pymongo

local_mongo_string = "mongodb://localhost:27017/"
postgres_connection_string = 'postgresql://localhost:5432/postgres'

def _get_postgres_connector():
    '''
    fetches Postgres connector
    :return: connector
    '''
    engine = sqlalchemy.create_engine(postgres_connection_string)
    connector = engine.connect()

    return connector

def execute_postgres_query_with_result(query: str) -> pd.DataFrame:
    """
    Executes retrieval query and returns dataframe of results
    :param query: SQL retrieval query string
    :return: a dataframe constructed from query results by pandas
    """
    connector = _get_postgres_connector()
    df = pd.read_sql_query(query, connector)

    connector.close()

    return df


def execute_query_without_result(query: str):
    """
    query used to execute SQL update query with no return
    :param query: SQL update query string
    :return:
    """
    connector = _get_postgres_connector()
    connector.execute(query)
    connector.close()

    return


def upsert_all_records(db_name: str, collection: str, id_array: list, data_dict: list, batch_size: int = 100000):
    '''
    Upersert values into mongodb
    :param db_name: name of database
    :param collection: name of collection
    :param data_dict: dataframe.to_records()
    :param batch_size: default 100,000
    :return: None
    '''
    if data_dict:
        client = pymongo.MongoClient(local_mongo_string)
        db = client[db_name]
        batch_size = batch_size  # about 600000 uwi will hit BSON file size limit 16MB
        id_array_list = np.array_split(id_array, len(id_array)//batch_size + 1)
        for sub_array in id_array_list:
            delete_by_id(db_name = db_name, collection=collection, id_list=sub_array.tolist())
        response = db[collection].insert_many(data_dict)
        insert_count = len(response.inserted_ids)
        log.info(f'Inserted count: Mongo.{db_name}DB.{collection} - {insert_count}')
        client.close()
    else:
        log.info(f'No data to insert: Mongo.{db_name}DB.{collection}')

    return

def delete_by_id(db_name: str, collection: str, id_list):
    '''
    deletes database data base on id
    :param db_name: name of database
    :param collection: name of collection
    :param id_list: list of _id
    :return: None
    '''
    client = pymongo.MongoClient(local_mongo_string)
    db = client[db_name]
    response = db[collection].delete_many({"_id": {"$in": id_list}})
    delete_count = response.deleted_count
    log.info(f'Deleted count: Mongo.{db_name}DB.{collection} - {delete_count}')
    client.close()

    return


def retrieve_from_mongo(db_name:str,collection:str)-> pd.DataFrame:
    '''
    retrieves entire collection from mongo
    :param db_name: name of database
    :param collection: name of collection
    :return: Dataframe of data
    '''
    client = pymongo.MongoClient(local_mongo_string)
    db = client[db_name]
    cur = db[collection].find()
    df = pd.DataFrame(list(cur))

    return df