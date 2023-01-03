from template import extract
from template import load
from template import transform
from loguru import logger as log


def load_values_from_raw_txt():

    df = extract.read_raw_csv()

    load.load_raw_data_into_postgres(raw_df=df)

    return


def postgres_to_mongo_etl():

    df = extract.read_from_postgres()

    df = transform.transform_postgres_data(df)

    load.load_data_into_mongo(df=df,database='publicDB',collection='customers')

    return

if __name__ == '__main__':

    postgres_to_mongo_etl()