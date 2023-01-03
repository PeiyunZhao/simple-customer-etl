import pandas as pd
import numpy as np

from customer_etl import util

def read_raw_csv() -> pd.DataFrame:

    df = pd.read_csv(r"/Users/thomaszhao/Dev/simple-customer-etl/src/raw.txt", delimiter='|')

    return df


def read_from_postgres() -> pd.DataFrame:

    df = util.execute_postgres_query_with_result(
        query="select * from public.customer"
    )

    return df
