import pandas as pd
import numpy as np

def read_raw_csv() -> pd.DataFrame:

    df = pd.read_csv(r"/Users/thomaszhao/Dev/simple-customer-etl/src/raw.txt", delimiter='|')

    return df