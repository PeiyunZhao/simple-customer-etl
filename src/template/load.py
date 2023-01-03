import pandas as pd
from customer_etl import util
from loguru import logger as log


def load_data_into_mongo(df:pd.DataFrame,database:str,collection:str):

    util.upsert_all_records(
        db_name=database,
        collection=collection,
        id_array=df['_id'].to_list(),
        data_dict=df.to_dict('records')
    )

    return

def load_raw_data_into_postgres(raw_df:pd.DataFrame):

    query = _build_query_string(raw_df=raw_df)

    log.info('loading values into postgres')
    util.execute_query_without_result(query)
    log.success('values successfully loaded')

    return

def _build_query_string(raw_df:pd.DataFrame):
    raw_df.fillna('null',inplace=True)
    row_column = "'"+ raw_df['First Name']+"',"
    row_column += "'"+ raw_df['Last Name']+"',"
    row_column += raw_df['index'].astype(str)+","
    row_column += raw_df['Membership Duration'].astype(str)+","
    row_column += "'"+ raw_df['ID']+"',"
    row_column += "'"+ raw_df['Middle Initial']+"',"
    row_column += "'"+ raw_df['Birth Date']+"',"
    row_column += "'"+ raw_df['Gender']+"',"
    row_column += "'"+ raw_df['Email']+"',"
    row_column += "'"+ raw_df['Education Level']+"',"
    row_column += "'"+ raw_df['Profession']+"',"
    row_column += "'"+ raw_df['Address']+"',"
    row_column += "'"+ raw_df['Phone Number']+"'"

    tuple_string = '),\n\t\t('.join(row_column.to_list()).replace("'null'",'null')

    query = f"""
    INSERT INTO public.customer (first_name, last_name, index, membership_duration, member_id,
       middle_initial, birth_date, gender, email, education,
       profession, address, phone_number)
    VALUES
        ({tuple_string})
        
    ON CONFLICT ON CONSTRAINT customer_pk
    DO UPDATE SET 
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    index = EXCLUDED.index,
    membership_duration = EXCLUDED.membership_duration,
    middle_initial = EXCLUDED.middle_initial,
    birth_date = EXCLUDED.birth_date,
    gender = EXCLUDED.gender,
    email = EXCLUDED.email,
    education = EXCLUDED.education,
    profession = EXCLUDED.profession,
    address = EXCLUDED.address,
    phone_number = EXCLUDED.phone_number;
    """

    return query
