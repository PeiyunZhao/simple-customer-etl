import pandas as pd
import numpy as np



def transform_postgres_data(df) -> pd.DataFrame:

    df.rename(columns={'member_id':'_id'},inplace=True)

    df['name'] = df.apply(
        lambda r: {'first_name':r['first_name'],'middle':r['middle_initial'],'last_name':r['last_name']},
        axis=1
    )

    df['professional_info'] = df.apply(
        lambda r: {'education':r['education'],'profession':r['profession']},
        axis=1
    )

    df['member_info'] = df.apply(
        lambda r: {'member_number':r['index'],'membership_duration':r['membership_duration'],'address':r['address']},
        axis=1
    )

    df['contact_info'] = df.apply(
        lambda r: {'email':r['email'],'phone_number':r['phone_number']},
        axis=1
    )

    return df[['_id','name','professional_info','member_info','contact_info']]