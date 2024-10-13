import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv() 

def load_data_from_postgres(query):
    try:
        conn=psycopg2.connect(host=os.getenv('DB_HOST'), dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), port=os.getenv("DB_PORT"))
        
        df=pd.read_sql_query(query, conn)
        pd.set_option('display.float_format', '{:.0f}'.format)

        conn.close()
        
        return df
    except Exception as e:
        print("error connecting", e)
        
def nan_values(df):
    mis_value=df.isna().sum()
    mis_value_percent= to_percent(df,mis_value)
        
    mis_value_dtype=df.dtypes

    mis_value_table=pd.concat([mis_value,mis_value_percent,mis_value_dtype],axis=1)
    mis_value_table=mis_value_table.rename(columns={0:"missing values",1:"missing values %",2:"missing values dtype"})
    mis_value_table=mis_value_table.sort_values('missing values %',ascending=False)
    return mis_value_table

def to_percent(df,data):
    perc=100*data/len(df)
    return perc
    
    