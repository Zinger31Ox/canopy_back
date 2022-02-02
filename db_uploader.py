# A lambda function to interact with AWS RDS MySQL
# https://towardsdatascience.com/how-to-read-excel-csv-file-in-bubble-io-using-aws-s3-lambda-a9af89153d2a thanks Jodie Zhou

# import boto3
import logging
# import os
import sys
# import uuid
import mysql.connector
# import pymysql
# import csv
import rds_config
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine

from fin_calcs import xirr_calc,mult_calc, coc_calc_quarterly

rds_host = rds_config.rds_host
name = rds_config.name
password = rds_config.password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

col_csv = ["date","Actual-Forecast", "GRI", "EBITDA", "Levered CF"]
col_sql = ["date", "GRI", "EBITDA"]

def db_fincalc(dataframe, typef, fin_column_name, timestamp):
    # will return an output DF with fin metrics with the following columms ["IRR", "Mult","Yield", "type", "fin_column_name", "Timestamp"] 
    
    data = [{
            'IRR': xirr_calc(dataframe, fin_column_name), 
            'Mult': mult_calc(dataframe, fin_column_name), 
            'Yield':coc_calc_quarterly(dataframe, fin_column_name), 
            "type": typef, 
            "fin_column_name":fin_column_name, 
            "Timestamp": timestamp}]

    return pd.DataFrame(data)


def db_upload(dataframe):
    

    # # Convert date column to datetime format
    dataframe["Date"]=pd.to_datetime(dataframe["Date"])
    # # Add a timestamp column
    dataframe["Timestamp"]=datetime.now()
    dataframe["Timestamp"]=pd.to_datetime(dataframe["Timestamp"])

    dataframe['GRI'] = dataframe['GRI'].astype(float)
    dataframe['EBITDA'] = dataframe['EBITDA'].astype(float)
    dataframe['Levered CF'] = dataframe['Levered CF'].astype(float)

    # Create an array "store" with the sub-dataframes with set columns: Date + Act-Forecast + "Variable" + Timestamp 
    
    if 'Actual-Forecast' in dataframe.columns:
        dataframe['Actual-Forecast'] = dataframe['Actual-Forecast'].str.lower()
    else:
        dataframe["Actual-Forecast"]="actual"

    store=[]
    for header in dataframe:
        if header == "Date" or header == "Actual-Forecast" or header == "Timestamp":
            pass
        else:         
            store.append(dataframe.loc[:,["Date", header,"Actual-Forecast", "Timestamp"]])
     
    db_data = 'mysql+mysqlconnector://' + name + ':' + password + '@' + rds_host + ':3306/' \
       + db_name + '?charset=utf8mb4'
    engine = create_engine(db_data)
    conn = engine.connect()

    # Execute the to_sql for writting DF into SQL
    for sdf in store:
        table_name=sdf.columns[1]
        sdf.rename(columns = {table_name:'Amount'}, inplace = True)
        
        # Calculate starting id for new sql update 
        try:
            id_start= pd.read_sql_query('select ifnull(max(id),0)+1 from '+table_name+"_table",conn).iloc[0,0]
        except Exception as e:
            id_start=1   
        
        # add an ID column to the dataframe and then send to sql throug the engine
        try:
            sdf.insert(0,'ID', range(id_start, id_start+ len(sdf)))
            print("Starting to upload to sql 1", file=sys.stderr)

            sdf.to_sql(table_name+"_table", engine, if_exists='append', index=False, chunksize=1000)
            print("uploaded to sql 1", file=sys.stderr)
        except Exception as e:
            logger.error(e)    
        
    
    # Execute the to_sql for writting financial metrics into SQL

    findb = db_fincalc(dataframe, dataframe.loc[0,"Actual-Forecast"] , "Levered CF", dataframe.loc[0,"Timestamp"])

    try:
        id_start= pd.read_sql_query('select ifnull(max(id),0)+1 from '+"findb_table",conn).iloc[0,0]
    except Exception as e:
        id_start=1   
    
    # add an ID column to the dataframe and then send to sql throug the engine
    try:
        findb.insert(0,'ID', range(id_start, id_start+ len(findb)))
        print("Starting to upload to sql 2", file=sys.stderr)
        findb.to_sql("findb_table", engine, if_exists='append', index=False, chunksize=1000)
        print("uploaded to sql 2", file=sys.stderr)
    except Exception as e:
            logger.error(e) 

    # improvement in the future to avoid slow append: https://blog.panoply.io/how-to-load-pandas-dataframes-into-sql
    
    engine.dispose()
    ## conn.close()
    return 'File loaded into RDS'

