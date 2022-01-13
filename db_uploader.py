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


rds_host = rds_config.rds_host
name = rds_config.name
password = rds_config.password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

col_csv = ["date", "GRI", "EBITDA"]
col_sql = ["date", "GRI", "EBITDA"]


def db_upload(dataframe):
    
    
    # OLD
    # s3 = boto3.client('s3')
	
    # s3_resource = boto3.resource('s3')
	
    # if event:
	# 	s3_records = event['Records'][0]
	# 	bucket_name = str(s3_records['s3']['bucket']['name'])
	# 	file_name = str(s3_records['s3']['object']['key'] )
		# download_path = '/tmp/{}'.format(file_name)
		# file_type = file_name.split('_')[0]
    # s3_resource.meta.client.download_file(bucket_name,file_name,download_path)



    # Convert date column to datetime format
    dataframe["Date"]=pd.to_datetime(dataframe["Date"])
    # Add a timestamp column
    dataframe["Timestamp"]=datetime.now()
    dataframe["Timestamp"]=pd.to_datetime(dataframe["Timestamp"])

    # Create an array "store" with the sub-dataframes  with columns: Date + Variable 
    
    store=[]
    for header in dataframe:
        if header == "Date" or header == "Timestamp":
            pass
        else:
            store.append(dataframe.loc[:,["Date",header, "Timestamp"]])
     
    db_data = 'mysql+mysqlconnector://' + name + ':' + password + '@' + rds_host + ':3306/' \
       + db_name + '?charset=utf8mb4'
    engine = create_engine(db_data)
    conn = engine.connect()
    
    # try:
    #     conn = pymysql.connect(host=rds_host, 
    #                            user=name, 
    #                            password=password, 
    #                            database=db_name)

    # except Exception as e:
    #     logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    #     logger.error(e)
    #     sys.exit()

    # logger.info("SUCCESS: Connection to RDS mysql instance succeeded")


    # create cursor
    ## cur=conn.cursor()
    # Execute the to_sql for writting DF into SQL
    for sdf in store:
        table_name=sdf.columns[1]
        sdf.rename(columns = {table_name:'Amount'}, inplace = True)
        
        try:
            id_start= pd.read_sql_query('select ifnull(max(id),0)+1 from '+table_name+"_table",conn).iloc[0,0]
        except Exception as e:
            id_start=1   
        
        try:
            sdf.insert(0,'ID', range(id_start, id_start+ len(sdf)))
            sdf.to_sql(table_name+"_table", engine, if_exists='append', index=False, chunksize=1000)
        except Exception as e:
                logger.error(e)    
        # conn.commit()
    
    # improvement in the future to avoid slow append: https://blog.panoply.io/how-to-load-pandas-dataframes-into-sql
    engine.dispose()
    ## conn.close()
    return 'File loaded into RDS' 