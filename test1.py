from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import boto3

def helloWorld():
    print('Hello World')

def writeIntoS3Bucket(df_final_val):
     s3 = boto3.client('s3')

     s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id='AKIA6ODU3WQXVOXT7AGB',
        aws_secret_access_key='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'
       )
     currTimeVal = datetime.now().strftime("%Y%m%d-%H%M%S")
     fileName_val='RandomQuotes_'+currTimeVal+'.csv'
     df_final_val.to_csv(fileName_val,sep='|',index = False)
     s3.Bucket('tipico-proj').upload_file(Filename=fileName_val, Key="api-data/"+fileName_val)
     
def print_random_quote():

    response = requests.get('https://api.quotable.io/random')

    dd=response.json()
    id_val=dd['_id']
    content_val=dd['content']
    author_val=dd['author']
    tags_val=dd['tags']
    authorSlug_val=dd['authorSlug']
    length_val=dd['length']
    dateAdded_val=dd['dateAdded']
    dateModified_val=dd['dateModified']
    aa=str(id_val)+',"'+content_val+'","'+author_val+'","'+tags_val[0]+'","'+authorSlug_val+'",'+str(length_val)+','+str(dateAdded_val)+','+str(dateModified_val)
    data_val=[[(id_val),content_val,author_val,tags_val[0],authorSlug_val,(length_val),str(dateAdded_val),str(dateModified_val)]]
    df_final = pd.DataFrame(data_val, columns=['id', 'content','author','tags','authorSlug','length','dateAdded','dateModified'])
    # df_final.to_csv('df_final.csv')
    writeIntoS3Bucket(df_final)
    print(aa)
    f = open("demofile2.txt", "w")
    f.write(aa)
    f.close()

with DAG(dag_id="test1",
         start_date=datetime(2021,1,1),
          #schedule_interval="@hourly",
         schedule_interval="*/10 * * * *",
         catchup=False) as dag:

        task1 = PythonOperator(
                task_id="hello_world",
                python_callable=helloWorld)
        task2 = PythonOperator(
                task_id="print_random_quote",
                python_callable=print_random_quote)
        
task1 >> task2