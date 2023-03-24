import boto3
import botocore
import pandas as pd

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'



def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    if not key_exists(to_bucket, to_key):
        s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                        to_bucket, to_key)        
        print(f'File {to_key} saved to S3 bucket {to_bucket}')
    else:
        print(f'File {to_key} already exists in S3 bucket {to_bucket}') 

# create_bucket('nyctcl-cs653-5215')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-01.parquet',
                      to_bucket='nyctlc-cs653-5215', to_key='yellow_tripdata_2017-01.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-02.parquet',
                      to_bucket='nyctlc-cs653-5215', to_key='yellow_tripdata_2017-02.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-03.parquet',
                      to_bucket='nyctlc-cs653-5215git remote add origin https://github.com/TK1507/CS653.git', to_key='yellow_tripdata_2017-03.parquet')









query = "select * from s3object s limit 10"
bucket='nyctlc-cs653-5215'
key= 'yellow_tripdata_2017-01.parquet'
expression_type = 'SQL'
input_serialization ={'Parquet':{}}
output_serialization ={'CSV':{}}

respone = s3.select_object_content(
    Bucket = bucket,
    Key = key,
    Expression = query,
    ExpressionType = expression_type,
    InputSerialization=input_serialization,
    OutputSerialization=output_serialization,
)

for event in respone['Payload']:
    if 'Records' in event:
        records = event['Record']['Payload'].decode('utf-8')
        print(records)
