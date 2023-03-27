
# # CS653 2/2565
# Homework 2: Amazon S3 Select with Taxi Trip Data
# Name: นายธีธัช คุ้มไข่มุก Student ID: 6509035215   
# สร้าง Amazon S3 bucket ชื่อ nyctlc-cs653-5215 โดยแทน xxxx ด้วยเลข 4 ตัวท้ายของรหัสน.ศ. ของตัวเอง import boto3
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




import boto3
import botocore
import pandas as pd


# a) ในเดือน Jan 2017 มีจ านวน yellow taxi rides ทั้งหมดเท่าไร แยกจ านวน rides ตาม
# ประเภทการจ่ายเงิน (payment)
import boto3
s3 = boto3.client('s3')
sum = 0

for i in range(1, 6):
    resp = s3.select_object_content(
        Bucket='nyctlc-cs653-5215',
        Key='yellow_tripdata_2017-01.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT COUNT(payment_type) FROM s3object s WHERE payment_type={i}",
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}}
    )

for event in resp['Payload']:
    if 'Record' in event:
        record= event['Records']['Payload'].decode('utf-8')
        sum=sum+int(record)
        print(f"มีyellow taxiทั้งหมด{sum}คัน")

# b) ในเดือน Jan 2017 yellow taxi rides ในแต่ละจุดรับผู้โดยสาร (Pickup location) เป็น
# จ านวน rides มากน้อยเท่าไร และมีค่าโดยสารรวมของ rides และจ านวนผู้โดยสารเฉลี่ยต่อ 
# rides ในแต่ละจุดเท่าไร       
# เนื่องจากคำสั่ง DISTINCT ไม่สามารถใช้กับ S3 Select จึงใช้คำสั่งของ pandas เพื่อ
# หาค่าทั้งหมดที่เป็นไปได้ของข้อมูล payment_type นั่นคือคำสั่ง 
# dataFrame.[‘payment_type’].unique() แล้วจัดเรียงค่าจากน้อยไปหามาก มีการคืนค่า
# มา 265 ค่าดังภาพ แปลว่ามีจุดรับผู้โดยสารรวม 265 แห่ง ดังภาพ

import numpy as np
yellow_jan_PULocationID=df['PULocationID'].unique()
np.sort(yellow_jan_PULocationID)
def cal_total_fare(id):
    resp = s3.select_object_content(
        Bucket='nyctlc-cs653-5215',
        Key='yellow_tripdata_2017-01.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT SUM(total_amount) FROM s3object s WHERE PULocationID={id}",
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}}
    )
    for event in resp['Payload']:
        if 'Records' in event:
            record = event['Records']['Payload'].decode('utf-8')
            try:
                isinstance(float(record), float)
                return float(record)
            except:
                return None


def cal_avg_passenger_count(id):
    resp = s3.select_object_content(
        Bucket='nyctlc-cs653-5215',
        Key='yellow_tripdata_2017-01.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT AVG(passenger_count) FROM s3object s WHERE PULocationID={id}",
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}},
    )
    for event in resp['Payload']:
        if 'Records' in event:
            record = event['Records']['Payload'].decode('utf-8')
            try:
                isinstance(float(record), float)
                return float(record)
            except:
                return None

pickUpLocationId=[]
total_fare_list=[]
avg_passenger_list=[]


for event in resp['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
        pickUpLocationId.append(i)
        print(f"จุดรับผู้โดยสารจุดที่ {i} มีจำนวน rides ของ yellow taxi เท่ากับ {int(records)} ครั้ง")
        total_fare = float("{:.2f}".format(total_fare))
        total_fare_list.append(total_fare)
        print("ค่าโดยสารรวม", total_fare)
        avg_passenger = cal_avg_passenger_count(i)
        if isinstance(avg_passenger, float):
            avg_passenger = float("{:.2f}".format(avg_passenger))
            avg_passenger_list.append(avg_passenger)
            print(f"ค่าโดยสารรวม {avg_passenger} บาท")
        else:
            avg_passenger = 0.0
            avg_passenger_list.append(total_fare)
            print("ค่าโดยสารรวม Not float")
        if isinstance(avg_passenger, float):
            avg_passenger = float("{:.2f}".format(avg_passenger))
            avg_passenger_list.append(avg_passenger)
            print(f"ค่าโดยสารรวม {avg_passenger} บาท")
        else:
            avg_passenger = 0.0
            avg_passenger_list.append(total_fare)
            print("ค่าโดยสารรวม Not float")



import pandas as pd

pickUpLocation = ['A', 'B', 'C']
total_fare_list = [50, 70, 100]
avg_passenger_list = [1.5, 2, 2.5]

data = {'จุดรับผู้โดยสารที่': pickUpLocation,
        'ค่าโดยสารรวม': total_fare_list,
        'จำนวนผู้โดยสารเฉลี่ยต่อรอบ': avg_passenger_list}

hw_item2 = pd.DataFrame(data)
hw_item2


# c) ในเดือน Jan - May 2017 มีจ านวน yellow taxi rides ทั้งหมดเท่าไร แยกจำนวน rides 
# ตามประเภทการจ่ายเงิน (payment)


type1=[]
type2=[]
type3=[]
type4=[]
type5=[]
sum_ride=[]



import boto3

s3 = boto3.client('s3')

def cal_ride_each_month(month):
    sum = 0
    type1 = []
    type2 = []
    type3 = []
    type4 = []
    type5 = []
    sum_ride = []

    for type in range(1, 6):
        resp = s3.select_object_content(
            Bucket='nyctlc-cs653-5215',
            Key=f'yellow_tripdata_2017-0{month}.parquet',
            ExpressionType='SQL',
            Expression=f"SELECT COUNT(payment_type) FROM s3object s WHERE payment_type = {type}",
            InputSerialization={'Parquet': {}},
            OutputSerialization={'CSV': {}},
        )

        for event in resp['Payload']:
            if 'Records' in event:
                record = event['Records']['Payload'].decode('utf-8')
                records = int(record)
                sum = sum + records
                if type == 1:
                    type1.append(records)
                elif type == 2:
                    type2.append(records)
                elif type == 3:
                    type3.append(records)
                elif type == 4:
                    type4.append(records)
                else:
                    type5.append(records)

                print(f"จำนวน yellow taxi ride เดือน {month} ที่มี payment_type={type} เท่ากับ {records}")
        
        sum_ride.append(sum)
        print(f"rides เดือน {month} มี yellow taxi rides รวมทั้งสิ้น {sum} เที่ยว")
        print()


import boto3

# define function cal_rides_each_month

for month in range(1, 6):
    cal_rides_each_month(month)




import pandas as pd

data={
    "month":['Jan','Feb','Mar','April','May'],
    "payment type1":type1,
    "payment type2":type2,
    "payment type3":type3,
    "payment type4":type4,
    "payment type5":type5,
    'sum':sum_rides
}

hw_item3=pd.DataFrame(data)
hw_item3

