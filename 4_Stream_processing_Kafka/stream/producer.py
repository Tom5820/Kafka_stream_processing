import csv
import json
import pandas as pd
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


# file = open('../avro_example/data/rides.csv')
csv_file = r'C:\Data engineer\taxi_datacsv.csv'
df = pd.read_csv(csv_file)
df_iter = pd.read_csv(csv_file, iterator=True, chunksize=1)
# csvreader = csv.reader(file)
# header = next(csvreader)
n = 0
while True:
    try:
        key = {"vendorId": int(df.VendorID.values[0])}
        value = {"vendorId": int(df.VendorID.values[0]), "passenger_count": int(df.passenger_count.values[0]), "trip_distance": float(df.trip_distance.values[0]), "payment_type": int(df.payment_type.values[0]), "total_amount": float(df.total_amount.values[0]),"PULocationID": int(df.PULocationID.values[0])}
        producer.send('yellow_taxi.json', value=value, key=key)
        df = next(df_iter)
        print("producing")
        sleep(1)
    except Exception as e:
        print(e)
        print("completed")
        break