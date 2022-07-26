import chunk
from ctypes import *
from email import iterators
from pickle import TRUE
CDLL("C:\ProgramData\Anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-ccff28b0.dll")
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
import pandas as pd
from time import sleep


def load_avro_schema_from_file():
    key_schema = avro.load("taxi_ride_key.avsc")
    value_schema = avro.load("taxi_ride_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    # file = open('data/rides.csv')
    csv_file = r'C:\Data engineer\taxi_datacsv.csv'
    df = pd.read_csv(csv_file)
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=1)
    # df = next(df)
    
    while True:
        try:
            key = {"vendorId": df.VendorID}
            value = {"vendorId": df.VendorID, "passenger_count": df.passenger_count, "trip_distance": df.trip_distance, "payment_type": df.payment_type, "total_amount": df.total_amount}
            df = next(df_iter)
            try :
                producer.produce(topic='yellow_taxi', key=key, value=value)
            except Exception as e:
                print(f"Exception while producing record value - {value}: {e}")
            else:
                print(f"Successfully producing record value - {value}")
        except StopIteration:
            print(e)
            print("completed")
            break
        producer.flush()
        sleep(1)

if __name__ == "__main__":
    send_record()