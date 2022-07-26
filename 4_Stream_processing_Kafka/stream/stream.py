import faust
from taxi_schema import TaxiRide


app = faust.App('stream', broker='kafka://localhost:9092')
topic = app.topic('yellow_taxi.json', value_type=TaxiRide)


@app.agent(topic)
async def start_reading(records):
    async for record in records:
        print(record)


if __name__ == '__main__':
    app.main()