from datetime import timedelta
import faust
from taxi_schema import TaxiRide


app = faust.App('stream.windowing', broker='kafka://localhost:9092')
topic = app.topic('yellow_taxi.json', value_type=TaxiRide)

PU_rides = app.Table('PU_rides_windowed', default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1),
)


@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.PULocationID):
        print(event)
        PU_rides[event.PULocationID] += 1


if __name__ == '__main__':
    app.main()