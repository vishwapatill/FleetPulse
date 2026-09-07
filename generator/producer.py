from kafka import KafkaProducer
import pandas as pd
import json

df=pd.read_parquet('data/green_tripdata_2026-01.parquet')

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

import pandas as pd
import time 
import random

freq_control=10

x=lambda y: {'ptime':time.localtime(),'dtime':time.localtime(time.time()+y)}


df=pd.read_parquet('data/green_tripdata_2026-01.parquet')
df.rename(columns={'lpep_dropoff_datetime':'dtime','lpep_pickup_datetime':'ptime'},inplace=True)
df['total_time_in_sec']=(df["dtime"]-df["ptime"]).apply(lambda x: x.total_seconds())
df.drop('dtime',axis=1,inplace=True)
df.drop('store_and_fwd_flag',axis=1,inplace=True)
df.drop('ptime',axis=1,inplace=True)


try:
    while True:
        t = dict(df.iloc[random.randrange(len(df))])

        t.update(x(int(t['total_time_in_sec'])))

        print(t)

        producer.send("orders", value=t)

        st = random.randint(0, freq_control)
        print(f"sleeping for {st}..")
        time.sleep(st)

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.flush()
    producer.close()
    print("Producer closed.")