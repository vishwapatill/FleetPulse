
from kafka import KafkaProducer
import pandas as pd
import json
import time
import random
import uuid
import copy
from datetime import datetime, timedelta


KAFKA_SERVER = "localhost:9093"
TOPIC = "taxi-data"

DATA_FILE = "data/green_tripdata_2026-01.parquet"

FREQ_CONTROL = 10

# 10% of events will be invalid
INVALID_PROBABILITY = 0.10

# 5% of events will be duplicates
DUPLICATE_PROBABILITY = 0.05


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ============================================================
# LOAD DATA
# ============================================================

print("Loading taxi dataset...")

df = pd.read_parquet(DATA_FILE)

df.rename(
    columns={
        "lpep_pickup_datetime": "original_ptime",
        "lpep_dropoff_datetime": "original_dtime"
    },
    inplace=True
)

df["total_time_in_sec"] = (
    df["original_dtime"] - df["original_ptime"]
).apply(lambda x: x.total_seconds())

df.drop(
    columns=[
        "original_ptime",
        "original_dtime",
        "store_and_fwd_flag"
    ],
    inplace=True,
    errors="ignore"
)

print(f"Loaded {len(df)} taxi records.")



def generate_times(duration):

    pickup = datetime.now()

    dropoff = pickup + timedelta(seconds=duration)

    return {
        "ptime": pickup.isoformat(),
        "dtime": dropoff.isoformat()
    }


def inject_invalid_data(record):

    error_type = random.choice([
        "negative_fare",
        "negative_duration",
        "invalid_pickup_location",
        "invalid_dropoff_location",
        "null_passenger_count",
        "invalid_payment_type",
        "future_timestamp",
        "huge_fare",
        "zero_passengers",
        "dropoff_before_pickup",
        "negative_distance"
    ])

    if error_type == "negative_fare":

        record["total_amount"] = -abs(
            float(record.get("total_amount", 0))
        )

    elif error_type == "negative_duration":

        record["total_time_in_sec"] = -random.randint(1, 3600)

    elif error_type == "invalid_pickup_location":

        record["PULocationID"] = 9999

    elif error_type == "invalid_dropoff_location":

        record["DOLocationID"] = -1

    elif error_type == "null_passenger_count":

        record["passenger_count"] = None

    elif error_type == "invalid_payment_type":

        record["payment_type"] = 999

    elif error_type == "future_timestamp":

        record["ptime"] = "2099-01-01T00:00:00"

    elif error_type == "huge_fare":

        record["total_amount"] = 999999.99

    elif error_type == "zero_passengers":

        record["passenger_count"] = 0

    elif error_type == "dropoff_before_pickup":

        pickup = datetime.fromisoformat(record["ptime"])

        record["dtime"] = (
            pickup - timedelta(
                seconds=random.randint(1, 3600)
            )
        ).isoformat()

    elif error_type == "negative_distance":

        if "trip_distance" in record:

            record["trip_distance"] = -abs(
                float(record["trip_distance"])
            )

        else:

            record["trip_distance"] = -10

    return record, error_type


# ============================================================
# CREATE EVENT
# ============================================================

def create_event():

    row = df.iloc[random.randrange(len(df))]

    record = row.to_dict()

    # Unique ID for deduplication
    record["event_id"] = str(uuid.uuid4())

    # Generate real-time timestamps
    duration = int(record["total_time_in_sec"])

    record.update(
        generate_times(duration)
    )

    # Convert Pandas / NumPy values to JSON-compatible values
    for key, value in record.items():

        if pd.isna(value):

            record[key] = None

        elif hasattr(value, "item"):

            record[key] = value.item()

    return record


# ============================================================
# MAIN PRODUCER LOOP
# ============================================================

last_event = None

try:

    print()
    print("============================================")
    print(" Kafka Taxi Real-Time Producer Started")
    print("============================================")

    while True:

        # ----------------------------------------------------
        # DUPLICATE EVENT
        # ----------------------------------------------------

        if (
            last_event is not None
            and random.random() < DUPLICATE_PROBABILITY
        ):

            event = copy.deepcopy(last_event)

            print("\n🔁 DUPLICATE EVENT")

        else:

            # ------------------------------------------------
            # CREATE EVENT
            # ------------------------------------------------

            event = create_event()

            # ------------------------------------------------
            # INVALID EVENT
            # ------------------------------------------------

            if random.random() < INVALID_PROBABILITY:

                event, error_type = inject_invalid_data(
                    event
                )

                print("\n⚠️ INVALID EVENT")
                print(f"   Type: {error_type}")

            else:

                print("\n✅ VALID EVENT")

        # ----------------------------------------------------
        # DISPLAY EVENT
        # ----------------------------------------------------

        print(f"   Event ID: {event.get('event_id')}")
        print(f"   Pickup:   {event.get('ptime')}")
        print(f"   Dropoff:  {event.get('dtime')}")
        print(f"   Fare:     {event.get('total_amount')}")
        print(f"   Distance: {event.get('trip_distance')}")
        print(f"   PU:       {event.get('PULocationID')}")
        print(f"   DO:       {event.get('DOLocationID')}")

        # ----------------------------------------------------
        # SEND TO KAFKA
        # ----------------------------------------------------

        producer.send(
            TOPIC,
            value=event
        )

        # Save event for possible duplicate
        last_event = copy.deepcopy(event)

        # ----------------------------------------------------
        # RANDOM DELAY
        # ----------------------------------------------------

        sleep_time = random.randint(
            0,
            FREQ_CONTROL
        )

        print(
            f"   Sleeping for {sleep_time} seconds..."
        )

        time.sleep(sleep_time)


except KeyboardInterrupt:

    print("\n\nStopping producer...")


finally:

    producer.flush()
    producer.close()

    print("Producer closed.")
