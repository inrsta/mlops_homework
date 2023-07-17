import pandas as pd
import os
from datetime import datetime


def get_input_path(year, month):
    default_input_pattern = "s3://nyc-duration/taxi_type=fhv/year={YEAR:04d}/month={MONTH:04d}/predictions.parquet"
    input_pattern = os.getenv("INPUT_FILE_PATTERN", default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = "s3://nyc-duration/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet"
    output_pattern = os.getenv("OUTPUT_FILE_PATTERN", default_output_pattern)
    return output_pattern.format(year=year, month=month)


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]

columns = [
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]
df_input = pd.DataFrame(data, columns=columns)

YEAR = 2022
MONTH = 1

input_file = get_input_path(2022, 1)

print(input_file)


df_input.to_parquet(
    input_file, engine="pyarrow", compression=None, index=False, storage_options=options
)

os.system("pipenv run python batch.py 2022 1")
