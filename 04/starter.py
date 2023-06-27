#!/usr/bin/env python
# coding: utf-8
import pickle
import pandas as pd
import numpy
import argparse

with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df



def run(year: int, month: int):
    print(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet')
    df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet')
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    df['predicted_duration'] = y_pred

    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    result_df = df[['ride_id', 'predicted_duration']]
    output_file = f"results_{year:04d}-{month:02d}"
    result_df.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )

    print(result_df.loc[:, 'predicted_duration'].mean())
    
    return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ride duration predicter')
    parser.add_argument('-y','--year', help='Year', required=True, type=int)
    parser.add_argument('-m','--month', help='Month', required=True, type=int)
    args = vars(parser.parse_args())
    run(year=args['year'], month=args['month'])


