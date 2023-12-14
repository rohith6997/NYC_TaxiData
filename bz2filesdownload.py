import argparse
import boto3
import bz2
import io
import json
from datetime import datetime

def process_events(decompressed_data):
    earliest_pickup_datetime = None
    latest_pickup_datetime = None

    with io.BytesIO(decompressed_data) as file:
        for line in file:
            json_obj = json.loads(line)
            pickup_datetime_str = json_obj.get('pickup_datetime', 'N/A')

            if pickup_datetime_str != 'N/A':
                pickup_datetime = datetime.strptime(pickup_datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")

                if earliest_pickup_datetime is None or pickup_datetime < earliest_pickup_datetime:
                    earliest_pickup_datetime = pickup_datetime

                if latest_pickup_datetime is None or pickup_datetime > latest_pickup_datetime:
                    latest_pickup_datetime = pickup_datetime

    return earliest_pickup_datetime, latest_pickup_datetime

def decompress_and_process_from_s3(bucket_name, object_prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=object_prefix)

    overall_earliest_pickup_datetime = None
    overall_latest_pickup_datetime = None

    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.split('.')[-1] == "bz2":
                # Decompress the data using bz2.BZ2File
                s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
                s3_data = s3_object['Body'].read()

                with io.BytesIO(s3_data) as bz2file, bz2.BZ2File(bz2file, 'rb') as f:
                    decompressed_data = f.read()

                # Process pickup datetime information
                earliest_pickup, latest_pickup = process_events(decompressed_data)

                # Update overall earliest and latest pickup datetimes
                if overall_earliest_pickup_datetime is None or earliest_pickup < overall_earliest_pickup_datetime:
                    overall_earliest_pickup_datetime = earliest_pickup

                if overall_latest_pickup_datetime is None or latest_pickup > overall_latest_pickup_datetime:
                    overall_latest_pickup_datetime = latest_pickup

    else:
        print('No related objects found in S3 Bucket.')

    print("Overall Earliest Pickup Datetime:", overall_earliest_pickup_datetime)
    print("Overall Latest Pickup Datetime:", overall_latest_pickup_datetime)

    return 0

def main():
    # Use argparse to parse command line arguments
    parser = argparse.ArgumentParser(description='Decompress and process Bz2 files from S3 in-memory')
    parser.add_argument('--bucket-name', required=True, help='Name of the S3 bucket')
    parser.add_argument('--object-prefix', required=True, help='Prefix for S3 object keys')

    args = parser.parse_args()

    # Call the operations for Bz2 files
    decompress_and_process_from_s3(args.bucket_name, args.object_prefix)

if __name__ == '__main__':
    main()
