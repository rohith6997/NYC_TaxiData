import os
import json
from datetime import datetime

def find_earliest_and_latest_pickup_datetimes(folder_path):

    earliest_pickup_datetime = None
    latest_pickup_datetime = None

    file_list = os.listdir(folder_path)

    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as file:
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


def main():

    snappy_folder_path = "D:/Data Engineering/NYCtaxi/Taxi_data/decompressed_files/"

    earliest, latest = find_earliest_and_latest_pickup_datetimes(snappy_folder_path)

    print("Earliest Pickup Datetime:", earliest)

    print("Latest Pickup Datetime:", latest)

    bz2_folder_path = "D:/Data Engineering/NYCtaxi/bz2FilesData/decompressed_files/"

    earliest, latest = find_earliest_and_latest_pickup_datetimes(bz2_folder_path)

    print("Earliest Pickup Datetime:", earliest)
    
    print("Latest Pickup Datetime:", latest)


if __name__ == '__main__':
    main()