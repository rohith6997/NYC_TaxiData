import boto3
import snappy
import io
import time

def download_and_measure_time(s3, bucket_name, file_key):

    start_time = time.time()

    #Obtain the object with the help of bucket name and key
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)

    #Convert the entire body content into string
    s3_data = s3_object['Body'].read()

    #print the size of content
    file_size = s3_object['ContentLength']  # Get file size

    #download time for each file
    download_time = time.time()-start_time

    print("The file size is {} bytes".format(file_size))

    return s3_data, download_time
    

def extract_and_decompress_from_s3(bucket_name, object_prefix, local_path):

    s3 = boto3.client('s3')

    #Put the entire content into dictionary
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=object_prefix)

    total_download_time=0

    if 'Contents' in response:
        for obj in response['Contents']:

            file_key = obj['Key']

            # Download Snappy-compressed file from S3
            s3_data, download_time = download_and_measure_time(s3, bucket_name, file_key)

            print("Download time for {}: {:.0f} seconds".format(file_key[len(object_prefix):], download_time))
            
            #Convert the bytestring s3_data into an object that can be accesed as a file using io module.
            snappyfile = io.BytesIO(s3_data)
            
            decompression_start_time = time.time()

            decompressed_data = io.BytesIO()

            #Decompress the data using stream_decompress
            snappy.stream_decompress(src=snappyfile, dst=decompressed_data)

            decompressed_data.seek(0)

            #Calculate the decompression time
            decompression_time = time.time()-decompression_start_time
            
            print("Decompression time for {}: {:.0f} seconds".format(file_key[len(object_prefix):], decompression_time))

            # Save decompressed data to a local file
            
            with open("D:/Data Engineering/NYCtaxi/Taxi_data/decompressed_files/"+f"decompressed_file_part_{file_key.split('-')[-1].split('.')[0]}.json", 'wb') as local_file:
                local_file.write(decompressed_data.read())
            
            #Print the total download time for all the partitions
            total_download_time = total_download_time + download_time

        print("Total Download time for all files:",total_download_time)

    else:
        print('No related objects found in S3 Bucket.')

    return 0



def main():

    local_path = "D:/Data Engineering/NYCtaxi/Taxi_data/files.snz"

    object_prefix = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"
    
    bucket_name = "aws-bigdata-blog"

    extract_and_decompress_from_s3(bucket_name, object_prefix, local_path)

if __name__ == '__main__':
    main()


