# Project Title
NYC Taxi Data

## Overview

This project performs the following tasks:

- Downloads and decompresses Bzip2 files from an S3 bucket.
- Processes the pickup datetime information from the decompressed data.

## Requirements

- Python 3 and necessary libraries

## Steps
1. Create a launch configuration with appropriate version and configurations that includes file name, type of file and arguements.
2. Install boto3 library using pip.
3. Create a main function and use argparser to parse command line arguements
4.  Replace placeholders like `BUCKET_NAME`, `OBJECT_PREFIX`, and others with the actual details related to your project.
5. Create a function to extract the s3 object, decompress it using .bz2 module.
6. Now read the decompressed data and extract the earliest and latest pickup date for each file.
7. Repeat the steps 4 and 5 for all the files and print the overall earliest and latest dates among all files.
  


