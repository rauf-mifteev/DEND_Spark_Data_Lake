# Data Lake with Apache Spark project for Udacity Data Engineer Nanodegree

## Project overview
This is Data Lake project for Udacity Data Engineer Nanodegree. In this project I create an **ETL pipeline** that extracts data from **AWS S3**, transforms it using **Apache Spark** and writes the data back into partitioned **parquet files** in table directories on **AWS S3**.  This JSON files represent a user activity logs collected by a music streaming app of an imaginary startup Sparkify. And the resulting database will be used for analytical purposes.

## Files in this project
* etl.py: Loads the JSON files from S3, processes the data and the write it into S3 as tables
* dl.cfg: Stores credentials for AWS

## Datasets

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

## Database schema

### Fact Table:
* songplays: Records of song plays in log files 

### Dimension Tables:
* artists: Artists in the music database
* songs: Songs in the music database
* users: Users of the app
* time: Timestamps of records

## Prerequisites
The code is **Python** script and it requires:

* [AWS Account](https://aws.amazon.com/)
* [PySpark](https://spark.apache.org/)

## Run the Code

`python etl.py`
