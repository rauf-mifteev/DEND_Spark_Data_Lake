import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    print("========== process_song_data ===========")
    
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
      
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']].dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artists_table = artists_table \
        .withColumnRenamed("artist_name", "name") \
        .withColumnRenamed("artist_location", "location") \
        .withColumnRenamed("artist_latitude", "latitude") \
        .withColumnRenamed("artist_longitude", "longitude") \
        .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    
    print("========== process_log_data ==========")
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    #log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df["page"] == "NextSong"]

    # extract columns for users table    
    users_table = df[["userId", "firstName", "lastName", "gender", "level"]].dropDuplicates()
    users_table = users_table \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name")
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_datetime(x), TimestampType())
    df = df.withColumn("datetime", get_timestamp(df.ts))
    
    # extract columns to create time table
    df = df.withColumn("hour", F.hour("timestamp"))        \
                   .withColumn("day", F.dayofweek("timestamp"))    \
                   .withColumn("week", F.weekofyear("timestamp"))  \
                   .withColumn("month", F.month("timestamp"))      \
                   .withColumn("year", F.year("timestamp"))        \
                   .withColumn("weekday", F.dayofweek("timestamp")) 
    time_table = df.select("datetime", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")
    song_df = song_df.withColumnRenamed("year", "song_year")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner")
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())
    songplays_table = songplays_table[["songplay_id", "datetime", "userId", "level", "song_id",
                                       "artist_id", "sessionId", "location", "userAgent", "month", "year"]]
    songplays_table = songplays_table \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("sessionId", "session_id") \
        .withColumnRenamed("userAgent", "user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    
    # S3 input and output
    input_data = "s3a://udacity-dend/"
    #output_data = "s3a://udacity-dend/P4-output/"
    
    # Local input and output folders
    #input_data = "./data/"
    output_data = "./data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("========== Job is done ==========")

if __name__ == "__main__":
    main()
