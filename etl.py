from cgi import print_environ_usage
import configparser
from datetime import datetime
import os
from time import monotonic
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import glob

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
                and then again loaded back to S3
        
    Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    # get filepath to song data file
    song_data  = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    print(df.count())
    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').\
            parquet(os.path.join(output_data,'songs.parquet'),'overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artist.parquet'),'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
                    and then again loaded back to S3. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
            
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df =  spark.read.json(log_data)
    
    # filter by actions for song plays
    songplays_table = df['ts', 'userId', 'level','sessionId', 'location', 'userAgent']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'user.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda r:str(int(int(r)/1000)))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda r:str(datetime.fromtimestamp(int(r)/1000)))
    df = df.withColumn('datetime',get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select([
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year') 
    ])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,'time.parquet'),'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song-data/A/A/A/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month')
    )
    songplays_table = songplays_table.select(monotonically_increasing_id().alias('songplay_id'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,'songplay.parquet'),'overwrite')


def main():
    """
    Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakeyangsun/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
