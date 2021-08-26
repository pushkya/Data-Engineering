#import libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,date_format,monotonically_increasing_id
from pyspark.sql.types import TimestampType


# Loading the credentials(access key and secret access key) from dl.cfg file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    '''
    Creates spark session with mentioned packages
    - returns spark session object 
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Loads song data from S3, creates two dimensional tables: songs table and artists table and then again writes it
    back to S3
    
    Parameters:
    - spark : spark session object
    - input_data : path of input S3 bucket
    - output_data: path of ouptut S3 bucket
    return type: none
    '''
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_df_table")
    
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'artist_name', 'year', 'duration'].dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partition_by('year', 'artist').parquet(output_data + 'songs_table/', mode= 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].withColumnRenamed('artist_name', 'name').withColumnRenamed('artist_location', 'location').withColumnRenamed('artist_latitude', 'latitude').withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()
        
    # write artists table to parquet files
    artists_table.wrtie.parquet(output_data + 'artists_table/', mode= 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Loads log data from S3, creates two dimensional tables: users table, time table and one fact table: songplays and then again writes it back to S3.
    
    Parameters:
    - spark : spark session object
    - input_data : path of input S3 bucket
    - output_data: path of ouptut S3 bucket
    return type: none
    '''
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender',  'level'].withColumnRenamed('userId','user_id').withColumnRenamed('firstName','first_name').withColumnRenamed('lastName', 'last_name').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table/', mode= 'overwrite')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    time_tab = df.withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = time_tab.select('start_time').withColumn('hour', hour(col('start_time'))).withColumn('day', dayofmonth(col('start_time'))) \
    .withColumn('week', weekofyear(col('start_time'))).withColumn('month', month(col('start_time'))).withColumn('year', year(col('start_time'))).withColumn('weekday', date_format(col('start_time'), 'E')).dropDuplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'users_table/', mode= 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.sql("select distinct song_id, artist_id, artist_name FROM song_df_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner").dropDuplicates()
    songplays_table = songplays_table['userId', 'level', 'sessionId', 'location', 'userAgent', 'song_id', 'artist_id', 'ts']
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = songplays_table.withColumn("start_time", get_timestamp(col("ts"))).drop('timestamp', 'ts')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partition_by('year', 'month').parquet(output_data + 'songplays_table/', mode= 'overwrite')


def main():
    '''
    Main function:
    - initiates spark session
    - declares input and output path of S3 buckets
    - processes song data and log data
    '''
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lakes/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
