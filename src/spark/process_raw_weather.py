from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, to_date


RAW_WEATHER_FILEPATH = '/tmp/nws/forecast.json'


def read_raw_weather(spark):
    return spark.read.json(RAW_WEATHER_FILEPATH)


def flatten_city_data(df):
    df = df.select('city', explode('forecasts').alias('forecasts'))
    return df.select('city', 'forecasts.*')


def select_required_columns(df):
    required_columns = ['city', 'startTime', 'isDaytime', 'temperature', 'shortForecast']
    return df.select(*required_columns)


def transform_start_dates(df):
    df = df.withColumn('start_date', to_date('startTime'))
    return df.drop('startTime')


def rename_columns(df):
    df = df.withColumnRenamed('isDaytime', 'is_daytime')
    df = df.withColumnRenamed('shortForecast', 'short_forecast')
    return df


def clean_weather(df):
    df = flatten_city_data(df)
    df = select_required_columns(df)
    df = transform_start_dates(df)
    df = rename_columns(df)
    return df


def save_weather(df):
    TMP_FILEPATH = '/tmp/spark/cleanweather.parquet'
    df.write.mode('overwrite').parquet(TMP_FILEPATH)


def process_weather():
    spark = SparkSession.builder.appName("Process raw weather").master("local").getOrCreate()
    raw_weather_df = read_raw_weather(spark)
    weather_df = clean_weather(raw_weather_df)
    save_weather(weather_df)
    spark.stop()


process_weather()