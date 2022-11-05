from pyspark.sql import SparkSession


CLEAN_EVENTS_FILEPATH = '/tmp/spark/cleanevents.parquet'
CLEAN_WEATHER_FILEPATH = '/tmp/spark/cleanweather.parquet'
CLEAN_HOLIDAYS_FILEPATH = '/tmp/spark/cleanholidays.parquet'


def read_clean_events(spark):
    return spark.read.parquet(CLEAN_EVENTS_FILEPATH)


def read_clean_weather(spark):
    return spark.read.parquet(CLEAN_WEATHER_FILEPATH)


def read_clean_holidays(spark):
    return spark.read.parquet(CLEAN_HOLIDAYS_FILEPATH)


def add_daytime_weather(events_df, weather_df):
    daytime_weather_df = (weather_df.filter(weather_df.is_daytime)
                                    .drop('is_daytime')
                                    .withColumnRenamed('temperature', 'daytime_temperature')
                                    .withColumnRenamed('short_forecast', 'daytime_forecast'))

    enriched_df = events_df.join(daytime_weather_df, ['start_date', 'city'], 'left')
    return enriched_df


def add_nighttime_weather(events_df, weather_df):
    night_weather_df = (weather_df.filter(~weather_df.is_daytime)
                                  .drop('is_daytime')
                                  .withColumnRenamed('temperature', 'night_temperature')
                                  .withColumnRenamed('short_forecast', 'night_forecast'))

    enriched_df = events_df.join(night_weather_df, ['start_date', 'city'], 'left')
    return enriched_df


def add_holidays(events_df, holidays_df):
    enriched_df = (events_df.join(holidays_df,
                                 (events_df.start_date == holidays_df.date), 'left'))

    return enriched_df.drop('date')


def get_enriched_events(events_df, weather_df, holidays_df):
    enriched_df = add_daytime_weather(events_df, weather_df)
    enriched_df = add_nighttime_weather(enriched_df, weather_df)
    enriched_df = add_holidays(enriched_df, holidays_df)
    return enriched_df.sort('start_date', 'start_time')


def save_enriched_events(df):
    TMP_FILEPATH = '/tmp/spark/enriched_events.parquet'
    df.write.mode('overwrite').parquet(TMP_FILEPATH)


def enrich_events():
    spark = SparkSession.builder.appName("Enrich events").master("local").getOrCreate()
    clean_events_df = read_clean_events(spark)
    clean_weather_df = read_clean_weather(spark)
    clean_holidays_df = read_clean_holidays(spark)
    enriched_df = get_enriched_events(clean_events_df, clean_weather_df, clean_holidays_df)
    save_enriched_events(enriched_df)
    spark.stop()


enrich_events()
