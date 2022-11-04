from pyspark.sql import SparkSession


RAW_HOLIDAYS_FILEPATH = '/tmp/abstract/holidays.json'


def read_raw_holidays(spark):
    return spark.read.json(RAW_HOLIDAYS_FILEPATH)


def save_holidays(df):
    TMP_FILEPATH = '/tmp/spark/cleanholidays.parquet'
    df.write.mode('overwrite').parquet(TMP_FILEPATH)


def process_holidays():
    spark = SparkSession.builder.appName("Process raw holidays").master("local").getOrCreate()
    raw_holidays_df = read_raw_holidays(spark)
    save_holidays(raw_holidays_df)
    spark.stop()


process_holidays()
