# Columns in event
# [
#   '_embedded',               # Extra links and other information
#   '_links',                  # Links to local attractions, event, and venue
#   'accessibility',           # Accessible ticket limit
#   'ageRestrictions',         # Age restrictions for event
#   'classifications',         # Details about type of event
#   'dates',                   # Date that tickets go on sale
#   'description',             # Description of event (sometimes empty)
#   'doorsTimes',              # Times doors open (sometimes empty)
#   'id',                      # Unique ticketmaster id for event
#   'images',                  # Links to images for event
#   'info',                    # Extra information about event (ie COVID disclaimer)
#   'locale',                  # Language event information is in (ie en-us)
#   'name',                    # Event name
#   'pleaseNote',              # Extra notes about event (ie COVID disclaimer)
#   'priceRanges',             # Minimum and maximum ticket prices
#   'products',                # Extra products for purchase (ie parking passes)
#   'promoter',                # Event promoter name
#   'promoters',               # List of event promoters
#   'sales',                   # Different times that tickets go on sale
#   'seatmap',                 # Link to seat map
#   'test',                    # Whether this is a test record
#   'ticketLimit',             # Overall ticket limit
#   'ticketing',               # Ticketing details
#   'type',                    # Type of occasion (ie 'event')
#   'url'                      # URL for event
# ]
#
# Columns to keep
#   id                       # Leave as is
#   name                     # Leave as is
#   description              # Leave as is or create if not present
#   url                      # Leave as is
#   start_day                # Extract from dates
#   start_time               # Extract from dates
#   info                     # Leave as is
#   pleaseNote               # Rename to please_note
#   event_type               # Extract from classification
#   genre                    # Extract from classification
#   subgenre                 # Extract from classification
#   minimum_ticket_price     # Extract from priceRanges
#   maximum_ticket_price     # Extract from priceRanges


from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit


RAW_EVENTS_FILEPATH = '/tmp/ticketmaster/events.json'


def read_raw_events(spark):
    return spark.read.json(RAW_EVENTS_FILEPATH)


def filter_out_test_events(df):
    return df.filter(~col('test'))


def drop_unnecessary_columns(df):
    columns_to_drop = [
        '_embedded',
        '_links',
        'accessibility',
        'ageRestrictions',
        'doorsTimes',
        'images',
        'locale',
        'products',
        'promoter',
        'promoters',
        'sales',
        'seatmap',
        'test',
        'ticketLimit',
        'ticketing',
        'type'
    ]

    return df.drop(*columns_to_drop)


def add_date_time_info(df):
    df = df.withColumn('start_date', col('dates')['start']['localDate'])
    df = df.withColumn('start_time', col('dates')['start']['localTime'])
    return df.drop('dates')


def rename_columns(df):
    return df.withColumnRenamed('pleaseNote', 'please_note')


def add_event_classification_info(df):
    df = df.withColumn('event_type', col('classifications')[0]['segment']['name'])
    df = df.withColumn('genre', col('classifications')[0]['genre']['name'])
    df = df.withColumn('sub_genre', col('classifications')[0]['subGenre']['name'])
    return df.drop('classifications')


def add_ticket_price_info(df):
    df = df.withColumn('min_ticket_price', col('priceRanges')[0]['min'])
    df = df.withColumn('max_ticket_price', col('priceRanges')[0]['max'])
    return df.drop('priceRanges')


def add_event_description(df):
    default = 'No description provided.'
    return df.withColumn('description', coalesce('description', lit(default)))


def clean_events(df):
    df = filter_out_test_events(df)
    df = drop_unnecessary_columns(df)
    df = add_date_time_info(df)
    df = rename_columns(df)
    df = add_event_classification_info(df)
    df = add_ticket_price_info(df)
    df = add_event_description(df)
    return df


def save_events(df):
    TMP_FILEPATH = '/tmp/spark/cleanevents.parquet'
    df.write.mode('overwrite').parquet(TMP_FILEPATH)


def process_events():
    spark = SparkSession.builder.appName("Process raw events").master("local").getOrCreate()
    events_df = read_raw_events(spark)
    events_df = clean_events(events_df)
    save_events(events_df)
    spark.stop()


def print_basic_dataset_attributes(df):
    print('Event Count: ', df.count())
    print('Columns: ', df.columns)
    print('Schema:')
    df.printSchema()
    print('First row:')
    df.show(1)


process_events()
