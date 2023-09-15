import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField,StringType

def create_keyspace(session): #5
    #create keyspace here
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")
def create_table(session): #6
    #create table here
    session.execute("""
    CREATE TABLE IF NOT EXITS spark_streams.created_user (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);    
    """)
    print("Table created successfully!")


def insert_data(session, **kwargs):
    #insertion here
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode =kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.create_users(id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """), (user_id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture)
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

def create_spark_connection(): #1
    #create spark connection
    try:
        s_conn = SparkSession.builder\
            .appName('SparkDataStreaming')\
            .config('spark.jars.packages',"com.datastax.spark:sark-cassandra-connector_2.13:3.4.1,"
                                          "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn): #8
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('Kafka') \
            .option('kafka.boostrap.Servers', 'localhost:9092') \
            .option('subcribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because of {e}")

    return spark_df

def create_cassandra_connection(): #2
    try
        #creating to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Couldn't create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df): #10
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),

    ])

    sel = spark_df.selectExpr("CAST(value AS STRING")\
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    print(sel)

    return sel

if __name__ == "main__":
    #create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None: #3

        spark_df = connect_to_kafka(spark_conn)#9
        selection_df = create_selection_df_from_kafka(spark_df) #11
        session = create_cassandra_connection()

        if session is not None: #4
            create_keyspace(session)
            create_table(session)
            insert_data(session) #7

            streaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                                .option('checkpointLocation', '/tmp/checkpoint')\
                                .option('keyspace', 'spark_streams') \
                                .option('table', 'created_users')\
                                .start()
            streaming_query.awaitTermination()