from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
import psycopg2

def connect_db():
  conn = psycopg2.connect(
        host = "my_postgres",
        database = "election",
        user = "admin",
        password = "admin")
  
  print('Connection to PostgreSQL established')
  return conn

def create_table(cur):
  cur.execute("CREATE TABLE IF NOT EXISTS election_table( \
    Coordinates VARCHAR(255) NOT NULL, \
    Latitude FLOAT NOT NULL, \
    Longitude FLOAT NOT NULL, \
    Name VARCHAR(255) NOT NULL, \
    Voted integer NOT NULL \
  );")

  print("Table created in PostgresSQL")

def init_spark():
  spark = SparkSession.builder \
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .getOrCreate()
  print('Spark initialized')
  return spark

def extract(spark):
  file = '/job/data/presidential_results.csv'
  return spark.read.csv(file, header=True)

def transform(df):
  df = df \
    .withColumn('Voted', df['Voted'].cast('int')) \
    .withColumn('Abstentions', df['Abstentions'].cast('int')) \
    .withColumn('Nulls', df['Nulls'].cast('int')) \
    .withColumn('Name', sf.concat(sf.col('First Name'), sf.lit(' '), sf.col('Surname')))

  kvs = sf.explode(sf.array([
    sf.struct(sf.lit(c).alias("Name"), sf.col(c).alias("Voted")) for c in ['Nulls', 'Abstentions']
  ])).alias("kvs")
  new_rows = df.select(['Coordinates'] + [kvs]).select(['Coordinates'] + ["kvs.Name", "kvs.Voted"])
  df = df.select('Coordinates', 'Voted', 'Name')
  df = df.union(new_rows)

  df = df.na.drop()
  df = df.withColumn('Latitude', sf.split('Coordinates', ', ')[0].cast('float'))
  df = df.withColumn('Longitude', sf.split('Coordinates', ', ')[1].cast('float'))
  return df.select('Coordinates', 'Latitude', 'Longitude', 'Name', 'Voted')

def load(df):
  conn = connect_db()
  cur = conn.cursor()
  create_table(cur)
  
  seq = [tuple(x) for x in df.collect()]
  records = ','.join(['%s'] * len(seq))
  query = "INSERT INTO election_table ( \
    Coordinates, \
    Latitude, \
    Longitude, \
    Name, \
    Voted \
  ) VALUES {}".format(records)
  cur.execute(query, seq)

  print("Data inserted into PostgreSQL")

  cur.close()
  print("Commiting changes to database")
  conn.commit()
  print("Closing connection", "\n")
  conn.close()
  print("ETL job done!")

spark = init_spark()

df = extract(spark)
df = transform(df)
df.show()
load(df)
