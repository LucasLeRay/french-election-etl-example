from pyspark.sql import functions as sf
from pyspark.sql import SparkSession

def init_spark():
  spark = SparkSession.builder.getOrCreate()
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

spark = init_spark()

df = extract(spark)
df = transform(df)

df.show()
