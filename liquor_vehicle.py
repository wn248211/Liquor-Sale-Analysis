'''
Find out the relationship between Liquor Sales and Vehicle Accidents
'''
import sys
import math
import uuid
from schema import liquor_schema
from matplotlib import pyplot as plt
import pandas as pd

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

cluster_seeds = ['199.60.17.32', '199.60.17.65']

spark = SparkSession.builder.appName('liquor vehicle').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
assert spark.version >= '2.4' # make sure we have Spark 2.4+


def main(liquor_sales_inputs, vehicle_accidents_inputs):
  # process vehicle accidents dataset
  va_df = spark.read.csv(vehicle_accidents_inputs, header=True)
  va_df = va_df.select(F.to_date(F.split(va_df['Crash Date & Time'], ' ')[0], "MM/dd/yyyy").alias('Date'),\
     'County', 'City', 'Drug/Alcohol Related')
  va_df = va_df.withColumn('Year', F.year(va_df['Date']))
  va_df = va_df.withColumn('Month', F.month(va_df['Date']))
  va_df = va_df.where("Year <= 2018 and Year >= 2013")
  va_df = va_df.where(va_df['Drug/Alcohol Related'].like('%Alcohol%'))
  va_df = va_df.groupBy(['Year', 'Month']).agg(F.count("*").alias('total_number_of_accidents'))

  # process liquor sales dataset
  liquor_df = spark.read.csv(liquor_sales_inputs, schema=liquor_schema)
  liquor_df = liquor_df.select(F.to_date(liquor_df['Date'], 'MM/dd/yyyy').alias('Date'), 'Category', 'Bottles Sold')
  liquor_df = liquor_df.withColumn('Year', F.year(liquor_df['Date']))
  liquor_df = liquor_df.withColumn('Month', F.month(liquor_df['Date']))
  liquor_df = liquor_df.where("Year <= 2018 and Year >= 2013")
  liquor_df = liquor_df.groupBy(['Year', "Month"]).sum('Bottles Sold')

  # join two datasets
  liquor_vehicle_df = liquor_df.join(va_df, ['Year', 'Month'])
  liquor_vehicle_df = liquor_vehicle_df.withColumn('id', F.lit(str(uuid.uuid1())))
  liquor_vehicle_df = liquor_vehicle_df.withColumnRenamed('Year', 'year').withColumnRenamed('Month', 'month')\
    .withColumnRenamed('sum(Bottles Sold)', 'bottles_sold')
  liquor_vehicle_df = liquor_vehicle_df.orderBy(['year', 'month'], ascending=True)
  liquor_vehicle_df.show()
  # liquor_vehicle_df.write.format("org.apache.spark.sql.cassandra") \
  #   .options(table="liquor_accidents", keyspace="qya23").mode('append').save()

  # calculate correlation coefficient
  six_values = liquor_vehicle_df.select(F.lit(1).alias('sum'), liquor_vehicle_df['bottles_sold'].alias('x'), liquor_vehicle_df['total_number_of_accidents'].alias('y'), \
    (liquor_vehicle_df['bottles_sold']**2).alias('x^2'), (liquor_vehicle_df['total_number_of_accidents']**2).alias('y^2'), \
    (liquor_vehicle_df['bottles_sold'] * liquor_vehicle_df['total_number_of_accidents']).alias('x*y'))
  six_sums = six_values.select(F.sum('sum'), F.sum('x'), F.sum('y'), F.sum('x^2'), F.sum('y^2'), F.sum('x*y'))
  params = six_sums.collect()[0]
  r = (params['sum(sum)'] * params['sum(x*y)'] - params['sum(x)'] * params['sum(y)']) / ((math.sqrt(params['sum(sum)'] * params['sum(x^2)'] - params['sum(x)']**2)) * (math.sqrt(params['sum(sum)'] * params['sum(y^2)'] - params['sum(y)']**2)))
  print('r^2 = {0:.6f}'.format(r**2))


if __name__ == '__main__':
  liquor_sales_inputs = sys.argv[1]
  vehicle_accidents_inputs = sys.argv[2]
  main(liquor_sales_inputs, vehicle_accidents_inputs)