import sys
from schema import liquor_schema
import json
from pyspark.sql import SparkSession, functions, types
import threading

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

cluster_seeds = ['199.60.17.32', '199.60.17.65']

spark = SparkSession.builder.appName('liquor sales consumer') \
  .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary
def process(batch_df, batch_id):
  if batch_df.count() == 0:
    return

  # parse json messages and transfer to a dataframe  
  liquor_list = []
  try:
    for json_str in batch_df.collect():
      liquor_list.append(json.loads(json_str['value']))
  except Exception:
    return
  liquor_df = spark.createDataFrame(liquor_list, liquor_schema)
  print('Batch: %d' % batch_id )
  liquor_df.show()

  # stored in cassandra
  liquor_df.write.format("org.apache.spark.sql.cassandra") \
    .options(table="liquor_sales_2019", keyspace="qya23").mode("append").save()

  # update total sales of this month
  liquor_df = liquor_df.select(functions.to_date(liquor_df['Date'], 'MM/dd/yyyy').alias('Date'), \
    'City', 'Category', 'Sale (Dollars)', 'Bottles Sold')
  liquor_df = liquor_df.withColumn('year', functions.year(liquor_df['Date']))
  liquor_df = liquor_df.withColumn('month', functions.month(liquor_df['Date']))
  update_df = liquor_df.groupBy(['year', 'month']).sum("Sale (Dollars)")
  
  monthly_total_sale = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="monthly_total_sale", keyspace="yla622").load()
  uuid = monthly_total_sale.where('year = 2019 and month = 10').select('id').collect()[0]['id']
  monthly_total_sale = monthly_total_sale.join(update_df, ['year', 'month'])
  
  if monthly_total_sale.count() == 0:
    update_df = update_df.select('year', 'month', update_df['sum(Sale (Dollars))'].alias('total_sale_in_dollars')).withColumn('id', functions.lit(uuid))
    update_df.write.format("org.apache.spark.sql.cassandra") \
      .options(table="monthly_total_sale", keyspace="yla622").mode('append').save()
    return

  monthly_total_sale = monthly_total_sale.select(monthly_total_sale['id'], monthly_total_sale['year'], monthly_total_sale['month'], \
    (monthly_total_sale['total_sale_in_dollars'] + monthly_total_sale['sum(Sale (Dollars))']).alias('total_sale_in_dollars'))
  monthly_total_sale.write.format("org.apache.spark.sql.cassandra") \
    .options(table="monthly_total_sale", keyspace="yla622").mode('append').save()


def main(topic):
  messages = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
    .option('subscribe', topic).load()
  
  result = messages.select(messages['value'].cast("string"))
  stream = result.writeStream.trigger(processingTime='10 seconds').foreachBatch(process).start()
  stream.awaitTermination(600)

if __name__ == '__main__':
  topic = sys.argv[1]
  main(topic)