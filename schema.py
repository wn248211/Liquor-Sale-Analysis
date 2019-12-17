from pyspark.sql import types

liquor_schema = types.StructType([
  types.StructField('Invoice/Item Number', types.StringType()),
  types.StructField('Date', types.StringType()),
  types.StructField('Store Number', types.StringType()),
  types.StructField('Store Name', types.StringType()),
  types.StructField('Address', types.StringType()),
  types.StructField('City', types.StringType()),
  types.StructField('Zip Code', types.StringType()),
  types.StructField('Store Location', types.StringType()),
  types.StructField('County Number', types.StringType()),
  types.StructField('County', types.StringType()),
  types.StructField('Category', types.StringType()),
  types.StructField('Category Name', types.StringType()),
  types.StructField('Vendor Number', types.StringType()),
  types.StructField('Vendor Name', types.StringType()),
  types.StructField('Item Number', types.StringType()),
  types.StructField('Item Description', types.StringType()),
  types.StructField('Pack', types.IntegerType()),
  types.StructField('Bottle Volume (ml)', types.IntegerType()),
  types.StructField('State Bottle Cost', types.FloatType()),
  types.StructField('State Bottle Retail', types.FloatType()),
  types.StructField('Bottles Sold', types.IntegerType()),
  types.StructField('Sale (Dollars)', types.FloatType()),
  types.StructField('Volume Sold (Liters)', types.FloatType()),
  types.StructField('Volume Sold (Gallons)', types.FloatType())
])