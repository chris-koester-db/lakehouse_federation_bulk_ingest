# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Synthetic Source Data 

# COMMAND ----------

# MAGIC %pip install dbldatagen==0.4.0

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text('catalog', 'lakefed_bulk_ingest', '01 Catalog')
dbutils.widgets.text('schema', 'default', '02 Schema')
dbutils.widgets.text('src_table', 'lakefed_src', '03 Source Table')
dbutils.widgets.text('row_count', '25000000', '04 Row Count')
dbutils.widgets.text('partitions', '32', '05 Partitions')

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

# https://databrickslabs.github.io/dbldatagen/public_docs/generating_cdc_data.html

def create_dataspec(row_count, partitions):
    
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
    
    dataspec = (
        dg.DataGenerator(spark, rows=row_count, partitions=partitions)
          .withColumn("customer_id","long", uniqueValues=row_count)
          .withColumn("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
          .withColumn("alias", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
          .withColumn("payment_instrument_type", values=['paypal', 'Visa', 'Mastercard',
                      'American Express', 'discover', 'branded visa', 'branded mastercard'],
                      random=True, distribution="normal")
          .withColumn("int_payment_instrument", "int",  minValue=0000, maxValue=9999,
                      baseColumn="customer_id", baseColumnType="hash", omit=True)
          .withColumn("payment_instrument",
                      expr="format_number(int_payment_instrument, '**** ****** *####')",
                      baseColumn="int_payment_instrument")
          .withColumn("email", template=r'\\w.\\w@\\w.com|\\w-\\w@\\w')
          .withColumn("email2", template=r'\\w.\\w@\\w.com')
          .withColumn("ip_address", template=r'\\n.\\n.\\n.\\n')
          .withColumn("md5_payment_instrument",
                      expr="md5(concat(payment_instrument_type, ':', payment_instrument))",
                      base_column=['payment_instrument_type', 'payment_instrument'])
          .withColumn("customer_notes", text=dg.ILText(words=(1,8)))
          .withColumn("created_ts", "timestamp", expr="now()")
          .withColumn("modified_ts", "timestamp", expr="now()")
          .withColumn("memo", expr="'original data'")
          .withColumn(
              "ts_col",
              "timestamp",
              data_range=dg.DateRange("2023-01-01 00:00:00", "2023-12-31 11:55:00", "seconds=8"),
              baseColumn="customer_id",
          )
          .withColumn(
              "date_col",
              "date",
              expr="cast(ts_col as date)",
              baseColumn="customer_id",
          )
      )
    
    return dataspec

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
src_table = dbutils.widgets.get('src_table')

spark.sql(f'use catalog {catalog}')
spark.sql(f'create schema if not exists {schema}')

# Set partitions to 1x or 2x number of cores
dataspec = create_dataspec(row_count=25_000_000, partitions=32)
df = dataspec.build()
df.writeTo(f"{catalog}.{schema}.{src_table}").using("delta").clusterBy("customer_id").createOrReplace()

# COMMAND ----------

display(spark.sql(f'optimize {catalog}.{schema}.{src_table}'))