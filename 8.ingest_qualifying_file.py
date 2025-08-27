# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest quealifyng folder
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 1.- Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run ../includes/configuration 

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text('p_data_source', 'testing')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('q1', StringType(), True),
                                    StructField('q2', StringType(), True),
                                    StructField('q3', StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option('multiLine', True) \
.json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.- Rename columns and add new columns 
# MAGIC 1. Renamed qualifyingId, raceId, driverId, constructorId
# MAGIC 2. Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit 

# COMMAND ----------

final_df = add_ingestion_date(qualifying_df) \
                       .withColumnRenamed('qualifyId', 'qualify_id') \
                       .withColumnRenamed('driverId', 'driver_id') \
                       .withColumnRenamed('raceId', 'race_id') \
                       .withColumnRenamed('constructorId', 'constructor_id') \
                       .withColumn('data_source', lit(v_data_source)) \
                       .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.- Write to output tp processed container in parquet format

# COMMAND ----------

re_arrange_partition_column(final_df,'race_id')

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = 'tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id'
merge_delta(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql select * from f1_processed.qualifying