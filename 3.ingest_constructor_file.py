# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 1.- Read the JSON file using the spark dataframe reader 

# COMMAND ----------

## Usaremos el método DDL para definir el schema de los datos
constructor_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructor_schema) \
    .json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.- Drop unwanted columns from the dataframe

# COMMAND ----------

#Esta es una manera de hacerlo.
constructor_dropped_df = constructor_df.drop('url')

#Otra manera de hacerlo es usando el nombre del dataframe
#constructor_droppped_df = constructor_df.drop(constructor_df.url)

#o la tercera con la funcion col 
#from pyspark.sql.functions import col 
#constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.- Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df) \
                                              .withColumnRenamed('constructorId', 'constructor_id') \
                                              .withColumnRenamed('constructorRef', 'constructor_ref') \
                                              .withColumn('data_source', lit(v_data_source)) \
                                              .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4.- Write output to parquet files 

# COMMAND ----------


constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit('Success')