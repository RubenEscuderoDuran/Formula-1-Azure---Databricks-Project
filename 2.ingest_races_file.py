# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

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
# MAGIC # Step 1.- Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC Del comando anterior, observamos que la columna de los titulos los toma como si fueran datos, para ello, tenemos que colocar un comando para que considere esa columna como el titulo de las columnas.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType
 #Con esto podemos generar nuesto propio schema a partir de los datos.

# COMMAND ----------

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                    StructField('year', IntegerType(), True),
                                    StructField('round', IntegerType(), True),
                                    StructField('circuitId', IntegerType(), True),
                                    StructField('name', StringType(), True),
                                    StructField('date', DateType(), True),
                                    StructField('time', StringType(), False),
                                    StructField('url', StringType(), True)
 ])

# COMMAND ----------

races_df = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/races.csv')  #''schema(circuits_schema)'' le estamos diciendo que utilice el schema anterior generado por nosotros. Puede tambien ir otra opcion que es ''option('inferSchema', True)'' con esto le decimos que vaya a fondo en los datos y que nos diga su tipo de schema.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.- Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df) \
                                .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                .withColumn('data_source', lit(v_data_source)) \
                                .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.- Select columns required

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('ingestion_date'), col('race_timestamp'), col('data_source'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.- Rename the columns as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed('raceId', 'race_id') \
  .withColumnRenamed('year', 'race_year') \
  .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5.- Write Data to DataLake as parquet

# COMMAND ----------

races_renamed_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.races') #Con la funcion df.write.parquet() le especificamos que nuestro dataframe se guarde como archivo parquet en la ruta mencionada.


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.races;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6.- Partition by year

# COMMAND ----------

races_renamed_df.write.mode('overwrite').option('overwriteSchema', 'true').partitionBy('race_year').format('delta').save(f'{processed_folder_path}/races')

# COMMAND ----------

dbutils.notebook.exit('Success')