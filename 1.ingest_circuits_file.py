# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run ../includes/configuration 

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 1.- Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC Del comando anterior, observamos que la columna de los titulos los toma como si fueran datos, para ello, tenemos que colocar un comando para que considere esa columna como el titulo de las columnas.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType #Con esto podemos generar nuesto propio schema a partir de los datos.

# COMMAND ----------

circuits_schema = StructType(fields=[StructField('circuit_Id', IntegerType(), False),
                                    StructField('circuitRef', StringType(), True),
                                    StructField('name', StringType(), True),
                                    StructField('location', StringType(), True),
                                    StructField('country', StringType(), True),
                                    StructField('lat', DoubleType(), True),
                                    StructField('lng', DoubleType(), True),
                                    StructField('alt', IntegerType(), True),
                                    StructField('url', StringType(), True)
 ])

# COMMAND ----------

circuits_df = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')    #''schema(circuits_schema)'' le estamos diciendo que utilice el schema anterior generado por nosotros. Puede tambien ir otra opcion que es ''option('inferSchema', True)'' con esto le decimos que vaya a fondo en los datos y que nos diga su tipo de schema.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.- Select only the required columns 

# COMMAND ----------

circuits_selected_df = circuits_df.select('circuit_Id', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt') # Con esta opción solo podemos seleccionar las columnas que queremos ver, pero si queremos hacer alguna modificación a ciertas columnas no se puede hacer, por eso es recomendable trabajar con alguna de las 3 formas de abajo mencionadas.


# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuit_Id, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df['circuit_Id'], circuits_df['circuitRef'], 
                                          circuits_df['name'], circuits_df['location'], circuits_df['country'], 
                                          circuits_df['lat'], circuits_df['lng'], circuits_df['alt'])

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuit_Id'), col('circuitRef'), col('name'), 
                                          col('location'), col('country'), col('lat'), col('lng'), col('alt')) #A lo largo del proyecto nos centraremos en esta forma para seleccionar las columnas del DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.- Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
  .withColumnRenamed('circuitRef', 'circuit_ref') \
  .withColumnRenamed('lat', 'latitude') \
  .withColumnRenamed('lng', 'longitude') \
  .withColumnRenamed('alt', 'altitude') \
  .withColumn('data_source', lit(v_data_source)) \
  .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4.- Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df) #withColumn es un comando para añadir columnas al dataframe, nos pide dos valores, el nombre de la columna y el tipo de datos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5.- Write Data to DataLake as parquet

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits') #Con la funcion df.write.parquet() le especificamos que nuestro dataframe se guarde como archivo parquet en la ruta mencionada.


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit('Success')