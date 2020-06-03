# Databricks notebook source
from os import listdir
from os.path import isfile, join
from pyspark import SparkConf, SparkContext

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/dataintelligence/Nicholas/Asegura2/salida")

# COMMAND ----------

full = spark.read.format("csv")\
        .option("header", "true")\
        .option("delimiter", ";")\
        .load(r"/mnt/dataintelligence/Nicholas/Asegura2/salida/Asegura_v2.csv")

# COMMAND ----------

full.columns

# COMMAND ----------

full.createOrReplaceTempView("full")
base_final = spark.sql("""                                        
            select 
            f.CO01MOR070CC,count(f.CO01MOR070CC)--,
            --f.CO01ACP040CC,count(f.CO01ACP040CC)
            --f.CO02NUM042IN,count(f.CO02NUM042IN)
            from full as f 
            where f.bgi = 1
             group by f.CO01MOR070CC--,f.CO01ACP040CC--,f.CO01MOR070CC,f.CO02NUM042IN
             order by 1 desc             
           """).show()

# COMMAND ----------

full.createOrReplaceTempView("full")
base_final = spark.sql("""                                        
            select             
            f.CO02NUM042IN,count(f.CO02NUM042IN)
            from full as f 
            where f.bgi = 1
             group by f.CO02NUM042IN--,f.CO01MOR070CC,f.CO02NUM042IN
             order by 1 desc             
           """).show()

# COMMAND ----------

a = pd.read_csv('dbfs:/mnt/dataintelligence/Nicholas/Asegura2/salida/Asegura_v2.csv', sep=';', header=0)