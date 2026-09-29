   # Databricks notebook source
# Databricks notebook source
import re
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

def a_snake(nombre):
    return re.sub(r"[^0-9a-zA-Z]+", "_", nombre.strip()).strip("_").lower()

def normalizar_columnas(df):
    return df.toDF(*[a_snake(c) for c in df.columns])

def agregar_auditoria(df, archivo):
    return (df.withColumn("fecha_ingesta", F.current_timestamp())
              .withColumn("archivo_origen", F.lit(archivo)))

def leer_csv(spark, ruta, esquema=None):
    r = (spark.read.option("header", True)
                   .option("multiLine", True)
                   .option("escape", '"'))
    return (r.schema(esquema) if esquema else r).csv(ruta)

def escribir_delta(df, catalog, schema, tabla, contenedor, storage, ambiente):
    ruta = f"abfss://{contenedor}@{storage}.dfs.core.windows.net/{ambiente}/{tabla}"
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .option("path", ruta)
       .saveAsTable(f"{catalog}.{schema}.{tabla}"))
