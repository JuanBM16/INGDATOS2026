# Databricks notebook source
# MAGIC %run ./00_funciones

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "azdatasmart03")
amb = dbutils.widgets.get("ambiente")
sa = dbutils.widgets.get("storage")

# COMMAND ----------
archivo = "Hospital_General_Information.csv"
df = normalizar_columnas(leer_csv(spark, f"abfss://raw@{sa}.dfs.core.windows.net/{archivo}"))
escribir_delta(agregar_auditoria(df, archivo), f"salud_{amb}", "bronze",
               "hospitales", "bronze", sa, amb)

# COMMAND ----------
display(spark.table(f"salud_{amb}.bronze.hospitales").limit(5))
