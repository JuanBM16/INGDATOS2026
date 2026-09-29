# Databricks notebook source
# MAGIC %run ./00_funciones

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "azdatasmart03")
amb = dbutils.widgets.get("ambiente")
sa = dbutils.widgets.get("storage")

# COMMAND ----------
cols = ["name", "age", "gender", "blood_type", "medical_condition",
        "date_of_admission", "doctor", "hospital", "insurance_provider",
        "billing_amount", "room_number", "admission_type", "discharge_date",
        "medication", "test_results"]
esquema = StructType([StructField(c, StringType()) for c in cols])

archivo = "healthcare_dataset.csv"
df = leer_csv(spark, f"abfss://raw@{sa}.dfs.core.windows.net/{archivo}", esquema)
escribir_delta(agregar_auditoria(df, archivo), f"salud_{amb}", "bronze",
               "admisiones", "bronze", sa, amb)

# COMMAND ----------
display(spark.table(f"salud_{amb}.bronze.admisiones").limit(5))
