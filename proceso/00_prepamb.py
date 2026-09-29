# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "azdatasmart02")
dbutils.widgets.text("credencial", "azdbjuan02")
amb = dbutils.widgets.get("ambiente")
sa = dbutils.widgets.get("storage")
cred = dbutils.widgets.get("credencial")

# COMMAND ----------
for c in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS ext_{c}
                  URL 'abfss://{c}@{sa}.dfs.core.windows.net/'
                  WITH (STORAGE CREDENTIAL `{cred}`)""")

# COMMAND ----------
spark.sql(f"""CREATE CATALOG IF NOT EXISTS salud_{amb}
              MANAGED LOCATION 'abfss://gold@{sa}.dfs.core.windows.net/catalogs/{amb}'""")
for s in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS salud_{amb}.{s}")

# COMMAND ----------
display(spark.sql("SHOW EXTERNAL LOCATIONS"))
display(spark.sql(f"SHOW SCHEMAS IN salud_{amb}"))
