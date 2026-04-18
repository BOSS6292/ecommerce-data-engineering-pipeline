# Databricks notebook source
storage_account = "Your Storage Account nam"
container = "Your Storage Container Name"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")

spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
"Enter your client id")

spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
"Enter your client secret id")

spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
"Enter your Client Endpoint")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("EcomDataPipeline").getOrCreate()

# COMMAND ----------

buyersDf = spark.read.format("parquet").load(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/to_processed_data/to_processed_buyers_data"
)

buyersDf.write.format("delta").mode("overwrite").save(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/buyers")

# COMMAND ----------

buyersDF = spark.read.format("delta").load("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/buyers")

# Casting Integer columns
integer_columns = [
    'buyers', 'topbuyers', 'femalebuyers', 'malebuyers',
    'topfemalebuyers', 'topmalebuyers', 'totalproductsbought',
    'totalproductswished', 'totalproductsliked', 'toptotalproductsbought',
    'toptotalproductswished', 'toptotalproductsliked'
]

for column_name in integer_columns:
    buyersDF = buyersDF.withColumn(column_name, col(column_name).cast(IntegerType()))

# Casting Decimal columns
decimal_columns = [
    'topbuyerratio', 'femalebuyersratio', 'topfemalebuyersratio',
    'boughtperwishlistratio', 'boughtperlikeratio', 'topboughtperwishlistratio',
    'topboughtperlikeratio', 'meanproductsbought', 'meanproductswished',
    'meanproductsliked', 'topmeanproductsbought', 'topmeanproductswished',
    'topmeanproductsliked', 'meanofflinedays', 'topmeanofflinedays',
    'meanfollowers', 'meanfollowing', 'topmeanfollowers', 'topmeanfollowing'
]

for column_name in decimal_columns:
    buyersDF = buyersDF.withColumn(column_name, col(column_name).cast(DecimalType(10, 2)))

# Normalize country names
buyersDF = buyersDF.withColumn("country", initcap(col("country")))

for col_name in integer_columns:
    buyersDF = buyersDF.fillna({col_name: 0})

# Calculate the ratio of female to male buyers
buyersDF = buyersDF.withColumn("female_to_male_ratio", 
                               round(col("femalebuyers") / (col("malebuyers") + 1), 2))

# Determine the market potential by comparing wishlist and purchases
buyersDF = buyersDF.withColumn("wishlist_to_purchase_ratio", 
                               round(col("totalproductswished") / (col("totalproductsbought") + 1), 2))

# Tag countries with a high engagement ratio
high_engagement_threshold = 0.5
buyersDF = buyersDF.withColumn("high_engagement",
                               when(col("boughtperwishlistratio") > high_engagement_threshold, True)
                               .otherwise(False))
                               
    # Flag markets with increasing female buyer participation
buyersDF = buyersDF.withColumn("growing_female_market",
                               when(col("femalebuyersratio") > col("topfemalebuyersratio"), True)
                               .otherwise(False))

buyersDF.write.format("delta").mode("overwrite").save("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/silver/buyers")