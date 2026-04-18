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

sellersDf = spark.read.format("parquet").load(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/to_processed_data/to_processed_sellers_data"
)

sellersDf.write.format("delta").mode("overwrite").save(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/sellers")

# COMMAND ----------

sellersDF = spark.read.format("delta").load("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/sellers")

sellersDF = sellersDF \
    .withColumn("nbsellers", col("nbsellers").cast(IntegerType())) \
    .withColumn("meanproductssold", col("meanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("meanproductslisted", col("meanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meansellerpassrate", col("meansellerpassrate").cast(DecimalType(10, 2))) \
    .withColumn("totalproductssold", col("totalproductssold").cast(IntegerType())) \
    .withColumn("totalproductslisted", col("totalproductslisted").cast(IntegerType())) \
    .withColumn("meanproductsbought", col("meanproductsbought").cast(DecimalType(10, 2))) \
    .withColumn("meanproductswished", col("meanproductswished").cast(DecimalType(10, 2))) \
    .withColumn("meanproductsliked", col("meanproductsliked").cast(DecimalType(10, 2))) \
    .withColumn("totalbought", col("totalbought").cast(IntegerType())) \
    .withColumn("totalwished", col("totalwished").cast(IntegerType())) \
    .withColumn("totalproductsliked", col("totalproductsliked").cast(IntegerType())) \
    .withColumn("meanfollowers", col("meanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("meanfollows", col("meanfollows").cast(DecimalType(10, 2))) \
    .withColumn("percentofappusers", col("percentofappusers").cast(DecimalType(10, 2))) \
    .withColumn("percentofiosusers", col("percentofiosusers").cast(DecimalType(10, 2))) \
    .withColumn("meanseniority", col("meanseniority").cast(DecimalType(10, 2)))

# Normalize country names and gender values
sellersDF = sellersDF.withColumn("country", initcap(col("country"))) \
                                                .withColumn("sex", upper(col("sex")))


#Add a column to categorize the number of sellers
sellersDF = sellersDF.withColumn("seller_size_category", 
                               when(col("nbsellers") < 500, "Small") \
                               .when((col("nbsellers") >= 500) & (col("nbsellers") < 2000), "Medium") \
                               .otherwise("Large"))

# Calculate the mean products listed per seller as an indicator of seller activity
sellersDF = sellersDF.withColumn("mean_products_listed_per_seller", 
                               round(col("totalproductslisted") / col("nbsellers"), 2))

# Identify markets with high seller pass rate
sellersDF = sellersDF.withColumn("high_seller_pass_rate", 
                               when(col("meansellerpassrate") > 0.75, "High") \
                               .otherwise("Normal"))

mean_pass_rate = sellersDF.select(round(avg("meansellerpassrate"), 2).alias("avg_pass_rate")).collect()[0]["avg_pass_rate"]

sellersDF = sellersDF.withColumn("meansellerpassrate",
                                 when(col("meansellerpassrate").isNull(), mean_pass_rate)
                                 .otherwise(col("meansellerpassrate")))

sellersDF.write.format("delta").mode("overwrite").save("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/silver/sellers")