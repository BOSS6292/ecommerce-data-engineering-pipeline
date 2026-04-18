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

countriesDf = spark.read.format("parquet").load(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/to_processed_data/to_processed_countries_data"
)

countriesDf.write.format("delta").mode("overwrite").save(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/countries")

# COMMAND ----------

countriesDF = spark.read.format("delta").load("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/countries")

# COMMAND ----------

countriesDF = countriesDF \
    .withColumn("sellers", col("sellers").cast(IntegerType())) \
    .withColumn("topsellers", col("topsellers").cast(IntegerType())) \
    .withColumn("topsellerratio", col("topsellerratio").cast(DecimalType(10, 2))) \
    .withColumn("femalesellersratio", col("femalesellersratio").cast(DecimalType(10, 2))) \
    .withColumn("topfemalesellersratio", col("topfemalesellersratio").cast(DecimalType(10, 2))) \
    .withColumn("femalesellers", col("femalesellers").cast(IntegerType())) \
    .withColumn("malesellers", col("malesellers").cast(IntegerType())) \
    .withColumn("topfemalesellers", col("topfemalesellers").cast(IntegerType())) \
    .withColumn("topmalesellers", col("topmalesellers").cast(IntegerType())) \
    .withColumn("countrysoldratio", col("countrysoldratio").cast(DecimalType(10, 2))) \
    .withColumn("bestsoldratio", col("bestsoldratio").cast(DecimalType(10, 2))) \
    .withColumn("toptotalproductssold", col("toptotalproductssold").cast(IntegerType())) \
    .withColumn("totalproductssold", col("totalproductssold").cast(IntegerType())) \
    .withColumn("toptotalproductslisted", col("toptotalproductslisted").cast(IntegerType())) \
    .withColumn("totalproductslisted", col("totalproductslisted").cast(IntegerType())) \
    .withColumn("topmeanproductssold", col("topmeanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("topmeanproductslisted", col("topmeanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meanproductssold", col("meanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("meanproductslisted", col("meanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meanofflinedays", col("meanofflinedays").cast(DecimalType(10, 2))) \
    .withColumn("topmeanofflinedays", col("topmeanofflinedays").cast(DecimalType(10, 2))) \
    .withColumn("meanfollowers", col("meanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("meanfollowing", col("meanfollowing").cast(DecimalType(10, 2))) \
    .withColumn("topmeanfollowers", col("topmeanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("topmeanfollowing", col("topmeanfollowing").cast(DecimalType(10, 2)))

countriesDF = countriesDF.withColumn("country", initcap(col("country")))

# Calculating the ratio of top sellers to total sellers
countriesDF = countriesDF.withColumn("top_seller_ratio", 
                                        round(col("topsellers") / col("sellers"), 2))

# countriesDF countries with a high ratio of female sellers
countriesDF = countriesDF.withColumn("high_female_seller_ratio", 
                                        when(col("femalesellersratio") > 0.5, True).otherwise(False))

# Adding a performance indicator based on the sold/listed ratio
countriesDF = countriesDF.withColumn("performance_indicator", 
                                        round(col("toptotalproductssold") / (col("toptotalproductslisted") + 1), 2))

# Flag countries with exceptionally high performance
performance_threshold = 0.8
countriesDF = countriesDF.withColumn("high_performance", 
                                        when(col("performance_indicator") > performance_threshold, True).otherwise(False))

countriesDF = countriesDF.withColumn("activity_level",
                                       when(col("meanofflinedays") < 30, "Highly Active")
                                       .when((col("meanofflinedays") >= 30) & (col("meanofflinedays") < 60), "Moderately Active")
                                       .otherwise("Low Activity"))


countriesDF.write.format("delta").mode("overwrite").save("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/silver/countries")