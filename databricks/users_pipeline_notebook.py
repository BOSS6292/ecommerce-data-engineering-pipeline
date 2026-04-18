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

userDf = spark.read.format("parquet").load(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/to_processed_data/to_processed_user_data"
)

userDf.write.format("delta").mode("overwrite").save(
"abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/users")

# COMMAND ----------

usersDF = spark.read.format("delta").load("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/bronze/users")

usersDF = usersDF.withColumn("countrycode", upper(col("countrycode")))

usersDF = usersDF.withColumn("language_full", 
                             expr("CASE WHEN language = 'EN' THEN 'English' " +
                                  "WHEN language = 'FR' THEN 'French' " +
                                  "ELSE 'Other' END"))

usersDF = usersDF.withColumn("gender", 
                             when(col("gender").startswith("M"), "Male")
                             .when(col("gender").startswith("F"), "Female")
                             .otherwise("Other"))

usersDF = usersDF.withColumn("civilitytitle_clean", 
                             regexp_replace("civilitytitle", "(Mme|Ms|Mrs)", "Ms"))

usersDF = usersDF.withColumn("years_since_last_login", col("dayssincelastlogin") / 365)

usersDF = usersDF.withColumn("account_age_years", round(col("seniority") / 365, 2))
usersDF = usersDF.withColumn("account_age_group",
                             when(col("account_age_years") < 1, "New")
                             .when((col("account_age_years") >= 1) & (col("account_age_years") < 3), "Intermediate")
                             .otherwise("Experienced"))

usersDF = usersDF.withColumn("current_year", year(current_date()))

usersDF = usersDF.withColumn("user_descriptor", 
                             concat(col("gender"), lit("_"), 
                                    col("countrycode"), lit("_"), 
                                    expr("substring(civilitytitle_clean, 1, 3)"), lit("_"), 
                                    col("language_full")))

usersDF = usersDF.withColumn("flag_long_title", length(col("civilitytitle")) > 10)
usersDF = usersDF.withColumn("hasanyapp", col("hasanyapp").cast("boolean"))
usersDF = usersDF.withColumn("hasandroidapp", col("hasandroidapp").cast("boolean"))
usersDF = usersDF.withColumn("hasiosapp", col("hasiosapp").cast("boolean"))
usersDF = usersDF.withColumn("hasprofilepicture", col("hasprofilepicture").cast("boolean"))
usersDF = usersDF.withColumn("socialnbfollowers", col("socialnbfollowers").cast(IntegerType()))
usersDF = usersDF.withColumn("socialnbfollows", col("socialnbfollows").cast(IntegerType()))
usersDF = usersDF.withColumn("productspassrate", col("productspassrate").cast(DecimalType(10, 2)))
usersDF = usersDF.withColumn("seniorityasmonths", col("seniorityasmonths").cast(DecimalType(10, 2)))
usersDF = usersDF.withColumn("seniorityasyears", col("seniorityasyears").cast(DecimalType(10, 2)))

usersDF = usersDF.withColumn("dayssincelastlogin",
                             when(col("dayssincelastlogin").isNotNull(),
                                  col("dayssincelastlogin").cast(IntegerType()))
                             .otherwise(0))

usersDF.write.format("delta").mode("overwrite").save("abfss://landing-zone-2@ecomstorageaccount.dfs.core.windows.net/delta/tables/silver/users")