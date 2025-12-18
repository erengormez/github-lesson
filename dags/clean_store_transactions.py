from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


spark = (
SparkSession.builder
.appName("CleanStoreTransactions")
.config("spark.jars.packages",
"org.postgresql:postgresql:42.7.3")
.getOrCreate()
)


# MinIO config
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9001")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "dataopsadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "dataopsadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


# Read raw data
df = spark.read.option("header", True).csv(
"s3a://dataops-bronze/raw/dirty_store_transactions.csv"
)


# Basic cleaning
df_clean = (
df.dropDuplicates()
.dropna(how="all")
.select([
trim(col(c)).alias(c) for c in df.columns
])
)


# Write to Postgres
(
df_clean.write
.format("jdbc")
.option("url", "jdbc:postgresql://postgres:5432/traindb")
.option("dbtable", "public.clean_data_transactions")
.option("user", "airflow")
.option("password", "airflow")
.option("driver", "org.postgresql.Driver")
.mode("overwrite")
.save()
)


spark.stop()
