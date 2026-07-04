from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.functions import col, to_date, when


def get_spark_session(app_name="Pharmacovigilance"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_schema():
    return StructType([
        StructField("report_id", StringType(), True),
        StructField("report_date", StringType(), True),
        StructField("drug_name", StringType(), True),
        StructField("drug_class", StringType(), True),
        StructField("adverse_event", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("outcome", StringType(), True),
        StructField("age_group", StringType(), True),
        StructField("country", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("seriousness_score", IntegerType(), True),
    ])


def load_and_clean(spark, path):
    df = spark.read.csv(path, header=True, schema=get_schema())

    df = df.withColumn("report_date", to_date(col("report_date"), "yyyy-MM-dd"))

    df = df.withColumn(
        "severity_numeric",
        when(col("severity") == "Mild", 1)
        .when(col("severity") == "Moderate", 2)
        .when(col("severity") == "Severe", 3)
        .otherwise(0)
    )

    df = df.dropna(subset=["drug_name", "adverse_event"])
    df = df.dropDuplicates(["report_id"])
    return df