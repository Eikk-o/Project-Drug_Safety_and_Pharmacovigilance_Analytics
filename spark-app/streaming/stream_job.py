from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.functions import col


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


def write_batch_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.write.format("mongodb") \
        .mode("append") \
        .option("connection.uri", "mongodb://mongo:27017") \
        .option("database", "pharmacovigilance") \
        .option("collection", "live_reports") \
        .save()


def main():
    spark = SparkSession.builder.appName("PharmacovigilanceStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stream_df = (spark.readStream
                 .option("header", True)
                 .schema(get_schema())
                 .csv("/opt/data/stream_input"))

    query = (stream_df.writeStream
             .foreachBatch(write_batch_to_mongo)
             .outputMode("append")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()