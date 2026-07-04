from pyspark.sql.functions import date_format, count, col


def monthly_report_evolution(df):
    return (df.withColumn("report_month", date_format("report_date", "yyyy-MM"))
              .groupBy("report_month")
              .agg(count("*").alias("num_reports"))
              .orderBy("report_month"))


def monthly_severe_evolution(df):
    return (df.filter(col("severity") == "Severe")
              .withColumn("report_month", date_format("report_date", "yyyy-MM"))
              .groupBy("report_month")
              .agg(count("*").alias("num_severe_reports"))
              .orderBy("report_month"))