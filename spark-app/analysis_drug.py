from pyspark.sql.functions import count, desc, col


def top_reported_drugs(df, n=10):
    return (df.groupBy("drug_name")
              .agg(count("*").alias("num_reports"))
              .orderBy(desc("num_reports"))
              .limit(n))


def reports_by_drug(df):
    return (df.groupBy("drug_name")
              .agg(count("*").alias("num_reports"))
              .orderBy(desc("num_reports")))


def severe_reports_by_drug(df):
    return (df.filter(col("severity") == "Severe")
              .groupBy("drug_name")
              .agg(count("*").alias("num_severe_reports"))
              .orderBy(desc("num_severe_reports")))