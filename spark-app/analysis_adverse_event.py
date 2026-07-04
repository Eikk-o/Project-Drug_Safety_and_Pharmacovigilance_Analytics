from pyspark.sql.functions import count, avg, desc


def most_frequent_adverse_events(df, n=10):
    return (df.groupBy("adverse_event")
              .agg(count("*").alias("num_reports"))
              .orderBy(desc("num_reports"))
              .limit(n))


def most_severe_adverse_events(df, n=10):
    return (df.groupBy("adverse_event")
              .agg(avg("seriousness_score").alias("avg_seriousness"),
                   count("*").alias("num_reports"))
              .orderBy(desc("avg_seriousness"))
              .limit(n))