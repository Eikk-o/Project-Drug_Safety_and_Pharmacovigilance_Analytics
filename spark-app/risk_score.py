from pyspark.sql.functions import count, avg, col, min as _min, max as _max, lit, rank
from pyspark.sql.window import Window


def compute_risk_score(df, w_reports=0.5, w_severity=0.5):
    agg = df.groupBy("drug_name").agg(
        count("*").alias("num_reports"),
        avg("severity_numeric").alias("avg_severity")
    )

    stats = agg.agg(
        _min("num_reports").alias("min_r"), _max("num_reports").alias("max_r"),
        _min("avg_severity").alias("min_s"), _max("avg_severity").alias("max_s")
    ).collect()[0]

    min_r, max_r = stats["min_r"], stats["max_r"]
    min_s, max_s = stats["min_s"], stats["max_s"]

    range_r = (max_r - min_r) if max_r != min_r else 1
    range_s = (max_s - min_s) if max_s != min_s else 1

    agg = (agg
           .withColumn("norm_reports", (col("num_reports") - lit(min_r)) / lit(range_r))
           .withColumn("norm_severity", (col("avg_severity") - lit(min_s)) / lit(range_s))
           .withColumn("risk_score",
                       lit(w_reports) * col("norm_reports") + lit(w_severity) * col("norm_severity")))

    window = Window.orderBy(col("risk_score").desc())
    return (agg.withColumn("risk_rank", rank().over(window))
               .select("drug_name", "num_reports", "avg_severity", "risk_score", "risk_rank")
               .orderBy("risk_rank"))