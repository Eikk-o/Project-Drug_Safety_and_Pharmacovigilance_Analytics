from pyspark.sql.functions import col, count, lit, when


def compute_prr(df, min_reports=1, prr_threshold=1.0):
    drug_totals = df.groupBy("drug_name").agg(count("*").alias("drug_total"))
    event_totals = df.groupBy("adverse_event").agg(count("*").alias("event_total"))
    total_reports = df.count()

    pair_counts = df.groupBy("drug_name", "adverse_event").agg(count("*").alias("pair_count"))

    joined = (pair_counts
              .join(drug_totals, "drug_name")
              .join(event_totals, "adverse_event")
              .withColumn("total_reports", lit(total_reports)))

    joined = (joined
              .withColumn("a", col("pair_count"))
              .withColumn("b", col("drug_total") - col("pair_count"))
              .withColumn("c", col("event_total") - col("pair_count"))
              .withColumn("d", col("total_reports") - col("drug_total") - col("event_total") + col("pair_count")))

    # Guard against division by zero (when b or d is 0, i.e. no "other" cases to compare against)
    joined = joined.withColumn(
        "prr",
        when((col("c") + col("d") == 0) | (col("c") == 0), None)
        .otherwise((col("a") / (col("a") + col("b"))) / (col("c") / (col("c") + col("d"))))
    )

    return (joined.filter((col("a") >= min_reports) & col("prr").isNotNull() & (col("prr") > prr_threshold))
                  .select("drug_name", "adverse_event", "a", "prr")
                  .orderBy(col("prr").desc()))


def hospitalization_signal(df):
    hosp = df.filter(col("outcome") == "Hospitalized")
    drug_totals = df.groupBy("drug_name").agg(count("*").alias("total_reports"))
    hosp_counts = hosp.groupBy("drug_name").agg(count("*").alias("hospitalizations"))

    return (hosp_counts.join(drug_totals, "drug_name")
            .withColumn("hospitalization_rate", col("hospitalizations") / col("total_reports"))
            .orderBy(col("hospitalization_rate").desc()))