from data_loader import get_spark_session, load_and_clean
from analysis_drug import top_reported_drugs, reports_by_drug, severe_reports_by_drug
from analysis_adverse_event import most_frequent_adverse_events, most_severe_adverse_events
from analysis_temporal import monthly_report_evolution, monthly_severe_evolution
from risk_score import compute_risk_score
from signal_detection import compute_prr, hospitalization_signal
from mongo_writer import write_to_mongo

DATA_PATH = "/opt/data/dataset_project.csv"
MONGO_DB = "pharmacovigilance"


def show_section(title, df, n=20):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    df.show(n, truncate=False)


def main():
    spark = get_spark_session()
    df = load_and_clean(spark, DATA_PATH)
    df.cache()

    # ---------- Drug Analysis ----------
    top_drugs = top_reported_drugs(df)
    show_section("DRUG ANALYSIS - Top Reported Drugs", top_drugs)
    write_to_mongo(top_drugs, MONGO_DB, "top_drugs")

    reports_drug = reports_by_drug(df)
    show_section("DRUG ANALYSIS - Number of Reports by Drug", reports_drug)
    write_to_mongo(reports_drug, MONGO_DB, "reports_by_drug")

    severe_drug = severe_reports_by_drug(df)
    show_section("DRUG ANALYSIS - Number of Severe Reports by Drug", severe_drug)
    write_to_mongo(severe_drug, MONGO_DB, "severe_reports_by_drug")

    # ---------- Adverse Event Analysis ----------
    freq_events = most_frequent_adverse_events(df)
    show_section("ADVERSE EVENT ANALYSIS - Most Frequent Adverse Events", freq_events)
    write_to_mongo(freq_events, MONGO_DB, "frequent_adverse_events")

    severe_events = most_severe_adverse_events(df)
    show_section("ADVERSE EVENT ANALYSIS - Most Severe Adverse Events", severe_events)
    write_to_mongo(severe_events, MONGO_DB, "severe_adverse_events")

    # ---------- Temporal Analysis ----------
    monthly_reports = monthly_report_evolution(df)
    show_section("TEMPORAL ANALYSIS - Monthly Evolution of Reports", monthly_reports)
    write_to_mongo(monthly_reports, MONGO_DB, "monthly_reports")

    monthly_severe = monthly_severe_evolution(df)
    show_section("TEMPORAL ANALYSIS - Monthly Evolution of Severe Cases", monthly_severe)
    write_to_mongo(monthly_severe, MONGO_DB, "monthly_severe")

    # ---------- Risk Score Computation ----------
    risk = compute_risk_score(df)
    show_section("RISK SCORE - Drug Ranking by Estimated Risk", risk)
    write_to_mongo(risk, MONGO_DB, "risk_scores")

    # ---------- Signal Detection ----------
    signals = compute_prr(df)
    show_section("SIGNAL DETECTION - Disproportionately Associated Adverse Events (PRR)", signals)
    write_to_mongo(signals, MONGO_DB, "signals")

    hosp_signals = hospitalization_signal(df)
    show_section("SIGNAL DETECTION - Hospitalization Rate by Drug", hosp_signals)
    write_to_mongo(hosp_signals, MONGO_DB, "hospitalization_signals")

    print("\nAll analyses completed and written to MongoDB database:", MONGO_DB)
    spark.stop()


if __name__ == "__main__":
    main()