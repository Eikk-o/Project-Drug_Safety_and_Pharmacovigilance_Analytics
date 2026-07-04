import time
import random
import csv
import os
from datetime import datetime

OUTPUT_DIR = "/opt/data/stream_input"

DRUGS = [("Metformin", "Antidiabetic"), ("Ibuprofen", "NSAID"), ("Amoxicillin", "Antibiotic")]
EVENTS = ["Nausea", "Diarrhea", "Gastrointestinal Bleeding", "Rash", "Headache"]
SEVERITIES = ["Mild", "Moderate", "Severe"]
OUTCOMES = ["Recovered", "Hospitalized", "Ongoing"]
AGE_GROUPS = ["Child", "Adult", "Senior"]
COUNTRIES = ["France", "Germany", "UK", "USA"]
MANUFACTURERS = ["PharmaA", "PharmaB", "PharmaC"]
SOURCES = ["Hospital", "Physician", "Patient"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


def generate_report(i):
    drug, drug_class = random.choice(DRUGS)
    severity = random.choice(SEVERITIES)
    base_score = {"Mild": 2, "Moderate": 5, "Severe": 8}[severity]
    seriousness = max(1, base_score + random.randint(-1, 1))
    return [
        f"S{i:05d}",
        datetime.now().strftime("%Y-%m-%d"),
        drug, drug_class,
        random.choice(EVENTS),
        severity,
        random.choice(OUTCOMES),
        random.choice(AGE_GROUPS),
        random.choice(COUNTRIES),
        random.choice(MANUFACTURERS),
        random.choice(SOURCES),
        seriousness
    ]


def main():
    i = 0
    while True:
        i += 1
        filename = os.path.join(OUTPUT_DIR, f"report_{i}.csv")
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["report_id", "report_date", "drug_name", "drug_class",
                              "adverse_event", "severity", "outcome", "age_group",
                              "country", "manufacturer", "source_type", "seriousness_score"])
            writer.writerow(generate_report(i))
        print(f"Written {filename}")
        time.sleep(5)


if __name__ == "__main__":
    main()