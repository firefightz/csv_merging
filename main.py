import boto3
import json
import pandas as pd

PERSON_CSV = "person.csv"
SCHOOL_CSV = "school.csv"
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"

def load_data():
    persons = pd.read_csv(PERSON_CSV)
    schools = pd.read_csv(SCHOOL_CSV)

    # Join on school name
    merged = persons.merge(
        schools,
        left_on="school",
        right_on="name",
        how="left",
        suffixes=("", "_school")
    )

    return merged

def build_message(row):
    return {
        "name": row["name"],
        "weight": str(row["weight"]),
        "height": str(row["height"]),
        "school_name": row["name_school"],
        "school_address": row["address"],
        "school_city": row["city"]
    }

def send_to_sqs(messages):
    sqs = boto3.client("sqs")

    for msg in messages:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(msg)
        )
        print("Sent:", msg["name"])

def main():
    df = load_data()

    payloads = [build_message(row) for _, row in df.iterrows()]

    send_to_sqs(payloads)

if __name__ == "__main__":
    main()
