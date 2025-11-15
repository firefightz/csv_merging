import pytest
import pandas as pd
import json
from moto import mock_aws  # Moto 5 unified mock
import boto3
from io import StringIO

# --- Sample CSV data ---
PERSON_CSV = """name,weight,height,school
Alice,120,5.5,Springfield High
Bob,150,5.9,Shelby High
"""

SCHOOL_CSV = """name,address,city
Springfield High,123 Main St,Springfield
Shelby High,456 Elm St,Shelbyville
"""

# --- Function to test ---
def process_and_send_to_sqs(person_csv, school_csv, queue_url, region="us-east-1"):
    persons_df = pd.read_csv(StringIO(person_csv))
    schools_df = pd.read_csv(StringIO(school_csv))

    merged_df = pd.merge(
        persons_df,
        schools_df,
        left_on="school",
        right_on="name",
        suffixes=('_person', '_school')
    )

    merged_df = merged_df.rename(columns={
        "name_person": "name",
        "weight": "weight",
        "name_school": "school_name",
        "address": "school_address"
    })

    final_df = merged_df[["name", "weight", "school_name", "school_address"]]

    sqs = boto3.client("sqs", region_name=region)
    message_ids = []
    for _, row in final_df.iterrows():
        msg = row.to_dict()
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(msg)
        )
        message_ids.append(response["MessageId"])
    return message_ids, final_df

# --- Unit tests ---
@mock_aws
def test_sqs_messages_sent_and_flat_json():
    # Create mocked SQS
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue["QueueUrl"]

    # Run function
    message_ids, final_df = process_and_send_to_sqs(PERSON_CSV, SCHOOL_CSV, queue_url)

    # Assert messages were sent
    assert len(message_ids) == 2

    # Assert merged dataframe has correct columns
    expected_columns = ["name", "weight", "school_name", "school_address"]
    assert list(final_df.columns) == expected_columns

    # Assert JSON messages are flat
    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get("Messages", [])
    for msg in messages:
        body = json.loads(msg["Body"])
        assert set(body.keys()) == set(expected_columns)

    # Check one message content
    first_msg = json.loads(messages[0]["Body"])
    assert first_msg["name"] == "Alice"
    assert first_msg["school_name"] == "Springfield High"
    assert first_msg["school_address"] == "123 Main St"
    assert first_msg["weight"] == 120

if __name__ == "__main__":
    pytest.main([__file__])
