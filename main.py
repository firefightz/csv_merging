import pandas as pd
import boto3
import json

# 1. Read CSV files
persons_df = pd.read_csv("person.csv")  # name, weight, height, school
schools_df = pd.read_csv("school.csv")  # name, address, city

# 2. Merge CSVs on the school name
merged_df = pd.merge(
    persons_df, 
    schools_df, 
    left_on="school", 
    right_on="name", 
    suffixes=('_person', '_school')
)

# 3. Create flat JSON messages
# Select and rename columns
merged_df = merged_df.rename(columns={
    "name_person": "name",
    "weight": "weight",
    "name_school": "school_name",
    "address": "school_address"
})

# Only keep the needed columns
final_df = merged_df[["name", "weight", "school_name", "school_address"]]

# Convert each row to JSON and send to SQS
sqs = boto3.client('sqs', region_name='us-east-1')  # replace with your region
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/your-queue-name"  # replace with your queue URL

for _, row in final_df.iterrows():
    msg = row.to_dict()
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(msg)
    )
    print(f"Sent message ID: {response['MessageId']}")
