import psycopg2
from faker import Faker
import random
from datetime import datetime

# --- Configuration ---
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "user": "testuser",
    "password": "testpass"
}

# --- Initialize Faker ---
faker = Faker()

# --- Connect to Postgres ---
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# --- Reset tables ---
cur.execute('DROP TABLE IF EXISTS bundle CASCADE;')
cur.execute('DROP TABLE IF EXISTS things CASCADE;')
conn.commit()

# --- Create tables ---
cur.execute("""
CREATE TABLE bundle (
    bundle_name TEXT PRIMARY KEY,
    bundle_address TEXT,
    created TIMESTAMP,
    type TEXT,
    active BOOLEAN,
    deactivated TIMESTAMP,
    bundle_list TEXT,
    bundle_update_frequency INT
);
""")

cur.execute("""
CREATE TABLE things (
    thing_url TEXT PRIMARY KEY,
    update_frequency INT
);
""")

conn.commit()

# --- Insert mock data ---

# Insert 5 mock bundles
for _ in range(5):
    bundle_name = faker.company()
    bundle_address = faker.address().replace("\n", ", ")
    created = faker.date_time_between(start_date='-2y', end_date='now')
    type_ = random.choice(["A", "B", "C"])
    active = random.choice([True, False])
    deactivated = faker.date_time_between(start_date=created, end_date='now') if not active else None
    bundle_list = ",".join([faker.word() for _ in range(3)])
    bundle_update_frequency = random.choice([1, 7, 30])
    
    cur.execute("""
    INSERT INTO bundle (bundle_name, bundle_address, created, type, active, deactivated, bundle_list, bundle_update_frequency)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (bundle_name, bundle_address, created, type_, active, deactivated, bundle_list, bundle_update_frequency))

# Insert 10 mock things
for _ in range(10):
    thing_url = faker.url()
    update_frequency = random.choice([1, 7, 30])
    
    cur.execute("""
    INSERT INTO things (thing_url, update_frequency)
    VALUES (%s, %s);
    """, (thing_url, update_frequency))

conn.commit()

# --- Verify data ---
cur.execute('SELECT * FROM bundle LIMIT 5;')
print("Bundles sample:")
for row in cur.fetchall():
    print(row)

cur.execute('SELECT * FROM things LIMIT 5;')
print("\nThings sample:")
for row in cur.fetchall():
    print(row)

# --- Close connection ---
cur.close()
conn.close()
