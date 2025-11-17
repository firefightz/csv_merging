import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

INPUT_FILE = "data.txt"

def main():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        with open(INPUT_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                name, group, token = line.split()

                query = sql.SQL("""
                    INSERT INTO mytable (name, server_group, token)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name, server_group)
                    DO UPDATE SET token = EXCLUDED.token
                """)

                cur.execute(query, (name, group, token))
                print(f"Updated DB → name={name}, group={group}, token={token}")

        conn.commit()
        print("All records processed.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
