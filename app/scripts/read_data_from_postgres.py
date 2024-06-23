import os
import psycopg2
import time

# Database connection parameters
db_params = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}

# Connect to the database
conn = psycopg2.connect(**db_params)

# Start timing
start_time = time.time()

# Fetch records using a server-side cursor
with conn.cursor() as cursor:
    # cursor.execute("TRUNCATE stock_data")
    # cursor.execute("ALTER TABLE stock_data DROP CONSTRAINT IF EXISTS stock_data_pkey;")
    # cursor.execute("ALTER TABLE stock_data ADD PRIMARY KEY (trackingID);")
    cursor.execute("select * from stock_data limit 5")
    records = cursor.fetchall()  # Fetch all the records
    print(records)

# End timing
end_time = time.time()
total_time = end_time - start_time

print(f"Total time taken to fetch records: {total_time:.2f} seconds")

# Print the results
for record in records:
    print(record)

# Close the connection
conn.close()
