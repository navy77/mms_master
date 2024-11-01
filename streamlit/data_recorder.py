import pymssql
import time
import queue
from mqtt_subscriber import data_queue  # Import the queue from mqtt_subscriber

def record_to_mssql():
    while True:
        time.sleep(15)  # Wait 5 minutes
        if not data_queue.empty():
            try:
                # Connect to the MSSQL database
                # conn = pymssql.connect(
                #     server="your_server", 
                #     user="your_username", 
                #     password="your_password", 
                #     database="your_database"
                # )
                # cursor = conn.cursor()
                
                while not data_queue.empty():
                    # Get data from the queue
                    record = data_queue.get()
                    data = record['data']
                    timestamp = record['timestamp']

                    # # Insert data into MSSQL
                    # cursor.execute("INSERT INTO your_table (data, timestamp) VALUES (%s, %s)", (data, timestamp))
                    # conn.commit()
                    print(f"Recorded to MSSQL: {data} at {timestamp}")
                
                # conn.close()
            except Exception as e:
                print(f"Error recording to MSSQL: {e}")

# Run the data recording in the main thread
record_to_mssql()
