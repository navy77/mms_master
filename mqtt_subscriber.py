import paho.mqtt.client as mqtt
import pymssql
import threading
import queue
import time
from datetime import datetime

data_queue = queue.Queue()

def on_message(client, userdata, message):
    payload = message.payload.decode()
    # Add timestamp to the message for logging purposes
    data_queue.put({"data": payload, "timestamp": datetime.now()})
    print(f"Received data: {payload}")
    print(data_queue)

# Function to handle MQTT subscription
def mqtt_subscribe():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect("192.168.0.160", 1883, 60)
    client.subscribe("status/#")
    client.loop_forever() 

# Function to record data to MSSQL every 5 minutes
def record_to_mssql():
    while True:
        time.sleep(10)  # Wait 5 minutes
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

                    # Insert data into MSSQL
                    # cursor.execute("INSERT INTO your_table (data, timestamp) VALUES (%s, %s)", (data, timestamp))
                    # conn.commit()
                    print(f"Recorded to MSSQL: {data} at {timestamp}")
                
                # conn.close()
            except Exception as e:
                print(f"Error recording to MSSQL: {e}")

# Run MQTT subscription and data recording in separate threads
mqtt_thread = threading.Thread(target=mqtt_subscribe)
record_thread = threading.Thread(target=record_to_mssql)

mqtt_thread.start()
record_thread.start()

# Keep the main thread running
mqtt_thread.join()
record_thread.join()
