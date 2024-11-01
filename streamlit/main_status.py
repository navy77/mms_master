import paho.mqtt.client as mqtt
import pandas as pd
import threading
import pymssql
import json
from datetime import datetime
import sys
from dotenv import load_dotenv
import os
load_dotenv()

server = os.getenv('SERVER')
username = os.getenv('USER_LOGIN')
password = os.getenv('PASSWORD')
database = os.getenv('DATABASE')
table = os.getenv('TABLE_2')

mqtt_broker = os.getenv('MQTT_BROKER')
mqtt_port = int(os.getenv('MQTT_PORT'))
mqtt_topic = "status/#"

msg_buffer = []
buffer_lock = threading.Lock()



def conn_sql():
    try:
        cnxn = pymssql.connect(server,username,password,database)
        cursor = cnxn.cursor()
        return cnxn,cursor
    except Exception as e:
        sys.exit()

# Function to record data to MSSQL
def record_data_to_mssql(df):
    df = df[['occurred', 'topic', 'mc_status']]
    df_split = df['topic'].str.split('/', expand=True)
    df['mc_no'] = df_split[3].values
    df['process'] = df_split[2].values
    df = df[['occurred','mc_status','mc_no','process']]
    mcstatus_list = ['occurred','mc_status','mc_no','process']
    cnxn,cursor = conn_sql()
    try:
        for index, row in df.iterrows():
            value = None
            for i in range(len(mcstatus_list)):
                address = mcstatus_list[i]
                if value == None:
                    value = ",'"+str(row[address])+"'"
                else:
                    value = value+",'"+str(row[address])+"'"
            
            insert_string = f"""
                    INSERT INTO [{database}].[dbo].[{table}] 
                    values(
                        getdate()
                        {value}
                        )
                        """
            print(insert_string)
            cursor.execute(insert_string)
            cnxn.commit()
        cursor.close()
    except Exception as e:
        print('error: '+str(e))
    # for _, row in df.iterrows():
    #     cursor.execute("INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)", (row['column1'], row['column2'], ...))
    # conn.commit()
    print("Data recorded to MSSQL")

def save_to_database():
    while True:
        if len(msg_buffer) >= 5:
            with buffer_lock:
                df = pd.DataFrame(msg_buffer)
                msg_buffer.clear()
            record_data_to_mssql(df)


def on_message(client, userdata, message):
    with buffer_lock:
        payload = json.loads(message.payload.decode())
        msg_buffer.append({
            "occurred": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp
            "topic": message.topic,
            "mc_status": payload.get("status")  # Extract "status" key from JSON payload
        })

def subscribe_mqtt():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect("192.168.0.160", 1883, 60)
    client.subscribe("status/#")
    client.loop_forever()

# Main entry point
if __name__ == "__main__":
    # Start the database saving thread
    db_thread = threading.Thread(target=save_to_database, daemon=True)
    db_thread.start()

    # Start the MQTT subscribe thread
    mqtt_thread = threading.Thread(target=subscribe_mqtt, daemon=True)
    mqtt_thread.start()

    # Keep the main thread alive to allow threads to continue running
    db_thread.join()
    mqtt_thread.join()