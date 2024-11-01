import paho.mqtt.client as mqtt
import pandas as pd
import threading
import pymssql
import json
import queue
from datetime import datetime
import sys
from dotenv import load_dotenv
import os
import time
load_dotenv()

server = os.getenv('SERVER')
username = os.getenv('USER_LOGIN')
password = os.getenv('PASSWORD')
database = os.getenv('DATABASE')
table = os.getenv('TABLE_2')

mqtt_broker = os.getenv('MQTT_BROKER')
mqtt_port = int(os.getenv('MQTT_PORT'))
mqtt_topic = "status/#"
loop_time = int(os.getenv('STATUS_SCHEDULE'))


data_queue = queue.Queue()

def conn_sql():
    try:
        cnxn = pymssql.connect(server,username,password,database)
        cursor = cnxn.cursor()
        return cnxn,cursor
    except Exception as e:
        sys.exit()

def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode())

    msg = {
        "occurred": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        "topic": message.topic,
        "mc_status": payload.get("status")
    }
    data_queue.put(msg)

def mqtt_subscribe():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect("192.168.0.160", 1883, 60)
    client.subscribe("status/#")
    client.loop_forever() 

def record_to_mssql(loop_time):
    while True:
        time.sleep(loop_time) 
        if not data_queue.empty():
            batch_data = []
            while not data_queue.empty():
                record = data_queue.get()
                batch_data.append(record)
            if batch_data:
                df = pd.DataFrame(batch_data)
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
                        cursor.execute(insert_string)
                        cnxn.commit()
                    cursor.close()
                except Exception as e:
                    print(f"Error recording to MSSQL: {e}")

mqtt_thread = threading.Thread(target=mqtt_subscribe)
record_thread = threading.Thread(target=record_to_mssql)

mqtt_thread.start()
record_thread.start()

mqtt_thread.join()
record_thread.join()


