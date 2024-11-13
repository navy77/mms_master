import os
from dotenv import load_dotenv
from utils.alarm import MC_ALARM
from utils.mc_status import MC_STATUS

load_dotenv()

def run_mc_status():
    try:
        mc_status_to_sqlserver = MC_STATUS(
            server=os.getenv('SERVER'),
            database=os.getenv('DATABASE'),
            user_login=os.getenv('USER_LOGIN'),
            password=os.getenv('PASSWORD'),
            table=os.getenv('TABLE_2'),
            table_columns=os.getenv('PRODUCTION_COLUMN_NAMES'),
            table_log=os.getenv('TABLE_LOG_2'),
            table_columns_log=os.getenv('TABLE_COLUMNS_LOG'),
            influx_server=os.getenv('INFLUX_SERVER'),
            influx_database=os.getenv('INFLUX_DATABASE'),
            influx_user_login=os.getenv('INFLUX_USER_LOGIN'),
            influx_password=os.getenv('INFLUX_PASSWORD'),
            influx_port=int(os.getenv('INFLUX_PORT')),
            column_names=os.getenv('MCSTATUS_TABLE_COLUMNS'),
            mqtt_topic=os.getenv('MQTT_TOPIC_2'),
            initial_db=os.getenv('INIT_DB')
        )
        mc_status_to_sqlserver.run()
    except Exception as e:
        print("Error in MC_STATUS:", e)

def run_mc_alarm():
    try:
        mc_alarm_to_sqlserver = MC_ALARM(
            server=os.getenv('SERVER'),
            database=os.getenv('DATABASE'),
            user_login=os.getenv('USER_LOGIN'),
            password=os.getenv('PASSWORD'),
            table=os.getenv('TABLE_3'),
            table_columns=os.getenv('PRODUCTION_COLUMN_NAMES'),
            table_log=os.getenv('TABLE_LOG_3'),
            table_columns_log=os.getenv('TABLE_COLUMNS_LOG'),
            influx_server=os.getenv('INFLUX_SERVER'),
            influx_database=os.getenv('INFLUX_DATABASE'),
            influx_user_login=os.getenv('INFLUX_USER_LOGIN'),
            influx_password=os.getenv('INFLUX_PASSWORD'),
            influx_port=int(os.getenv('INFLUX_PORT')),
            column_names=os.getenv('ALARMLIST_TABLE_COLUMNS'),
            mqtt_topic=os.getenv('MQTT_TOPIC_3'),
            initial_db=os.getenv('INIT_DB')
        )
        mc_alarm_to_sqlserver.run()
    except Exception as e:
        print("Error in MC_ALARM:", e)

project_2 = os.getenv("PROJECT_TYPE_2", "").strip()
project_3 = os.getenv("PROJECT_TYPE_3", "").strip()

if project_2 == "MCSTATUS":
    run_mc_status()

if project_3 == "ALARMLIST":
    run_mc_alarm()
