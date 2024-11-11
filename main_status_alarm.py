import os

from dotenv import load_dotenv
load_dotenv()
import dotenv
from utils.alarm import MC_ALARM
from utils.mc_status import MC_STATUS
project_2 = os.environ["PROJECT_TYPE_2"]
project_3 = os.environ["PROJECT_TYPE_3"]
try:
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file,override=True)

    if project_2 == "MCSTATUS":
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
            initial_db=os.getenv('INIT_DB'))
        mc_status_to_sqlserver.run()

    if project_3 == "ALARMLIST":
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
            initial_db=os.getenv('INIT_DB'))
        mc_alarm_to_sqlserver.run()

except Exception as e:
    print(e)
    