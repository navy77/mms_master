from influxdb import InfluxDBClient
import dotenv
import os
import time
import datetime

def drop_mqtt_consumer():

    day = datetime.datetime.now().strftime("%d")
    if day == '1':
        # client = InfluxDBClient(host='192.168.0.180', port=8087, username='admin', password='admin', database='influx')
        time.sleep(1)
        client = InfluxDBClient(os.environ["INFLUX_SERVER"], os.environ["INFLUX_PORT"], os.environ["INFLUX_USER_LOGIN"], os.environ["INFLUX_PASSWORD"], os.environ["INFLUX_DATABASE"])
        client.query('DROP MEASUREMENT mqtt_consumer')
        print("Dropped measurement mqtt_consumer")
        client.close()
    else:print("not 1st day")

if __name__ == "__main__":
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file,override=True)
    drop_mqtt_consumer()
