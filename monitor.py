from influxdb import InfluxDBClient
import dotenv
import os
import time
import pandas as pd
# import datetime
from datetime import datetime

class MONITOR:
    def __init__(self):
        self.mqtt_topic = os.getenv('MQTT_TOPIC_4')
        self.influx_server = os.environ["INFLUX_SERVER"]
        self.influx_login = os.environ["INFLUX_USER_LOGIN"]
        self.influx_password = os.environ["INFLUX_PASSWORD"]
        self.influx_database = os.environ["INFLUX_DATABASE"]
        self.influx_port = os.environ["INFLUX_PORT"]
        self.process = os.environ["PROCESS"]
        self.df_influx = None
        self.df_edit = None
        self.df_insert = None

    def get_influx(self):
        try:
            result_lists = []
            client = InfluxDBClient(self.influx_server,self.influx_port,self.influx_login,self.influx_password,self.influx_database )
            mqtt_topic_value = list(str(self.mqtt_topic).split(","))
            for i in range(len(mqtt_topic_value)):
                query = f"select topic,modbus,broker from mqtt_consumer where topic = '{mqtt_topic_value[i]}' order by time desc limit 1"
                result = client.query(query)
                if list(result):
                    result = list(result)[0][0]
                    result_lists.append(result)
                    # result_df = pd.DataFrame.from_dict(result_lists)
            if result_lists:
                result_df = pd.DataFrame.from_dict(result_lists)
                self.df_influx = result_df
            else:  
                result_df = pd.DataFrame()
                self.df_influx = result_df
            return self.df_influx
        except Exception as e:
            print(e)

    def edit_col(self):
        try:
            df = self.df_influx.copy()
            df_split = df['topic'].str.split('/', expand=True)
            df['mc_no'] = df_split[3].values
            df['process'] = df_split[2].values
            # df.drop(columns=['time'],inplace=True)
            df.rename(columns = {'time':'data_timestamp'}, inplace = True)
            df["data_timestamp"] =   pd.to_datetime(df["data_timestamp"]).dt.tz_convert(None)
            df["data_timestamp"] = df["data_timestamp"] + pd.DateOffset(hours=7)    
            df["data_timestamp"] = df['data_timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M'))
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
            print(current_time)
            df["current_time"] =  current_time
            df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])
            df['current_time'] = pd.to_datetime(df['current_time'])
            df['diff'] = df["current_time"] -  df["data_timestamp"]
            df['judge'] = df['diff'].apply(lambda x: 0 if x > pd.Timedelta(minutes=4) else 1)
            # print(df)
            df = df[df['judge'] == 1]
            print(df)
            df.drop(columns=['data_timestamp','diff','current_time','judge'],inplace=True)
            df.fillna(0,inplace=True)
            self.df_edit = df

        except Exception as e:
            print(e)

    def convert_data(self):
        try:
            topic_list = self.mqtt_topic.split(",")
            df_all = pd.DataFrame({
                'topic': topic_list,
                'mc_no': [topic.split('/')[-1] for topic in topic_list],  
                'process': [self.process.lower()] * len(topic_list)  
            })
            df = pd.merge(df_all, self.df_edit, on=['topic', 'mc_no', 'process'], how='left')
            df['modbus'] = df['modbus'].fillna(0)
            df['broker'] = df['broker'].fillna(0)
            self.df_insert = df
        except Exception as e:
            print(e)

    def convert_data2(self):
        try:
            topic_list = self.mqtt_topic.split(",")
            df_all = pd.DataFrame({
                'topic': topic_list,
                'mc_no': [topic.split('/')[-1] for topic in topic_list],  
                'process': [self.process.lower()] * len(topic_list)  
            })
            df_all['modbus'] = 0
            df_all['broker'] = 0
            self.df_insert = df_all
        except Exception as e:
            print(e)

    def insert_influx(self,df,measurement):
        client = InfluxDBClient(self.influx_server,self.influx_port,self.influx_login,self.influx_password,self.influx_database)
        json_payload = []
        for _, row in df.iterrows():
            json_body = {
                "measurement": measurement,
                "tags": {
                    "topic": row['topic'],
                    "process": row['process'],
                    "mc_no": row['mc_no']
                },
                "fields": {
                    "modbus": float(row['modbus']),
                    "broker": float(row['broker'])
                }
            }
            json_payload.append(json_body)
        try:
            client.write_points(json_payload)
            print(self.df_insert)
            print("successfully")
        except Exception as e:
            print(e)

    def main(self):
        self.get_influx()
        if not self.df_influx.empty:
            self.edit_col()
            self.convert_data()
            self.insert_influx(self.df_insert,"iot_monitor")

        else:              
            self.convert_data2()
            self.insert_influx(self.df_insert,"iot_monitor")  


if __name__ == "__main__":
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file,override=True)
    try:
        monitor = MONITOR()
        monitor.main()
    except Exception as e:
        print(e)