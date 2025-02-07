from influxdb import InfluxDBClient
import dotenv
import os
import sys
import time
import pandas as pd
from datetime import datetime
import pymssql

class ADD_DATA:
    def __init__(self):
        self.influx_server = os.environ["INFLUX_SERVER"]
        self.influx_login = os.environ["INFLUX_USER_LOGIN"]
        self.influx_password = os.environ["INFLUX_PASSWORD"]
        self.influx_database = os.environ["INFLUX_DATABASE"]
        self.influx_port = os.environ["INFLUX_PORT"]
        self.influx_column =  os.environ["PRODUCTION_COLUMN_NAMES"]
        self.influx_measurement = os.getenv["INFLUX_MEASUREMENT"]

        self.database = os.environ["DATABASE"]
        self.server = os.environ["SERVER"]
        self.user_login = os.environ["USER_LOGIN"]
        self.password = os.environ["PASSWORD"]

        self.ext_database = os.environ["EXT_DATABASE"]
        self.ext_table = os.environ["EXT_TABLE"]
        self.ext_table_columns = os.environ["EXT_TABLE_COLUMNS"]

        self.mqtt_topic = os.environ["MQTT_TOPIC_1"]

        self.df_influx = None
        self.df_edit = None
        self.df_insert = None
        self.df_ext = None

    def conn_sql(self):
        try:
            cnxn = pymssql.connect(self.server, self.user_login, self.password, self.database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            print('error: '+str(e))
            sys.exit()
    
    def query_external(self):
        try:
            conn = pymssql.connect(server=self.server, user=self.user_login, password=self.password, database=self.ext_database)
            cursor = conn.cursor()
            query = f"""SELECT {self.ext_table_columns} FROM {self.ext_table} """ 
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            self.df_ext = df
            return self.df_ext
        
        except pymssql.DatabaseError as e:
            print("Database error:", e)
            return None
        finally:
            cursor.close()
            conn.close()

    def get_influx(self):
        try:
            result_lists = []
            client = InfluxDBClient(self.influx_server,self.influx_port,self.influx_login,self.influx_password,self.influx_database )
            mqtt_topic_value = list(str(self.mqtt_topic).split(","))
            for i in range(len(mqtt_topic_value)):
                query = f"select * from mqtt_consumer where topic = '{mqtt_topic_value[i]}' order by time desc limit 1"
                print(query)
                result = client.query(query)
                if list(result):
                    result = list(result)[0][0]
                    result_lists.append(result)
            if result_lists:
                result_df = pd.DataFrame.from_dict(result_lists)
                self.df_influx = result_df
            else:  
                result_df = pd.DataFrame()
                self.df_influx = result_df
            return self.df_influx
        except Exception as e:
            print('error: '+str(e))

    def edit_col(self):
        try:
            df = self.df_influx.copy()
            
            df_split = df['topic'].str.split('/', expand=True)
            df['mc_no'] = df_split[3].values
            df['process'] = df_split[2].values
            df = pd.merge(df, self.df_ext, on=['mc_no'], how='left')
            df.drop(columns=['mc_no','process','host'],inplace=True)
            # df.drop(columns=['time'],inplace=True)
            # df.rename(columns = {'time':'data_timestamp'}, inplace = True)
            # df["data_timestamp"] =   pd.to_datetime(df["data_timestamp"]).dt.tz_convert(None)
            # df["data_timestamp"] = df["data_timestamp"] + pd.DateOffset(hours=7)    
            # df["data_timestamp"] = df['data_timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M'))
            # current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
            # df["current_time"] =  current_time
            # df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])
            # df['current_time'] = pd.to_datetime(df['current_time'])
            # df['diff'] = df["current_time"] -  df["data_timestamp"]
            # df['judge'] = df['diff'].apply(lambda x: 0 if x > pd.Timedelta(minutes=5) else 1)
            # # print(df)
            # df = df[df['judge'] == 1]
            # df.drop(columns=['data_timestamp','diff','current_time','judge'],inplace=True)
            df.fillna(0,inplace=True)
            self.df_edit = df

        except Exception as e:
            print('error: '+str(e))

    def insert_influx(self,df,measurement):
        client = InfluxDBClient(self.influx_server,self.influx_port,self.influx_login,self.influx_password,self.influx_database)
        fields_list = self.influx_column
        fields_list = list(str(self.influx_column).split(","))
        json_payload = []
        for _, row in df.iterrows():
            json_body = {
                "measurement": measurement,
                "tags": {
                    "topic": row['topic']
                },
                "fields": {field: (float(row[field]) if isinstance(row[field], (int, float)) else str(row[field])) for field in fields_list}
            }
            json_payload.append(json_body)
        try:
            client.write_points(json_payload)
            print("Insert influxdb successfully")
        except Exception as e:
            print('error: '+str(e))

    def main(self):
        self.get_influx()
        self.query_external()
        # if not self.df_influx.empty:
        #     self.edit_col()
        #     self.insert_influx(self.df_edit,self.influx_measurement)

if __name__ == "__main__":
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file,override=True)
    try:
        add_data = ADD_DATA()
        add_data.main()
    except Exception as e:
        print(e)