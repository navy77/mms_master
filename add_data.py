from influxdb import InfluxDBClient
import dotenv
import os
import sys
import time
import pandas as pd
from datetime import datetime
import pymssql

class MONITOR:
    def __init__(self):
        self.influx_server = os.environ["INFLUX_SERVER"]
        self.influx_login = os.environ["INFLUX_USER_LOGIN"]
        self.influx_password = os.environ["INFLUX_PASSWORD"]
        self.influx_database = os.environ["INFLUX_DATABASE"]
        self.influx_port = os.environ["INFLUX_PORT"]

        self.database = os.environ["DATABASE"]
        self.table = "MONITOR_IOT"
        self.server = os.environ["SERVER"]
        self.user_login = os.environ["USER_LOGIN"]
        self.password = os.environ["PASSWORD"]


        self.ext_database = os.environ["EXT_DATABASE"]
        self.ext_table = os.environ["EXT_TABLE"]
        self.ext_table_columns = os.environ["EXT_TABLE_COLUMNS"]

        self.df_influx = None
        self.df_edit = None
        self.df_insert = None

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
            print(df)
            return df
        
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
                    # result_df = pd.DataFrame.from_dict(result_lists)
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
            # df.drop(columns=['time'],inplace=True)
            df.rename(columns = {'time':'data_timestamp'}, inplace = True)
            df["data_timestamp"] =   pd.to_datetime(df["data_timestamp"]).dt.tz_convert(None)
            df["data_timestamp"] = df["data_timestamp"] + pd.DateOffset(hours=7)    
            df["data_timestamp"] = df['data_timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M'))
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
            df["current_time"] =  current_time
            df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])
            df['current_time'] = pd.to_datetime(df['current_time'])
            df['diff'] = df["current_time"] -  df["data_timestamp"]
            df['judge'] = df['diff'].apply(lambda x: 0 if x > pd.Timedelta(minutes=5) else 1)
            # print(df)
            df = df[df['judge'] == 1]
            df.drop(columns=['data_timestamp','diff','current_time','judge'],inplace=True)
            df.fillna(0,inplace=True)
            self.df_edit = df

        except Exception as e:
            print('error: '+str(e))

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
            df['mac_id'] = df['mac_id'].fillna(0)
            self.df_insert = df
        except Exception as e:
            print('error: '+str(e))

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
            df_all['mac_id'] = 0
            self.df_insert = df_all
        except Exception as e:
            print('error: '+str(e))

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
                    "broker": float(row['broker']),
                    "mac_id": str(row['mac_id'])
                }
            }
            json_payload.append(json_body)
        try:
            client.write_points(json_payload)
            # print(self.df_insert)
            print("Insert influxdb successfully")
        except Exception as e:
            print('error: '+str(e))

    def df_to_db(self):
        init_list = ['mc_no','process']
        insert_db_value = ['broker','modbus','mac_id']
        col_list = init_list+insert_db_value
        cnxn,cursor = self.conn_sql()
        try:
            df = self.df_insert.copy()
            df_split = df['topic'].str.split('/', expand=True)
            df['mc_no'] = df_split[3].values
            df['process'] = df_split[2].values    
            df.drop(columns=['topic'],inplace=True)
            print(df)
            for index, row in df.iterrows():
                value = None
                for i in range(len(col_list)):
                    address = col_list[i]

                    if value == None:
                        value = ",'"+str(row[address])+"'"
                    else:
                        value = value+",'"+str(row[address])+"'"
                
                insert_string = f"""
                INSERT INTO [{self.database}].[dbo].[{self.table}] 
                values(
                    getdate()
                    {value}
                    )
                    """
                cursor.execute(insert_string)
                cnxn.commit()
            cursor.close()
            self.df_insert = None

            print(f"insert sql successfully")     
        except Exception as e:
            print('error: '+str(e))

    def main(self):
        print(self.get_influx())
        self.query_external()
        # if not self.df_influx.empty:
        #     self.edit_col()
        #     self.convert_data()
        #     self.insert_influx(self.df_insert,"iot_monitor")
        #     self.df_to_db()
        # else:              

        #     self.convert_data2()
        #     self.insert_influx(self.df_insert,"iot_monitor")  
        #     self.df_to_db()

if __name__ == "__main__":
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file,override=True)
    try:
        monitor = MONITOR()
        monitor.main()
    except Exception as e:
        print(e)