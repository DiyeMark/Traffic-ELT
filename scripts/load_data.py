import os
import pandas as pd
import mysql.connector as mysql
from mysql.connector import Error
from sqlalchemy import create_engine


class LoadData:
    def __init__(self):
        pass

    def connect(self, db_name=None):
        try:
            conn = mysql.connect(host='localhost',
                                 user='sensor_user',
                                 password='pKPXS68BRcqg$bX$',
                                 database=db_name, buffered=True)
            cur = conn.cursor()
            return conn, cur
        except Error as err:
            print("Database connection error: {}".format(err))

    def create_db(self, db_name: str) -> None:
        try:
            conn, cur = self.connect()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            conn.commit()
            cur.close()
        except Error as err:
            print("Error : {}".format(err))

    def create_tables(self, db_name: str) -> None:
        conn, cur = self.connect(db_name)
        sql_file = 'open_traffic_schema.sql'
        fd = open(sql_file, 'r')
        read_sql_file = fd.read()
        fd.close()

        sql_commands = read_sql_file.split(';')

        for command in sql_commands:
            try:
                res = cur.execute(command)
            except Exception as ex:
                print("Command skipped: ", command)
                print(ex)
        conn.commit()
        cur.close()

        return


    def insert_to_table(self, db_name: str, table_name: str, df: pd.DataFrame) -> None:
            conn, cur = self.connect(db_name)

            for _, row in df.iterrows():
                print(row)
                sql_query = f"""INSERT INTO {table_name} (track_id, type, traveled_d, avg_speed, 
                    lat, lon, speed, lon_acc, lat_acc, time)
                         VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                data = (
                    row[0], row[1], row[2], row[3], (row[4]), (row[5]), row[6], row[7], row[8], row[9])

                try:
                    # Execute the SQL command
                    cur.execute(sql_query, data)

                    # Commit your changes in the database
                    conn.commit()
                    print("Data Inserted Successfully")
                except Exception as e:
                    conn.rollback()
                    print("Error: ", e)
            return


if __name__ == "__main__":
    ld = LoadData()
    # ld.connect()
    ld.create_db(db_name="sensor_data")
    ld.create_tables(db_name="sensor_data")

    data = pd.read_csv('../data/20181024_d1_0830_0900.csv', sep='[,;:]', index_col=False)
    ld.insert_to_table(db_name="sensor_data", table_name="open_traffic", df=data)
