import pandas as pd
import psycopg2 

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO CASES(location_id,total_cases,new_cases,total_cases_per_million,new_cases_per_million, new_cases_smoothed_per_million, date_time) VALUES(%(iso_code)s,%(total_cases)s,
                                        %(new_cases)s,%(total_cases_per_million)s,%(new_cases_per_million)s, %(new_cases_smoothed_per_million)s, %(date)s)""" 
    for idx, row in dataset_df.iterrows():
        insert_dict = dict()
        for col in cols:
            if pd.isna(row[col]):
                insert_dict[col] = None
            else:
                insert_dict[col] = row[col]
        cursor.execute(query, insert_dict)
        conn.commit()

conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
cursor = conn.cursor()

queryTable = """CREATE TABLE CASES (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(80) REFERENCES locations(location_id),
    total_cases NUMERIC,
    new_cases NUMERIC,
    total_cases_per_million NUMERIC,
    new_cases_per_million NUMERIC,
    new_cases_smoothed_per_million NUMERIC,
    date_time DATE    
);"""

cursor.execute(queryTable)
conn.commit()
insert_row(["iso_code","total_cases","new_cases","total_cases_per_million","new_cases_per_million", "new_cases_smoothed_per_million", "date"], conn, cursor)
conn.close()
