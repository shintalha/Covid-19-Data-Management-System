import pandas as pd
import psycopg2 

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    dataset_df = dataset_df[dataset_df.iso_code.apply(lambda row : row.split("_")[0]!="OWID")]
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO DEATHS(location_id,total_deaths,new_deaths,new_deaths_smoothed,total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million, date_time) 
    VALUES(%(iso_code)s,%(total_deaths)s,%(new_deaths)s,%(new_deaths_smoothed)s,%(total_deaths_per_million)s,%(new_deaths_per_million)s,%(new_deaths_smoothed_per_million)s, %(date)s)""" 
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

queryTable = """CREATE TABLE DEATHS (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(80) REFERENCES locations(location_id),
    total_deaths INTEGER,
    new_deaths INTEGER,
    new_deaths_smoothed NUMERIC,
    total_deaths_per_million NUMERIC,
    new_deaths_per_million NUMERIC,
    new_deaths_smoothed_per_million NUMERIC,
    date_time DATE    
);"""

cursor.execute(queryTable)
conn.commit()
insert_row(["iso_code","total_deaths","new_deaths","new_deaths_smoothed","total_deaths_per_million", "new_deaths_per_million", "new_deaths_smoothed_per_million","date"], conn, cursor)
conn.close()
