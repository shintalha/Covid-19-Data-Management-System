import psycopg2
import pandas as pd

query = """CREATE TABLE Locations (
    location_id VARCHAR(80) PRIMARY KEY,
    country VARCHAR(80),
    population BIGINT,
    rate_age_65_older NUMERIC,
    rate_age_70_older NUMERIC,
    median_age NUMERIC,
    handwashing_facilities NUMERIC );"""

conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
cursor = conn.cursor()
cursor.execute(query)
conn.commit()

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO Locations VALUES(%(iso_code)s,%(location)s,%(population)s,%(aged_65_older)s,
                                        %(aged_70_older)s,%(median_age)s,%(handwashing_facilities)s)""" 
    for idx, row in dataset_df.iterrows():
        insert_dict = dict()
        for col in cols:
            if pd.isna(row[col]):
                insert_dict[col] = None
            else:
                insert_dict[col] = row[col]
        cursor.execute(query, insert_dict)
        conn.commit()

insert_row(["iso_code","location","population","aged_65_older","aged_70_older","median_age","handwashing_facilities"], conn, cursor)
conn.close()
