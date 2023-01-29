import psycopg2
import pandas as pd

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    dataset_df = dataset_df[dataset_df.iso_code.apply(lambda row : row.split("_")[0]!="OWID")]
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO Locations VALUES(%(iso_code)s,%(continent)s,%(location)s,%(population)s,%(aged_65_older)s,
                                        %(aged_70_older)s,%(median_age)s)""" 
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

query = """DROP TABLE IF EXISTS Locations;"""
cursor = conn.cursor()
cursor.execute(query)
conn.commit()


query = """CREATE TABLE Locations (
    location_id VARCHAR(80) PRIMARY KEY,
    continent VARCHAR(80),
    country VARCHAR(80),    
    population BIGINT,
    rate_age_65_older NUMERIC,
    rate_age_70_older NUMERIC,
    median_age NUMERIC );"""

cursor = conn.cursor()
cursor.execute(query)
conn.commit()
insert_row(["iso_code","continent","location","population","aged_65_older","aged_70_older","median_age"], conn, cursor)
conn.close()