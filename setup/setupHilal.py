import pandas as pd
import psycopg2 

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    dataset_df = dataset_df[dataset_df.iso_code.apply(lambda row : row.split("_")[0]!="OWID")].dropna()
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO VACCINATIONS(location_id, total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time) 
VALUES(%(iso_code)s,%(total_vaccinations)s, %(people_vaccinated)s,%(people_fully_vaccinated)s,%(total_boosters)s, %(new_vaccinations)s, %(new_vaccinations_smoothed)s, %(date)s)""" 
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


queryTable = """CREATE TABLE VACCINATIONS(
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(80) REFERENCES locations(location_id),
    total_vaccinations NUMERIC,
    people_vaccinated NUMERIC,
    people_fully_vaccinated NUMERIC,
    total_boosters NUMERIC,
    new_vaccinations NUMERIC,
    new_vaccinations_smoothed NUMERIC,
    date_time DATE
);"""

cursor.execute(queryTable)
conn.commit()
insert_row(["iso_code","total_vaccinations","people_vaccinated","people_fully_vaccinated","total_boosters", "new_vaccinations","new_vaccinations_smoothed", "date"], conn, cursor)
conn.close()
