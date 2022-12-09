import pandas as pd
import psycopg2 

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO Hospital_AND_ICU(location_id,icu_patients ,icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,weekly_hosp_admissions_per_million , date_time) VALUES(%(iso_code)s,%(icu_patients)s,
                                        %(icu_patients_per_million)s,%(hosp_patients)s,%(hosp_patients_per_million)s, %(weekly_icu_admissions)s, %(weekly_icu_admissions_per_million)s, %(weekly_hosp_admissions)s, %(weekly_hosp_admissions_per_million)s, %(date)s);"""
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

queryTable = """CREATE TABLE Hospital_AND_ICU (
    ID SERIAL primary key,
    location_id varchar(80) references locations (location_id),
    icu_patients integer,
    icu_patients_per_million numeric,
    hosp_patients integer,
    hosp_patients_per_million numeric,
    weekly_icu_admissions integer,
    weekly_icu_admissions_per_million numeric,
    weekly_hosp_admissions integer,
    weekly_hosp_admissions_per_million numeric,
    date_time date
);"""

cursor.execute(queryTable)
conn.commit()
insert_row(["iso_code","icu_patients","icu_patients_per_million","hosp_patients","hosp_patients_per_million", "weekly_icu_admissions", "weekly_icu_admissions_per_million", "weekly_hosp_admissions", "weekly_hosp_admissions_per_million", "date"], conn, cursor)
conn.close()
