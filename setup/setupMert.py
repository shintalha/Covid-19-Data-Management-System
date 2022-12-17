import pandas as pd
import psycopg2 

def insert_row(cols: list, conn, cursor):
    dataset_df = pd.read_csv("setup/dataset.csv")
    dataset_df = dataset_df.loc[:,cols].drop_duplicates()
    dataset_df = dataset_df[dataset_df.iso_code.apply(lambda row : row.split("_")[0]!="OWID")]
    # Sütun sayısı kadar %s ekle
    query = """INSERT INTO COVID_TESTS(location_id, total_tests, new_tests,total_tests_per_thousand,new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time) 
                VALUES(%(iso_code)s,%(total_tests)s,
                                        %(new_tests)s,%(total_tests_per_thousand)s,%(new_tests_per_thousand)s, %(new_tests_smoothed)s, %(positive_rate)s, %(date)s)""" 
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

queryTable = """CREATE TABLE COVID_TESTS (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(80) REFERENCES locations(location_id),
    total_tests BIGINT,
    new_tests NUMERIC,
    total_tests_per_thousand NUMERIC,
    new_tests_per_thousand NUMERIC,
    new_tests_smoothed NUMERIC,
    positive_rate NUMERIC,
    date_time DATE    
);"""

cursor.execute(queryTable)
conn.commit()
insert_row(["iso_code","total_tests","new_tests","total_tests_per_thousand","new_tests_per_thousand", "new_tests_smoothed", "positive_rate", "date"], conn, cursor)
conn.close()
