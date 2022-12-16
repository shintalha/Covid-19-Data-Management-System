import psycopg2 


conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
cursor = conn.cursor()

queryTable = """CREATE TABLE user_table (
    ID SERIAL primary key,
    NAME varchar(80) NOT NULL,
    SURNAME varchar(80) NOT NULL,
    EMAIL varchar(80) NOT NULL,
    PASSWORD varchar(80) NOT NULL,
    admin boolean
);"""

cursor.execute(queryTable)
conn.commit()
conn.close()
