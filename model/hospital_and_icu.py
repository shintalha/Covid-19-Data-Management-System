import psycopg2 

#Celal's table(HOSPITAL_AND_ICU) operation functions
#Connects to the database when it is created
#Closes the connection with the database when it is destructed
class hospital_and_icu:
    #Constructer
    def __init__(self):
        self.connection = None
        self.connect()
    
    #Destructure    
    def __del__(self):
        try:
            self.connection.close()
        except:
            pass        
    
    #connection to the database    
    def connect(self):
        self.connection = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
    
    #Connection control
    def con_control(self):
        try:
            self.connection.status
        except:
            self.connect()

    #Selecting by primary key value
    def selectFromLOCandDate(self, loc_id, date):
        query = f"""select id FROM HOSPITAL_AND_ICU
        WHERE (location_id = '{loc_id}' and date_time = '{date}')""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return result
    
    def selectFromLOC(self, location_id, offset):
        query = f"""SELECT location_id, date_time, icu_patients ,
        icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million FROM HOSPITAL_AND_ICU
        WHERE (((icu_patients IS NOT NULL) OR (hosp_patients IS NOT NULL)) and (location_id = '{location_id}'))
        ORDER BY date_time desc OFFSET {offset} ROWS FETCH NEXT 50 ROWS ONLY;""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return result
    
    #Selecting all rows primary key value
    def selectAll(self):
        query = """SELECT location_id, date_time, icu_patients ,
        icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million FROM HOSPITAL_AND_ICU
        WHERE ((icu_patients IS NOT NULL) OR (hosp_patients IS NOT NULL))""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return result
    
    def selectAll(self, offset):
        query = f"""SELECT location_id, date_time, icu_patients ,
        icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million FROM HOSPITAL_AND_ICU
        WHERE ((icu_patients IS NOT NULL) OR (hosp_patients IS NOT NULL))
        ORDER BY date_time desc OFFSET {offset} ROWS FETCH NEXT 50 ROWS ONLY""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return result

    #Deleting a row by id
    def delete(self, id):
        query = """DELETE FROM HOSPITAL_AND_ICU AS H WHERE H.id = %s""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (id,))
            self.connection.commit()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()

    #Inserting a new row to the table
    def insert(self, iso_code, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, date):
        
        query = f"""INSERT INTO HOSPITAL_AND_ICU(location_id, icu_patients ,
        icu_patients_per_million ,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million , date_time) 
        VALUES('{iso_code}',{icu_patients},{icu_patients_per_million},{hosp_patients},
        {hosp_patients_per_million}, {weekly_icu_admissions}, {weekly_icu_admissions_per_million}, 
        {weekly_hosp_admissions}, {weekly_hosp_admissions_per_million}, '{date}');""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()
        
    #Updating a row by id
    def update(self, iso_code, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, date):
        query = f"""UPDATE HOSPITAL_AND_ICU SET (location_id,icu_patients ,
        icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million , date_time) 
        = ('{iso_code}',{icu_patients},{icu_patients_per_million},{hosp_patients},
        {hosp_patients_per_million}, {weekly_icu_admissions}, {weekly_icu_admissions_per_million}, 
        {weekly_hosp_admissions}, {weekly_hosp_admissions_per_million}, '{date}') WHERE HOSPITAL_AND_ICU.date_time = '{date}' and HOSPITAL_AND_ICU.location_id = '{iso_code}'""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()
            
    def is_there(self, date, location):
        query = f"""select count(id) from hospital_and_icu
                where (date_time = '{date}' and location_id = '{location}')
                group by date, location_id"""

        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
        except psycopg2.DatabaseError:
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return True if result is not None and result[0]>0 else False