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
    def selectFromID(self, id):
        query = """SELECT * FROM HOSPITAL_AND_ICU AS H WHERE H.id = %s""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (id,))
            return cursor.fetchone()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()
    
    #Selecting all rows primary key value
    def selectAll(self):
        query = """SELECT * FROM HOSPITAL_AND_ICU""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()

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
        
        query = """INSERT INTO Hospital_AND_ICU(location_id,icu_patients ,
        icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million , date_time) 
        VALUES(%(iso_code)s,%(icu_patients)s,%(icu_patients_per_million)s,%(hosp_patients)s,
        %(hosp_patients_per_million)s, %(weekly_icu_admissions)s, %(weekly_icu_admissions_per_million)s, 
        %(weekly_hosp_admissions)s, %(weekly_hosp_admissions_per_million)s, %(date)s);""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, {
                'iso_code': iso_code,
                'icu_patients': icu_patients,
                'icu_patients_per_million': icu_patients_per_million,
                'hosp_patients': hosp_patients,
                'hosp_patients_per_million': hosp_patients_per_million,
                'weekly_icu_admissions': weekly_icu_admissions,
                'weekly_icu_admissions_per_million': weekly_icu_admissions_per_million,
                'weekly_hosp_admissions': weekly_hosp_admissions,
                'weekly_hosp_admissions_per_million': weekly_hosp_admissions_per_million,
                'date': date
            })
            self.connection.commit()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()
        
    #Updating a row by id
    def update(self, id, iso_code, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, date):
        query = """UPDATE HOSPITAL_AND_ICU SET (location_id,icu_patients ,
        icu_patients_per_million,hosp_patients ,hosp_patients_per_million ,
        weekly_icu_admissions ,weekly_icu_admissions_per_million ,weekly_hosp_admissions ,
        weekly_hosp_admissions_per_million , date_time) 
        = (%(iso_code)s,%(icu_patients)s,%(icu_patients_per_million)s,%(hosp_patients)s,
        %(hosp_patients_per_million)s, %(weekly_icu_admissions)s, %(weekly_icu_admissions_per_million)s, 
        %(weekly_hosp_admissions)s, %(weekly_hosp_admissions_per_million)s, %(date)s) WHERE HOSPITAL_AND_ICU.id = %(id)s""" 
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, {
                'id': id,
                'iso_code': iso_code,
                'icu_patients': icu_patients,
                'icu_patients_per_million': icu_patients_per_million,
                'hosp_patients': hosp_patients,
                'hosp_patients_per_million': hosp_patients_per_million,
                'weekly_icu_admissions': weekly_icu_admissions,
                'weekly_icu_admissions_per_million': weekly_icu_admissions_per_million,
                'weekly_hosp_admissions': weekly_hosp_admissions,
                'weekly_hosp_admissions_per_million': weekly_hosp_admissions_per_million,
                'date': date
            })
            self.connection.commit()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
        finally:
            cursor.close()