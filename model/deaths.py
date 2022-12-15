import psycopg2 as ps
#Zehra's table --Deaths--
#Operation Functions

#Constructed --> connects to the db
#Destructed --> closes connection

class Deaths:
    #Constructor to connect -initalizer-
    def __init__(self):
        self.columns = "['location_id','total_deaths','new_deaths','new_deaths_smoothed',\
        'total_deaths_per_million','new_deaths_per_million','new_deaths_smoothed_per_million','date_time']"
        self.connection = None
        self.connect()

    #Deconstructor to disconnect
    def __del__(self):
        try:
            self.connection.close()
        except:
            pass

    #Database connection
    def connect(self):
        self.conn = ps.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
    #Check connection 
    def check_connection(self):
        try:
            self.connection.status
        except:
            self.connect()

    #Read by id(will be used if necessary)
    def readFromId(self):
        query = """SELECT * FROM DEATHS AS dt WHERE dt.id = %s""" 
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, (id,))
            return self.cursor.fetchone()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()

    #Read all
    def readAll(self):
        query = """SELECT * FROM DEATHS"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, (id,))
            return self.cursor.fetchone()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()

    #Insert new row
    def insert_row(self, location_id,total_deaths,new_deaths,new_deaths_smoothed,total_deaths_per_million,\
                 new_deaths_per_million,new_deaths_smoothed_per_million, date_time):
        
        query = """INSERT INTO DEATHS(location_id, total_deaths, new_deaths_smoothed,new_deaths,
        total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,date_time) 
        VALUES(%(location_id)s,%(total_deaths)s,%(new_deaths_smoothed)s,%(new_deaths)s,%(total_deaths_per_million)s, 
        %(new_deaths_per_million)s, %(new_deaths_smoothed_per_million)s, %(date_time)s)"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, {
                'location_id': location_id,
                'total_deaths': total_deaths,
                'new_deaths_smoothed': new_deaths_smoothed,
                'total_deaths_per_million': total_deaths_per_million,
                'new_deaths_per_million': new_deaths_per_million,
                'new_deaths_smoothed_per_million': new_deaths_smoothed_per_million,
                'new_deaths': new_deaths,
                'date_time': date_time
            })
            self.connection.commit()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()

#Update a row by using id
    def update_row(self, location_id,total_deaths,new_deaths,new_deaths_smoothed,total_deaths_per_million,\
                 new_deaths_per_million,new_deaths_smoothed_per_million, date_time):
        
        query = """UPDATE DEATHS(location_id, total_deaths, new_deaths_smoothed,new_deaths,
        total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,date_time) 
        VALUES(%(location_id)s,%(total_deaths)s,%(new_deaths_smoothed)s,%(new_deaths)s,%(total_deaths_per_million)s, 
        %(new_deaths_per_million)s, %(new_deaths_smoothed_per_million)s, %(date_time)s)"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, {
                'location_id': location_id,
                'total_deaths': total_deaths,
                'new_deaths_smoothed': new_deaths_smoothed,
                'total_deaths_per_million': total_deaths_per_million,
                'new_deaths_per_million': new_deaths_per_million,
                'new_deaths_smoothed_per_million': new_deaths_smoothed_per_million,
                'new_deaths': new_deaths,
                'date_time': date_time
            })
            self.connection.commit()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()

#Delete row by id
    def delete(self, id):
        query = """DELETE FROM DEATHS AS dt WHERE dt.id = %s""" 
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, (id,))
            self.connection.commit()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()