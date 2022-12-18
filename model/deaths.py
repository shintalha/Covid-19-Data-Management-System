import psycopg2 as ps
import numpy as np
from datetime import datetime
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
        self.cursor = None
        self.connect()

    #Deconstructor to disconnect
    def __del__(self):
        try:
            self.connection.close()
        except:
            pass

    #Database connection
    def connect(self):
        self.connection = ps.connect(database="postgres",
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
    def read_with_id(self, id):
        query = """SELECT * FROM DEATHS AS V WHERE d.id = %s ORDER BY d.id;"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, (id,))
            return self.cursor.fetchone()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()

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
            return True
        except ps.DatabaseError:  
            self.connection.rollback()
            return False
        finally:
            self.cursor.close()
            self.connection.close()

#Update a row by using id
    def update_row(self, id,location_id,total_deaths,new_deaths,new_deaths_smoothed,total_deaths_per_million,\
                 new_deaths_per_million,new_deaths_smoothed_per_million, date_time):
        
        query = """UPDATE DEATHS(location_id, total_deaths, new_deaths_smoothed,new_deaths,
        total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,date_time) 
        VALUES(%(location_id)s,%(total_deaths)s,%(new_deaths_smoothed)s,%(new_deaths)s,%(total_deaths_per_million)s, 
        %(new_deaths_per_million)s, %(new_deaths_smoothed_per_million)s, %(date_time)s) WHERE DEATHS.id = %(id)s"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, {
                'id': id,
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
            return True
        except ps.DatabaseError:  
            self.connection.rollback()
            return False
        finally:
            self.cursor.close()
            self.connection.close()

#Delete row by id
    def delete(self, id):
        query = """DELETE FROM DEATHS AS d WHERE d.id = %s""" 
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, (id,))
            self.connection.commit()
            return True
        except ps.DatabaseError:  
            self.connection.rollback()
            return False
        finally:
            self.cursor.close()
            


    def get_location_names(self):
        loc_names = np.array(self.query_location_names())
        loc_count = loc_names.shape[0]
        return [name.replace(" ","_") for name in loc_names.reshape(-1,loc_count)[0] if name is not None]

    def query_location_names(self):
        query = """SELECT DISTINCT country FROM DEATHS AS dt 
                LEFT JOIN LOCATIONS AS L 
                ON dt.location_id = L.location_id
                ORDER BY country ASC"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()


    def read_filter(self, limit=-1, offset=0, loc_name="?", date="?"):
        date_str = ""
        query = "SELECT * FROM DEATHS AS dt" 
        if loc_name!="?":
            query += " LEFT JOIN LOCATIONS AS L ON dt.location_id = L.location_id WHERE country=%(loc_name)s"
        if date!="?":
            date_str = " date_time >= TO_DATE(%(date)s,'YYYY-MM-DD')"
        if loc_name=="?" and date!="?":
            query += " WHERE" + date_str
        elif loc_name!="?" and date!="?":
            query += " AND" + date_str

        query += " ORDER BY dt.id OFFSET %(offset)s"
        if limit != -1:
            query += " LIMIT " + str(limit)
        query += ";"
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, {"offset":str(offset),
                                        "loc_name":loc_name,
                                        "date":date})
            return self.cursor.fetchall()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()        

        

    def get_dates(self,loc,start=-1):
        dates = np.array(self.query_dates(loc,start))
        date_count = dates.shape[0]
        dates_list = dates.reshape(-1,date_count)[0]
        return [date.strftime('%Y-%m-%d') for date in dates_list if date is not None]    




    def query_dates(self,loc,start):
        loc_str = ""
        start_str = ""
        query = "SELECT DISTINCT date_time FROM DEATHS AS dt"
        if loc != "?":
            loc_str = " LEFT JOIN LOCATIONS AS L ON dt.location_id = L.location_id WHERE country = %(loc_str)s"
        if start != -1:
            start = datetime.strptime(start,'%Y-%m-%d')
            start_str = " date_time > %(start)s"
        query += loc_str
        if loc == "?" and start != -1:
            query += " WHERE"
        elif loc != "?" and start != -1:
            query += " AND"
        query += start_str
        query += " ORDER BY date_time ASC;"
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query,{'loc_str':loc, 'start':start})
            return self.cursor.fetchall()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()        


    def readFid(self, id):
        query = """SELECT * FROM DEATHS AS dt WHERE dt.id = %s ORDER BY dt.id;"""
        self.check_connection()
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, (id,))
            return self.cursor.fetchone()
        except ps.DatabaseError:  
            self.connection.rollback()
        finally:
            self.cursor.close()
