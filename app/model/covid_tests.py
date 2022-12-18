import psycopg2 as ps
import numpy as np
from datetime import datetime

# Class for CovidTests table
# Connects to the db when it is constructed
# Checks db connection before each operation
# Closes connection when it is destructed
class CovidTests:

    # Initialize object and connect to database
    def __init__(self):
        self.columns = ["location_id", "total_tests", "new_tests", "total_tests_per_thousand", "new_tests_per_thousand", "new_tests_smoothed", "positive_rate", "date_time"]
        self.conn = None
        self.connect()
        self.cusor = None
    
    # Close connection to the database and destruct
    def __del__(self):
        try:
            self.conn.close()
        except:
            pass

    # Connect to the database
    def connect(self):
        self.conn = ps.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")

    # Check connection and connect again if it is closed 
    def check_conn(self):
        try:
            self.conn.status
        except:
            self.connect()
    
    def get_dates(self,loc,start=-1):
        dates = np.array(self.query_dates(loc,start))
        date_count = dates.shape[0]
        dates_list = dates.reshape(-1,date_count)[0]
        return [date.strftime('%Y-%m-%d') for date in dates_list if date is not None]
    
    def query_dates(self,loc,start):
        loc_str = ""
        start_str = ""
        query = "SELECT DISTINCT date_time FROM COVID_TESTS AS CT"
        if loc != "?":
            loc_str = " LEFT JOIN LOCATIONS AS L ON CT.location_id = L.location_id WHERE country = %(loc_str)s"
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
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query,{'loc_str':loc, 'start':start})
            return self.cursor.fetchall()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()


    def get_location_names(self):
        loc_names = np.array(self.query_location_names())
        loc_count = loc_names.shape[0]
        return [name.replace(" ","_") for name in loc_names.reshape(-1,loc_count)[0] if name is not None]

    def query_location_names(self):
        query = """SELECT DISTINCT country FROM COVID_TESTS AS CT 
                LEFT JOIN LOCATIONS AS L 
                ON CT.location_id = L.location_id
                ORDER BY country ASC"""
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()

    # Read a row by id
    def read_by_id(self, id):
        query = """SELECT * FROM COVID_TESTS AS CT WHERE CT.id = %s ORDER BY CT.id;"""
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, (id,))
            return self.cursor.fetchone()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()


    def read_filter(self, limit=-1, offset=0, loc_name="?", date="?"):
        date_str = ""
        query = "SELECT * FROM COVID_TESTS AS CT" 
        if loc_name!="?":
            query += " LEFT JOIN LOCATIONS AS L ON CT.location_id = L.location_id WHERE country=%(loc_name)s"
        if date!="?":
            date_str = " date_time >= TO_DATE(%(date)s,'YYYY-MM-DD')"
        if loc_name=="?" and date!="?":
            query += " WHERE" + date_str
        elif loc_name!="?" and date!="?":
            query += " AND" + date_str

        query += " ORDER BY CT.id OFFSET %(offset)s"
        if limit != -1:
            query += " LIMIT " + str(limit)
        query += ";"
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, {"offset":str(offset),
                                        "loc_name":loc_name,
                                        "date":date})
            return self.cursor.fetchall()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()

    # Insert a row into table
    def insert_row(self, location_id, total_tests, new_tests,total_tests_per_thousand, \
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time):
        
        query = """INSERT INTO COVID_TESTS(location_id, total_tests, new_tests,total_tests_per_thousand,
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time) 
        VALUES(%(location_id)s,%(total_tests)s,%(new_tests)s,%(total_tests_per_thousand)s,%(new_tests_per_thousand)s, 
        %(new_tests_smoothed)s, %(positive_rate)s, %(date_time)s)"""
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, {
                'location_id': location_id,
                'total_tests': total_tests,
                'new_tests': new_tests,
                'total_tests_per_thousand': total_tests_per_thousand,
                'new_tests_per_thousand': new_tests_per_thousand,
                'new_tests_smoothed': new_tests_smoothed,
                'positive_rate': positive_rate,
                'date_time': date_time
            })
            self.conn.commit()
            return True
        except ps.DatabaseError:  
            self.conn.rollback()
            return False
        finally:
            self.cursor.close()


    #Update a row by id
    def update(self, id, location_id, total_tests, new_tests,total_tests_per_thousand, \
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time):
        query = """UPDATE COVID_TESTS SET(location_id, total_tests, new_tests,total_tests_per_thousand,
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time) 
        = (%(location_id)s,%(total_tests)s,%(new_tests)s,%(total_tests_per_thousand)s,%(new_tests_per_thousand)s, 
        %(new_tests_smoothed)s, %(positive_rate)s, %(date_time)s) WHERE COVID_TESTS.id = %(id)s"""
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, {
                'id': id,
                'location_id': location_id,
                'total_tests': total_tests,
                'new_tests': new_tests,
                'total_tests_per_thousand': total_tests_per_thousand,
                'new_tests_per_thousand': new_tests_per_thousand,
                'new_tests_smoothed': new_tests_smoothed,
                'positive_rate': positive_rate,
                'date_time': date_time
            })
            self.conn.commit()
            return True
        except ps.DatabaseError:  
            self.conn.rollback()
            return False
        finally:
            self.cursor.close()


    # Delete a row by id
    def delete(self, id):
        query = """DELETE FROM COVID_TESTS AS CT WHERE CT.id = %s""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, (id,))
            self.conn.commit()
            return True
        except ps.DatabaseError:  
            self.conn.rollback()
            return False
        finally:
            self.cursor.close()