import psycopg2 as ps

# Class for CovidTests table
# Connects to the db when it is constructed
# Checks db connection before each operation
# Closes connection when it is destructed
class CovidTests:

    # Initialize object and connect to database
    def __init__(self):
        self.columns = ['location_id', 'total_tests', 'new_tests', 'total_tests_per_thousand',\
             'new_tests_per_thousand', 'new_tests_smoothed', 'positive_rate', 'date_time'] # May be needed :O
        self.conn = None
        self.connect()
    
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
    
    # Read a row by id
    def read(self, id):
        query = """SELECT * FROM COVID_TESTS AS CT WHERE CT.id = %(id)s;"""
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (id,))
            return cursor.fetchone()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()

    # Read all data
    def read(self):
        query = """SELECT * COVID_TESTS CASES""" 
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()

    # Insert a row into table
    def insert_row(self, location_id, total_tests, new_tests,total_tests_per_thousand, \
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time):
        
        query = """INSERT INTO COVID_TESTS(location_id, total_tests, new_tests,total_tests_per_thousand,
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time) 
        VALUES(%(location_id)s,%(total_tests)s,%(new_tests)s,%(total_tests_per_thousand)s,%(new_tests_per_thousand)s, 
        %(new_tests_smoothed)s, %(positive_rate)s, %(date_time)s)"""
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, {
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
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()


    #Update a row by id
    def update(self, location_id, total_tests, new_tests,total_tests_per_thousand, \
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time):
        query = """UPDATE COVID_TESTS SET(location_id, total_tests, new_tests,total_tests_per_thousand,
        new_tests_per_thousand,new_tests_smoothed,positive_rate,date_time) 
        VALUES(%(location_id)s,%(total_tests)s,%(new_tests)s,%(total_tests_per_thousand)s,%(new_tests_per_thousand)s, 
        %(new_tests_smoothed)s, %(positive_rate)s, %(date_time)s)"""
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, {
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
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()


    # Delete a row by id
    def delete(self, id):
        query = """DELETE FROM COVID_TESTS AS CT WHERE CT.id = %(id)s""" 
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (id,))
            self.conn.commit()
        except ps.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()