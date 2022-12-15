import psycopg2 as psg

# Class for Vaccinations table
# Connects to the db when it is constructed
# Checks db connection before each operation
# Closes connection when it is destructed
class Vaccinations:

    # Initialize object and connect to database
    def __init__(self):
        self.columns = ['location_id', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'total_boosters', 'new_vaccinations', 'new_vaccinations_smoothed', 'date_time'] # May be needed :O
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
        self.conn = psg.connect(database="postgres",
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
        query = """SELECT * FROM VACCINATIONS AS V WHERE V.id = %(id)s;"""
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (id,))
            return cursor.fetchone()
        except psg.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()

    # Read all data
    def read(self):
        query = """SELECT * VACCINATIONS CASES""" 
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except psg.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()

    # Insert a row into table
    def insert_row(self, location_id, total_vaccinations, people_vaccinated, \
         people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time):
        query = """INSERT INTO VACCINATIONS(location_id, total_vaccinations, people_vaccinated, 
        people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time) 
        VALUES(%(iso_code)s,%(total_vaccinations)s, %(people_vaccinated)s,%(people_fully_vaccinated)s,
        %(total_boosters)s, %(new_vaccinations)s, %(new_vaccinations_smoothed)s, %(date)s)""" 

        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, {
                'location_id': location_id,
                'total_vaccinations': total_vaccinations,
                'people_vaccinated': people_vaccinated,
                'people_fully_vaccinated': people_fully_vaccinated,
                'total_boosters': total_boosters,
                'new_vaccinations': new_vaccinations,
                'new_vaccinations_smoothed': new_vaccinations_smoothed,
                'date_time': date_time
            })
            self.conn.commit()
        except psg.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()


    #Update a row by id
    def update(self, location_id, total_vaccinations, people_vaccinated, \
         people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time):
        query = """INSERT INTO VACCINATIONS(location_id, total_vaccinations, people_vaccinated, 
        people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time) 
        VALUES(%(iso_code)s,%(total_vaccinations)s, %(people_vaccinated)s,%(people_fully_vaccinated)s,
        %(total_boosters)s, %(new_vaccinations)s, %(new_vaccinations_smoothed)s, %(date)s)""" 
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, {
                'location_id': location_id,
                'total_vaccinations': total_vaccinations,
                'people_vaccinated': people_vaccinated,
                'people_fully_vaccinated': people_fully_vaccinated,
                'total_boosters': total_boosters,
                'new_vaccinations': new_vaccinations,
                'new_vaccinations_smoothed': new_vaccinations_smoothed,
                'date_time': date_time
            })
            self.conn.commit()
        except psg.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()


    # Delete a row by id
    def delete(self, id):
        query = """DELETE FROM VACCINATIONS AS V WHERE V.id = %(id)s""" 
        self.check_conn()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (id,))
            self.conn.commit()
        except psg.DatabaseError:  
            self.conn.rollback()
        finally:
            cursor.close()