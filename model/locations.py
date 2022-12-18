import psycopg2 

#locations table operation functions
class Locations:
    def __init__(self):
        self.columns = ["location_id","continent","location","population","aged_65_older","aged_70_older","median_age"]
        self.conn = None
        self.connect()
        self.cursor = None

    def __del__(self):
        try:
            self.conn.close()
        except:
            pass

    # Connect to the database
    def connect(self):
        self.conn = psycopg2.connect(database="postgres",
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
    

    #Finding by primary key value
    def find_by_id(self, location_id):
        query = """SELECT * FROM LOCATIONS L WHERE L.location_id = %s""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, (location_id,))
            return self.cursor.fetchone()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            
    
    #Finding all rows primary key value
    def find_all(self):
        query = """SELECT * FROM LOCATIONS""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            

    #Deleting a row by id
    def delete(self, location_id):
        query = """DELETE FROM LOCATIONS L WHERE L.location_id = %s""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, (location_id,))
            self.conn.commit()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            

    #Inserting a new row to the table
    def save(self, location_id, country, population, aged_65_older, aged_70_older, median_age, handwashing_facilities):
        
        query = """INSERT INTO LOCATIONS(location_id, country, population, aged_65_older,
            aged_70_older, median_age, handwashing_facilities) 
        VALUES(%(location_id)s,%(country)s,%(population)s,%(aged_65_older)s,
            %(aged_70_older)s,%(median_age)s,%(handwashing_facilities)s)""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, {
                'location_id': location_id,
                'country': country,
                'population': population,
                'aged_65_older': aged_65_older,
                'age_70_older': aged_70_older,
                'median_age': median_age,
                'handwashing_facilities': handwashing_facilities
            })
            self.conn.commit()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            
        
    #Updating a row by id
    def update(self, location_id, country, population, aged_65_older, aged_70_older, median_age, handwashing_facilities):
        query = """UPDATE LOCATIONS SET (location_id, country, population, aged_65_older,
            aged_70_older, median_age, handwashing_facilities) = 
            (%(location_id)s,%(country)s,%(population)s,%(aged_65_older)s,
            %(aged_70_older)s,%(median_age)s,%(handwashing_facilities)s) WHERE LOCATIONS.location_id = %(location_id)s""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, {
                'location_id': location_id,
                'country': country,
                'population': population,
                'aged_65_older': aged_65_older,
                'age_70_older': aged_70_older,
                'median_age': median_age,
                'handwashing_facilities': handwashing_facilities
            })
            self.conn.commit()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            

        #Finding by primary key value
    def get_country_names(self):
        query = """SELECT L.country FROM LOCATIONS L""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            
    
    def get_id_by_country_name(self, country):
        query = """SELECT L.location_id FROM LOCATIONS L WHERE L.country = %s""" 
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, (country,))
            return self.cursor.fetchone()
        except psycopg2.DatabaseError:  
            self.conn.rollback()
        finally:
            self.cursor.close()
            

    def is_there(self, l_id):
        query = f"""SELECT LOCATION_ID FROM LOCATIONS
                WHERE (LOCATION_ID = '{l_id}')"""
        
        self.check_conn()
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            return self.cursor.fetchone()
        except psycopg2.DatabaseError:
            self.conn.rollback()
        finally:
            self.cursor.close()