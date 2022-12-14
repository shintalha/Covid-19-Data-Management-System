import psycopg2 

#locations table operation functions
class locations:
    #connection to the database
    def connect():
        conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
        return conn

    #Finding by primary key value
    def findById(location_id):
        query = """SELECT * FROM LOCATIONS L WHERE L.location_id = %s""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (location_id,))
            return cursor.fetchone()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
    
    #Finding all rows primary key value
    def findAll():
        query = """SELECT * FROM LOCATIONS""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    #Deleting a row by id
    def delete(location_id):
        query = """DELETE FROM LOCATIONS L WHERE L.location_id = %s""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (location_id,))
            connection.commit()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    #Inserting a new row to the table
    def save(location_id, country, population, aged_65_older, aged_70_older, median_age, handwashing_facilities):
        
        query = """INSERT INTO LOCATIONS(location_id, country, population, aged_65_older,
            aged_70_older, median_age, handwashing_facilities) 
        VALUES(%(location_id)s,%(country)s,%(population)s,%(aged_65_older)s,
            %(aged_70_older)s,%(median_age)s,%(handwashing_facilities)s)""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, {
                'location_id': location_id,
                'country': country,
                'population': population,
                'aged_65_older': aged_65_older,
                'age_70_older': aged_70_older,
                'median_age': median_age,
                'handwashing_facilities': handwashing_facilities
            })
            connection.commit()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
        
    #Updating a row by id
    def update(location_id, country, population, aged_65_older, aged_70_older, median_age, handwashing_facilities):
        query = """UPDATE LOCATIONS SET (location_id, country, population, aged_65_older,
            aged_70_older, median_age, handwashing_facilities) = 
            (%(location_id)s,%(country)s,%(population)s,%(aged_65_older)s,
            %(aged_70_older)s,%(median_age)s,%(handwashing_facilities)s) WHERE LOCATIONS.location_id = %(location_id)s""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, {
                'location_id': location_id,
                'country': country,
                'population': population,
                'aged_65_older': aged_65_older,
                'age_70_older': aged_70_older,
                'median_age': median_age,
                'handwashing_facilities': handwashing_facilities
            })
            connection.commit()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

        #Finding by primary key value
    def getCountryNames():
        query = """SELECT L.country FROM LOCATIONS L""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
    
    def getIdByCountryName(country):
        query = """SELECT L.location_id FROM LOCATIONS L WHERE L.country = %s""" 
        connection = locations.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (country,))
            return cursor.fetchone()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()