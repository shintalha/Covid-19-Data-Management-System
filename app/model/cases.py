import psycopg2 

#Talha's table operation functions
class cases:
    #connection to the database
    def connect():
        conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")
        return conn

    #Finding by primary key value
    def findById(id):
        query = """SELECT * FROM CASES C WHERE C.id = %s""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (id,))
            return cursor.fetchone()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
    
    #Finding all rows primary key value
    def findAll():
        query = """SELECT * FROM CASES""" 
        connection = cases.connect()
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
    def delete(id):
        query = """DELETE FROM CASES C WHERE C.id = %s""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (id,))
            connection.commit()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    #Inserting a new row to the table
    def save(location_id, total_cases, new_cases, total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, date_time):
        
        query = """INSERT INTO CASES(location_id,total_cases,new_cases,total_cases_per_million,
        new_cases_per_million, new_cases_smoothed_per_million, date_time) 
        VALUES(%(location_id)s, %(total_cases)s, %(new_cases)s,%(total_cases_per_million)s,
        %(new_cases_per_million)s, %(new_cases_smoothed_per_million)s, %(date_time)s)""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, {
                'location_id': location_id,
                'total_cases': total_cases,
                'new_cases': new_cases,
                'total_cases_per_million': total_cases_per_million,
                'new_cases_per_million': new_cases_per_million,
                'new_cases_smoothed_per_million': new_cases_smoothed_per_million,
                'date_time': date_time
            })
            connection.commit()
            return True
        except psycopg2.DatabaseError:  
            connection.rollback()
            return False
        finally:
            cursor.close()
            connection.close()
        
    #Updating a row by id
    def update(id,location_id, total_cases, new_cases, total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, date_time):
        query = """UPDATE CASES SET (location_id,total_cases,new_cases,total_cases_per_million,
        new_cases_per_million, new_cases_smoothed_per_million, date_time) 
        = (%(location_id)s,%(total_cases)s, %(new_cases)s,%(total_cases_per_million)s,
        %(new_cases_per_million)s, %(new_cases_smoothed_per_million)s, %(date_time)s) WHERE CASES.id = %(id)s""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, {
                'id': id,
                'location_id': location_id,
                'total_cases': total_cases,
                'new_cases': new_cases,
                'total_cases_per_million': total_cases_per_million,
                'new_cases_per_million': new_cases_per_million,
                'new_cases_smoothed_per_million': new_cases_smoothed_per_million,
                'date_time': date_time
            })
            connection.commit()
            return True
        except psycopg2.DatabaseError:  
            connection.rollback()
            return False
        finally:
            cursor.close()
            connection.close()

    def findByLocationId(location_id):
        query = """SELECT * FROM CASES C WHERE C.location_id = %s""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (location_id,))
            return cursor.fetchone()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    def Get100ByOffset(offset):
        query = """SELECT * FROM CASES ORDER BY CASES.id OFFSET %s ROWS FETCH FIRST 100 ROW ONLY""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (offset,))
            return cursor.fetchall()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
    
    def Get100ByOffsetAndCountry(country, offset):
        query = """SELECT * FROM CASES WHERE CASES.location_id = %s ORDER BY CASES.id OFFSET %s ROWS FETCH FIRST 100 ROW ONLY""" 
        connection = cases.connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, (country,offset,))
            return cursor.fetchall()
        except psycopg2.DatabaseError:  
            connection.rollback()
        finally:
            cursor.close()
            connection.close()