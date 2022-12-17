import psycopg2

# Class for user_table table
class User:

    # Initialize object and connect to database
    def __init__(self):
        self.columns = ['name', 'surname', 'email', 'password', 'admin']
        self.connection = None
        self.connect()
        isAdmin = False
    
    # Close connection to the database and destruct
    def __del__(self):
        try:
            self.conn.close()
        except:
            pass

    # Connect to the database
    def connect(self):
        self.connection = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")

    # Check connection and connect again if it is closed 
    def con_control(self):
        try:
            self.conn.status
        except:
            self.connect()
    
    def selectByEmailReturnPasswordAndID(self, email):
        query = f"""select password, id from user_table
                    where (email = '{email}');"""
                    
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
        except psycopg2.Error:  
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return result
        
    def register(self, name, surname, email, password, admin):
        query = f"""insert into user_table(name, surname, email, password, admin)
                    values('{name}','{surname}','{email}','{password}','{admin}');"""
        
        self.con_control()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            result = True
        except psycopg2.DatabaseError:  
            self.connection.rollback()
            result = False
        finally:
            cursor.close()
            return result
        
    def selectByID(self, id):
        query = f"""select name, surname, email, password, admin from user_table
                    where id = '{id}'"""
        
        self.con_control
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
        
    def isAdmin(self, id):
        query = f"""select admin from user_table
                    group by id having(id={id});"""
        
        self.con_control
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
        except psycopg2.DatabaseError:  
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            return result[0]
        
        