import psycopg2
import random
import time
from psycopg2 import DatabaseError
from datetime import datetime
import threading



def convertTuple(tup):
   result = ""
   for string in tup:
     unpacked_string = str(string)
     result = result + unpacked_string + " "
   return result

def connect():
        def annonymous_query(number):
            return ("INSERT INTO guestbook "
               "(visitor_email, date, message) "
               f"VALUES ( 'jim@gmail.com{number}', current_date, 'This is a test.');")
            
        read_only_thread = threading.Thread()
        read_write_thread = threading.Thread()

        while True: 
            if not read_only_thread.is_alive():
                read_only_thread = threading.Thread(target=read_from_db
                , args=(tuple(["readonly.log"])))
                read_only_thread.start()
            if not read_write_thread.is_alive():
                read_write_thread = threading.Thread(target=write_to_db, args=(
                "readwrite.log", annonymous_query)
                )
                read_write_thread.start()
            time.sleep(0.25)


         
                
def read_from_db(file_path):
    try:
        print("[INFO] Connecting to Read Only DB ... ")    
        connection_ro = psycopg2.connect(user="postgres",
                                  password="password",
                                  host="192.168.4.65",
                                  port="5431",
                                  database="postgres")
        print(connection_ro.get_dsn_parameters(), "\n")
        with open(file_path, "a", buffering=1) as read_only_file:
            while True:
                read_only_file.seek(0)
                print(f"[INFO] Executing commands ... ")
                cursor_read_only = connection_ro.cursor()
                cursor_read_only.execute("SELECT * from guestbook;")
                fetch_record = cursor_read_only.fetchall()
                for string in fetch_record:
                    read_only_file.write(f"{convertTuple(string)}\n")
                cursor_read_only.close()
                time.sleep(0.25)
    except:
        print("Exception occured trying to close connection")
    finally:
        if connection_ro:
            connection_ro.cursor().close()
            connection_ro.close()
            print("Writing connection got closed")
            return
        print("Couldn't close connection")
        return


def write_to_db(file_path, query_function):
    with open(file_path, "a", buffering=1) as read_write_file:
        try:
            connection_rw = psycopg2.connect(user="postgres",
                                          password="password",
                                          host="192.168.4.65",
                                          port="5432",
                                          database="postgres")
            print("[INFO] Connecting to Read Write DB ... ")    
            read_write_file.write("[INFO] Connecting to Read Write DB ... ")    
            print(connection_rw.get_dsn_parameters(), "\n")
            while True:
                rand = random.random()*200 
                query = query_function(rand)
                print(f"[INFO] Executing commands ... write to db")
                execute_write_query(connection_rw, query)
                read_write_file.write(f"STATUS: [OK] TIMESTAMP : [{datetime.now()}] [" + query+"]\n")
                time.sleep(0.25)
        except:
            print("Exception occured trying to close connection")
            read_write_file.write(f"STATUS: [CONNECTION DROPPED] TIMESTAMP : [{datetime.now()}] [---]\n")
        finally:
            if connection_rw:
                connection_rw.cursor().close()
                connection_rw.close()
                print("Writing connection got closed")
                return
            print("Couldn't close connection")
            return

def execute_write_query(connection_rw, query):
        cursor_read_write = connection_rw.cursor()
        cursor_read_write.execute(query)
        connection_rw.commit()
        cursor_read_write.close()
            


if __name__ == "__main__":
    connect()
