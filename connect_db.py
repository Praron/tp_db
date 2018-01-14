import psycopg2
import sys

def connect_DB():
    try:
        connection = psycopg2.connect(host='localhost', database='forum', user='admin', password='admin')
        connection.autocommit = True
    except Exception as error:
        print(error)

    return connection
