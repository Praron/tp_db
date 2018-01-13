import psycopg2
import sys

def connect_DB():
    try:
        connection = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='ident')
        connection.autocommit = True
    except Exception as error:
        print(error)

    return connection
