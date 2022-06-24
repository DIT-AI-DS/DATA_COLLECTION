"""
File: <api_processing.>.py
-------------------
"""

# Standard for making HTTP requests in Python
import requests
# Work with JSON (string, or file containing JSON object)
import json
#  Interact with the underlying operating system
import os
# Python data analysis toolkit
import pandas as pd
# Integrate the SQLite database with Python
import sqlite3 as sqlite3
# Python SQL toolkit and Object Relational Mapper. NEEDED FOR PANDA
from sqlalchemy import create_engine


class Sqlite3Wrapper:

    """ This class is used to hold information for SQLITE3 objects."""

    def __init__(self, **kwargs):
        """Initialize db class variables"""
        try:
            self.active_connection = sqlite3.connect(kwargs.get('filename_path'))  # Connecting to the sqlite DB
            self.active_cursor = self.active_connection.cursor()  # Retrieving data
            self.table = kwargs.get('table')

        except sqlite3.Error as error:
            print("Error while connecting to sqlite3", error)

    def close(self):
        """close sqlite3 connection"""
        if self.active_connection:
            self.active_connection.close()
            print("The SQLite3 connection is closed")


class MysqlWrapper:

    """ This class is used to hold information for MySQL objects."""

    def __init__(self, **kwargs):

        # Defining the database credentials per object
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.host = kwargs.get("host")
        self.port = kwargs.get("port")
        self.database = kwargs.get("database")
        self.mysql_conn = None
        self.engine = None

    def connect(self):

        # Getting the connection object (ENGINE) for the MySQL Database
        engine = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(self.user,
                                                                                self.password,
                                                                                self.host, self.port,
                                                                                self.database))

        try:
            # Connecting to the specific MySQL Database
            self.mysql_conn = engine.connect()

        except Exception as ex:
            print("Connection could not be made due to the following error: \n", ex)

        return self.mysql_conn

    def close(self):
        try:
            self.mysql_conn.invalidate()
            self.mysql_conn.close()
        except Exception as ex:
            print("Connection could not be closed due to the following error: \n", ex)
        finally:
            print("The MySQL connection is closed")


def http_fetcher(url: str, params=None, headers=None):
    """ Session object allows one to persist certain parameters across requests.
    It also persists cookies across all requests made from the Session instance
    and will use urllib3’s connection pooling.
    This function process the API stream and make a get request
    """
    # Connecting to the API
    with requests.Session() as session:  # Create a session object
        response_object = session.get(url, params=params, headers=headers)
        return response_object


def format_response(response_object, params, response_api_format: str):
    # Parse the request object to a json object or a text object
    file_name = None
    for key in params:
        file_name = params.get(key) + ".json"
    if response_api_format == "json":
        return response_object.json(), file_name  # Returning a JSON object from API by pulling the data from API
    return response_object.text()  # Returning the data from API by pulling the data


def collect_data(file_name, path, response_api):
    # Store a requests object to a text file

    file_path = path + "/" + file_name
    if os.path.exists(file_path):
        print('file already exists')
    else:
        # create a text file
        with open(file_path, 'w') as fp:
            # Converting the Python objects into appropriate JSON object before store it to a file
            fp.write(json.dumps(response_api))


def convert_to_csv(response_dict, path, csv_file_name):
    # Convert a JSON string (dumps) to a Xls file
    pd.read_json(json.dumps(response_dict)).to_csv(path + "/" + csv_file_name)


def convert_to_excel(response_dict, path, excel_file_name):
    # Convert a JSON string (dumps) to a cvs file
    pd.read_json(json.dumps(response_dict)).to_excel(path + "/" + excel_file_name)


def convert_to_file(response_dict, path, file_name):
    # Get the file type by unpacking the tuple
    file_name, file_extension = os.path.splitext(file_name)
    if file_extension == ".csv":
        file_name += ".csv"
        convert_to_csv(response_dict, path, file_name)
    if file_extension == ".xlsx":
        file_name += ".xlsx"
        convert_to_excel(response_dict, path, file_name)


def convert_to_mysql(mysql_obj: MysqlWrapper, table_name, response_dict):
    # Convert dataframe to sql table for MySQL

    try:
        # Getting the connection object (ENGINE) for the MySQL Database
        # Connecting to the specific MySQL Database
        mysql_obj.connect()

        # Writing the dataframe to the MySQL Database
        pd.read_json(json.dumps(response_dict)).to_sql(table_name, mysql_obj.connect(), if_exists='append', index=False)

    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)


def convert_to_sqlite(sqlite3_conn, sqlite3_table, response_dict):
    # Convert dataframe to sql table for SQLITE3
    try:

        pd.read_json(json.dumps(response_dict)).to_sql(name=sqlite3_table, con=sqlite3_conn)

    except sqlite3.OperationalError:
        print("No such table: " + sqlite3_table)


def read_from_mysql(mysql_conn, mysql_table):
    # Read a dataframe from a MySQL database
    return pd.read_sql_query("SELECT * from " + mysql_table, mysql_conn)


def read_from_sqlite3(sqlite3_conn, sqlite3_table):
    # Read a dataframe from a SQLITE3 database
    return pd.read_sql_query("SELECT * from " + sqlite3_table, sqlite3_conn)


# Define main() function for auto test
def main():  # Driver code

    # GETTING THE API DATA
    url = "https://api.datamuse.com/words"

    # Setting the global object
    params, headers = {"rel_rhy": "forgetful"}, {}
    sqlite3_obj = None
    mysql_obj = None

    # Getting the JSON Object
    response_object = http_fetcher(url, params=params, headers=headers)

    # Formatting the API data / JSON Object
    response_dict, json_file_name = format_response(response_object, params, "json")
    collect_data(json_file_name, "files", response_dict)

    # Converting a json set TO CSV
    csv_file_name = json_file_name[:len(json_file_name) - 5] + ".csv"
    convert_to_file(response_dict, "files", csv_file_name)

    # Converting a json set to XLS
    excel_file_name = json_file_name[:len(json_file_name) - 6] + ".xlsx"
    convert_to_file(response_dict, "files", excel_file_name)

    # Converting a json set TO SQLITE3
    try:
        # Creating the SQLITE3 object
        sqlite3_obj = Sqlite3Wrapper(filename_path="files/api.db", table="table_01")
        convert_to_sqlite(sqlite3_obj.active_connection, sqlite3_obj.table, response_dict)
        # Printing the SQLITE3 API data stored
        print(read_from_sqlite3(sqlite3_obj.active_connection, sqlite3_obj.table))

    except Exception as e:  # Catch an exception and save its error message
        print("Oops!", e.__class__, "occurred.")
        print(e)
    finally:
        sqlite3_obj.close()

    # Converting a json set TO MYSQL
    try:
        # Creating the SQL Alchemy object
        mysql_obj = MysqlWrapper(host='192.168.68.112',  # Remote MySQL Server
                                 user='azizdasilva',
                                 password='administrator',
                                 port=3306,
                                 database='api_storage')

        convert_to_mysql(mysql_obj, "table_01", response_dict)
        # Printing the MySQL API data stored
        print(read_from_mysql(mysql_obj.connect(), "table_01"))

    except Exception as e:  # Catch an exception and save its error message
        print("Oops!", e.__class__, "occurred.")
        print(e)

    finally:
        mysql_obj.close()


if __name__ == '__main__':
    # Execute main() function in standalone mode
    main()
