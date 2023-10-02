""" this module is used to set connections and close
them, using the credentials in configs"""

import cx_Oracle
import yaml


class DBConnection:
    """ 
    this is the class responsible for connecting
    to the oracle database using the credentials
    """
    def __init__(self):
        self.connection = None

    def connect(self):
        """ function to create a connection object"""
        if not self.connection:
            with open('configs/credentials.yaml', 'r', encoding='utf-8') as yaml_file:
                credentials = yaml.safe_load(yaml_file)

            dsn = cx_Oracle.makedsn(
                credentials['hostname'],
                credentials['port'],
                service_name=credentials['service_name']
            )

            self.connection = cx_Oracle.connect(
                user=credentials['username'],
                password=credentials['password'],
                dsn=dsn
            )
            print("connected successfully")

        return self.connection

    def close(self):
        """ close the connection """
        if self.connection:
            self.connection.close()
            self.connection = None
            print("connection closed")
