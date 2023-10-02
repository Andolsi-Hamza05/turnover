from ETL.scripts.connection import DBConnection
from ETL.scripts.utils import persist_data
import cx_Oracle
import pandas as pd


def extract_table(proc_name):
    
    db = DBConnection()
    connection = db.connect()

    try:
        cursor = connection.cursor()

        # Execute the stored procedure and pass an output parameter for the ref cursor
        p_cursor = cursor.var(cx_Oracle.CURSOR)
        cursor.callproc(f'{proc_name}', (p_cursor,))

        # Fetch the result set from the ref cursor
        p_cursor_value = p_cursor.getvalue()

        # Convert the result set into a DataFrame
        if p_cursor_value:
            columns = [desc[0] for desc in p_cursor_value.description]
            df = pd.DataFrame(p_cursor_value.fetchall(), columns=columns)
            persist_data(df,proc_name,'raw_data')
            print(f"the data is loaded to {proc_name}")
        else:
            df = pd.DataFrame()
        
        # Close the cursor and connection
        cursor.close()
        connection.close()

        return df

    except cx_Oracle.DatabaseError as e:
        print("Error:", e)
        connection.rollback()
        
        



