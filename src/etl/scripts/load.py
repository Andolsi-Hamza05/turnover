import pandas as pd
from sqlalchemy import create_engine
from configs.postgresql import database_url


def load_dataframe_to_postgresql(dataframe, table_name):
    """
    Load a DataFrame into a PostgreSQL table.

    Parameters:
    - dataframe: The DataFrame to be loaded.
    - table_name: The name of the table to be created in PostgreSQL.

    Returns:
    None
    """
    # Create a connection to PostgreSQL
    engine = create_engine(database_url)
    
    # Determine DataFrame column data types
    data_types = dataframe.dtypes
    
    # Define a mapping of pandas data types to SQL data types
    sql_type_mapping = {
        'int64': 'INTEGER',
        'float64': 'NUMERIC',
        'object': 'VARCHAR',
        'datetime64[ns]': 'DATE'
    }
    
    # Generate CREATE TABLE statement with matching data types
    def generate_create_table_statement(table_name, data_types):
        columns = []
        for column, dtype in data_types.items():
            sql_type = sql_type_mapping.get(str(dtype), 'VARCHAR')
            columns.append(f"{column} {sql_type}")
        columns_definition = ', '.join(columns)
        return f"CREATE TABLE {table_name} ({columns_definition})"
    
    # Execute CREATE TABLE statement
    create_table_statement = generate_create_table_statement(table_name, data_types)
    engine.execute(create_table_statement)
    
    # Load data into the table
    dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
