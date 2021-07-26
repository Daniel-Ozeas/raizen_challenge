import psycopg2
from airflow.hooks.postgres_hook import PostgresHook

def conection_db(user, password, dbname, host, port):
    """
    Create the connection and the cursor to the Postgres db 
    Parameters
    user:str
    password:str
    host:str
    port:str
    dbname:str
    """
    try:
        conn = psycopg2.connect(user=user, password=password, host=host, port=port, dbname=dbname)
        cur = conn.cursor()
        print('Connection and cursor was created.')
        return conn, cur

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error while connecting to PostgreSQL db:', error)

def create_table(conn, cur, query):
    """
    Used to create a table in db
    Parameters:
    conn (class: psycopg2.extensions.connection): connector to db
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    query (str): the sql query 
    Returns:
    Nothing. Only execute the command.
    """
    try:
        cur.execute(query)
        conn.commit()
        print("Table created successfully in PostgreSQL")

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error while creating PostgreSQL table:', error) 

def insert_data(conn, cur, query, df):
    """
    Used to insert data into a table in postgresdb
    Parameters:
    conn (class: psycopg2.extensions.connection): connector to db
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    query (str): the sql query 
    df (list): data cleaned
    Returns:
    Nothing. Only execute the command.
    """

    try:
        cur.executemany(query, df)
        conn.commit()

        print('Data was inserted sucessfully')
        
    except (Exception, psycopg2.Error) as error:
        print('Error while inserting data:', error)

def creating_index(conn, cur, index_columns):
    """
    Create the index in postgres table 
    Parameters: 
    conn (class: psycopg2.extensions.connection): connector to Postgres database
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    index_columns (list): list of desired indexe column 
    Returns:
    Nothing. Just execute the command.
    """
    try:        
        for index_column in index_columns:
            index_query = f"""
            CREATE INDEX idx_{index_column} 
            ON tb_anp_fuel_sales({index_column});
            """
            cur.execute(index_query)
            conn.commit()
            print(f'Index idx_{index_column} created sucessfully')
        
    except (Exception, psycopg2.Error) as error:
        print('Error while creating index:', error)   