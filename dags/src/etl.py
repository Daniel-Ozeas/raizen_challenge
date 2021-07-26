import pandas as pd
from src.database import conection_db, create_table, insert_data, creating_index
from sql_queries.sql_queries import create_table_query, insert_query

# Database Info
host        = 'postgres'
port        = '5432'
dbname      = 'anp'
tablename   = 'tb_anp_fuel_sales'
password    = 'airflow'
username    = 'airflow'
index_colum = ['year_month', 'product']

# Extract
def extract_data(path, **kwargs):
    xls = pd.read_excel(path, sheet_name=['DPCache_m3', 'DPCache_m3_2', 'DPCache_m3_3'], engine='openpyxl')
    df = pd.concat(xls, ignore_index=True)
    print('The data was extracted sussecfully')
    return df

# Transform data
def transform_data(df, **kawargs):
    """Transform xlsx data file"""
 
    # Transform Month Columns into rows columns
    value_vars = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']

    df_month_value = pd.melt(df,
                            id_vars=['COMBUSTÍVEL','ANO', 'ESTADO'],
                            value_vars=value_vars,
                            var_name='MONTH',
                            value_name='volume')
    
    # Change each month to month of the year
    look_up = {'Jan':'01', 'Fev':'02', 'Mar':'03', 'Abr':'04', 'Mai':'05','Jun':'06','Jul':'07','Ago':'08','Set':'09','Out':'10','Nov':'11','Dez':'12'}
    df_month_value['MONTH'] = df_month_value['MONTH'].apply(lambda x: look_up[x])
    
    # Create year-month column with only year and month
    df_month_value['year-month'] = pd.to_datetime(df_month_value.ANO.astype(str) + '/' + df_month_value.MONTH.astype(str) + '/' + "1").dt.date
    
    # Split unit from COMBUSTIVEL column
    df_month_value[['COMBUSTÍVEL', 'unit']] = pd.DataFrame(df_month_value
                                                         .COMBUSTÍVEL
                                                         .str
                                                         .split('(',1)
                                                         .tolist(), columns = ['COMBUSTÍVEL','unit'])
    df_month_value['unit'] = df_month_value['unit'].str.replace(')', '')
    
    # Final 
    final_df = df_month_value[['year-month', 'ESTADO', 'COMBUSTÍVEL', 'unit', 'volume']]
    final_df.columns = ['year-month', 'uf', 'product', 'unit', 'volume']
    
    # Change data type from year-month column
    final_df['year-month'] = final_df['year-month'].astype(str)

    # Fill NaN Values 
    final_df['volume'] = final_df['volume'].fillna(0)

    df_to_load = list(final_df.itertuples(index=False, name=None))
    print('The data was transformed successfully')
    return df_to_load
    
def load_data_into_db(conn, cur, cleaned_data):
    
    insert_data(conn, cur, insert_query, cleaned_data)
    print("Data was inserted successfully in PostgresDB")  	


def etl():
    conn, cur = conection_db(username,password,dbname,host,port)

    create_table(conn, cur, create_table_query)
    
    df = extract_data('./data/vendas-combustiveis-m3.xlsx')
    
    transformed_data = transform_data(df)
    
    load_data_into_db(conn, cur, transformed_data)
    
    creating_index(conn, cur, index_colum)

if __name__ == "__main__":
    etl()

