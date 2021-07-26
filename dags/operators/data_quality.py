
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = '',
                 table = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        
    def execute(self,context):
        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        for table in self.table:
            records = postgres.get_records(f'SELECT COUNT(*) FROM {table};')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed. {table} returned no results')
            if records[0][0] < 1:
                raise ValueError(f'Data quality check failed. {table} returned no results')
            
            logging.info(f'Data quality on table {table} check passed with {records[0][0]} records')

            total_per_product = postgres.get_records(f'SELECT DISTINCT(product), SUM(volume) as total FROM {table} GROUP BY product;')
            logging.info(f'Total por produto inserido:{total_per_product}')
            expected_values = {('ETANOL HIDRATADO ', 429887748.697065), ('GLP ', 482779725.872813), ('GASOLINA C ', 1209811880.678870)}
            
            for i in expected_values:
                for j in total_per_product:
                    if i[0] == j[0] and round(i[1],1) != round(j[1],1):
                        raise ValueError('Somatoria dos dados incorreto para a coluna {i[0]}')
            logging.info(f'Sum Data quality check on table {table} passed.')