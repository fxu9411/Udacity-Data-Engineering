from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 query="",
                 delete_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.query = query
        self.delete_load = delete_load


    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info('This table is {0}'.format(self.table))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.delete_load:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            
        sql_statement = f"INSERT INTO {self.table} \n {self.query}"
        redshift_hook.run(sql_statement)
        logging.info(f"Executed:\n{sql_statement}")
