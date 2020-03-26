'''
Helper module for GCP
'''
from google.cloud import bigquery, logging
from google.cloud.logging_v2 import enums
from google.cloud.exceptions import NotFound

class Logger(object):
    '''
    Logger class
    '''
    def __init__(self, log_name):
        self.client = logging.Client()
        self.logger = self.client.logger(log_name)
    
    def info(self, text):
        self.logger.log_text(text, severity=enums.LogSeverity.INFO)

    def error(self, text):
        self.logger.log_text(text, severity=enums.LogSeverity.ERROR)


class BigQueryHelper(object):
    '''
    BigQuery Helper class
    '''
    def __init__(self, logger):
        self.client = bigquery.Client()
        self.logger = logger

    def get_table_ref(self, dataset, table):
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        return table_ref

    def get_table(self, dataset, table):
        try:
            table_ref = self.get_table_ref(dataset, table)
            table = self.client.get_table(table_ref)
            return table
        except NotFound:
            return False
    
    def load_table(self, dataset, table, frame, partition=None, **kwargs):
        '''
        Currently supports only time-based partitioning.
        '''
        # Set job config
        job_config = bigquery.LoadJobConfig(**kwargs)
        # Should not be empty if clustering_fields is provided in kwargs
        if partition:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition)
        # Load table
        table_ref = self.get_table_ref(dataset, table)
        load_job = self.client.load_table_from_dataframe(frame, table_ref, job_config=job_config)
        load_job.result()
        self.logger.info(f'LOAD TABLE -- Destination: {load_job.destination}, Output Rows: {load_job.output_rows}')

    def run_query(self, query, _return=False, partition=None, **kwargs):
        '''
        Currently supports only time-based partitioning.
        '''
        # Set job config
        job_config = bigquery.QueryJobConfig(**kwargs)
        # Should not be empty if clustering_fields is provided in kwargs
        if partition:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition)
        # Query
        query_job = self.client.query(query, job_config=job_config)
        res = query_job.result()
        self.logger.info(f'RUN QUERY -- Query: {query}, Estimated Bytes Processed: {query_job.estimated_bytes_processed}')
        if _return:
            return res.to_dataframe()        
