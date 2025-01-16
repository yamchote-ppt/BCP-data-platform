class AuditLog:
    def __init__(self, job_date_time, processing_notebook, staging_table_name, staging_table_path, fact_table_name, fact_table_path, mismatch_table, additional_info=None):
        self.job_date_time = job_date_time
        self.processing_notebook = processing_notebook
        self.staging_table_name = staging_table_name
        self.staging_table_path = staging_table_path
        self.fact_table_name = fact_table_name
        self.fact_table_path = fact_table_path
        self.mismatch_table = mismatch_table
        self.additional_info = additional_info or {}

    def save_log(self, storage_path):
        log_entry = {
            "job_date_time": self.job_date_time,
            "processing_notebook": self.processing_notebook,
            "staging_table_name": self.staging_table_name,
            "staging_table_path": self.staging_table_path,
            "fact_table_name": self.fact_table_name,
            "fact_table_path": self.fact_table_path,
            "mismatch_table": self.mismatch_table,
            "additional_info": self.additional_info
        }
        with open(storage_path, 'a') as log_file:
            log_file.write(f"{log_entry}\n")