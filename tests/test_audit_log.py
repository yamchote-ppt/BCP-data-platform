import unittest
from datetime import datetime
from src.etl.audit_log import AuditLog

class TestAuditLog(unittest.TestCase):

    def setUp(self):
        self.audit_log = AuditLog(
            job_date=datetime.now(),
            processing_notebook="test_notebook.ipynb",
            staging_table_name="staging_table",
            staging_table_path="abfss://path/to/staging_table",
            fact_table_name="fact_table",
            fact_table_path="abfss://path/to/fact_table",
            mismatch_table="mismatch_table",
            additional_info={"status": "success"}
        )

    def test_audit_log_initialization(self):
        self.assertIsInstance(self.audit_log.job_date, datetime)
        self.assertEqual(self.audit_log.processing_notebook, "test_notebook.ipynb")
        self.assertEqual(self.audit_log.staging_table_name, "staging_table")
        self.assertEqual(self.audit_log.staging_table_path, "abfss://path/to/staging_table")
        self.assertEqual(self.audit_log.fact_table_name, "fact_table")
        self.assertEqual(self.audit_log.fact_table_path, "abfss://path/to/fact_table")
        self.assertEqual(self.audit_log.mismatch_table, "mismatch_table")
        self.assertEqual(self.audit_log.additional_info, {"status": "success"})

    def test_save_log_entry(self):
        # Assuming save_log_entry method saves the log and returns a success message
        result = self.audit_log.save_log_entry()
        self.assertEqual(result, "Log entry saved successfully.")

if __name__ == '__main__':
    unittest.main()