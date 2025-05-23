# ETL Project

This project implements an ETL (Extract, Transform, Load) process for processing data and loading it into fact tables. It includes functionality for logging the details of the ETL process through an `AuditLog` class.

## Project Structure

```
etl-project
├── src
│   ├── etl
│   │   ├── __init__.py
│   │   ├── factPos.py
│   │   └── audit_log.py
├── tests
│   ├── __init__.py
│   └── test_audit_log.py
├── requirements.txt
└── README.md
```

## Files Overview

- **src/etl/__init__.py**: Marks the directory as a Python package. Can be used to initialize package-level variables or import specific classes or functions.

- **src/etl/factPos.py**: Contains the main ETL logic for processing data and loading it into fact tables. Includes classes and methods for data transformation and loading.

- **src/etl/audit_log.py**: Contains the `AuditLog` class, which records details of the ETL process, including job date and time, processing notebook, staging table name and path, fact table name and path, mismatch table, and other related information. It also includes methods for saving log entries to persistent storage.

- **tests/__init__.py**: Marks the directory as a Python package for testing. Can be used to initialize package-level variables or import specific test cases.

- **tests/test_audit_log.py**: Contains unit tests for the `AuditLog` class, ensuring that it correctly records and saves the ETL process details.

- **requirements.txt**: Lists the dependencies required for the project, including any libraries needed for the ETL process and logging.

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd etl-project
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

To run the ETL process, execute the `factPos.py` script. Ensure that the necessary configurations are set up in the script.

## Audit Logging

The `AuditLog` class provides functionality to log the details of each ETL job, which can be useful for tracking and debugging purposes. Make sure to implement the necessary methods in `audit_log.py` to save the logs appropriately.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.#   B C P - d a t a - p l a t f o r m  
 