# Violation Date Count - PySpark Application - Taylor Clark

This PySpark script processes parking violation data and outputs the date (month and day) with the highest number of violations.

## Requirements

- Apache Spark (PySpark)
- Python 3.x
- CSV file with a column `issue_date` in `yyyy-MM-ddTHH:mm:ss.sss` format

## Installation

1. Install PySpark:

   ```bash
   pip install pyspark

2. Prepare a CSV file with a column issue_date in the required format.

## Running the Application

Change the parallelism level as needed

## Output
The script outputs the month and day with the highest number of violations:
"MM-dd": count

Example:
"06-15": 150

