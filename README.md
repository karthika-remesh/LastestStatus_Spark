## LastestStatus_Spark

# Introduction
This Spark application is designed to process input CSV data and extract the latest status information for a given cei_code and p_key from the data. The latest status is determined based on the provided epoch timestamp.

# Prerequisites
Before running the Spark application, make sure you have the following prerequisites:

Apache Spark installed and configured.
A valid input CSV file containing data with the following columns:
cei_code: The CEI code.
d_name: Display name.
u_name: User name.
cei_status: Status (e.g., 'success', 'failed').
updated_at: Timestamp in the format 'yyyy-MM-dd HH:mm:ss.SSS'.
A JSON configuration file that specifies the cei_code and p_key to filter the data.
The epoch timestamp in seconds (e.g., 1630857600999) that represents the cutoff time for filtering the latest data.

# Usage
   To run the Spark application, follow these steps:

Ensure you have the prerequisites mentioned above.

Build the Spark application and package it as a JAR file.

Run the Spark application using the following command:

# Submit Spark Job
```spark-submit --class Main --master <master-url> <path-to-jar> <input-csv-file> <output-directory> <config-json-file> <epoch-timestamp>```
<br/><master-url>: The Spark master URL.
<br/><path-to-jar>: The path to the JAR file created in step 2.
<br/><input-csv-file>: The path to the input CSV file.
<br/><output-directory>: The directory where the output will be written.
<br/><config-json-file>: The path to the JSON configuration file containing cei_code and p_key.
<br/><epoch-timestamp>: The epoch timestamp (in seconds) for filtering the latest data.
The application will process the input data and generate output files in the specified output directory.

# Output
The output will be a set of CSV files, one for each cei_code and p_key combination specified in the JSON configuration file. Each output file will contain the latest status information based on the provided epoch timestamp.

# Example
Here's an example command to run the Spark application:


spark-submit --class Main --master local[2] my-spark-app.jar input.csv output/ config.json 1630857600999
This command will process the input.csv file, apply the filter specified in config.json, and generate output files in the output/ directory based on the epoch timestamp 1630857600.
