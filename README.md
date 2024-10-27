# Amazon-Sales-Data-Analysis-and-Processing-using-PySpark-and-Airflow-Project
The Amazon Sales Data Analysis and Processing using PySpark and Airflow. This pipeline processes an Amazon sales dataset by applying transformations to filter, analyze, and sort products. The final processed data is uploaded to an S3 bucket for storage and further analysis.

## Introduction
The Amazon Sales Data Analysis and Processing project leverages cloud and big data technologies to efficiently analyze Amazon sales data. Utilizing AWS services like S3 and EC2, alongside Apache Airflow and PySpark, this project automates the filtering of products with over 20% discounts, computes total discount values, and ranks products by popularity based on ratings and review count. Results are combined into a final output file and stored in S3, enabling scalable storage and streamlined data access. This project highlights practical applications of data engineering tools for processing large datasets in real time.


## Architecture 
![Project Architecture](https://github.com/Bornarekrishna/Amazon-Sales-Data-Analysis-and-Processing-using-PySpark-and-Airflow-Project/blob/main/Architecture.png)


## Technology Used
1. AWS S3: AWS offers a scalable cloud storage solution that allows you to store and retrieve data efficiently.
2. AWS EC2: AWS provides Elastic Compute Cloud (EC2) as a flexible virtual server solution, enabling users to run applications in the cloud with scalable processing power.
3. Apache Airflow: Apache Airflow is a powerful workflow automation tool for designing, scheduling, and monitoring complex data pipelines. It allows users to define tasks as Directed Acyclic Graphs (DAGs), enabling efficient workflow management and execution while supporting scalability and real-time task orchestration across projects.
4. PySpark: PySpark is the Python interface for Apache Spark, enabling scalable, fast data processing across distributed systems, ideal for handling large datasets through efficient filtering, transformation, and aggregation.

## Project Execution Flow

1. Extract Data
Process: Trigger the extract_data task to download the raw Amazon sales data from a to specified location to the local file system as /tmp/amazon_sales.csv.
Output: Stores raw data for initial processing.

2. Trigger Data Processing Pipeline (Daily)
Process: A scheduled Airflow DAG runs daily, triggering the following data processing tasks.
Output: Initiates the end-to-end data transformation and analysis pipeline.

3. Read and Select Columns
Task: read_amazon_data
Operation: Spark reads the CSV file, selecting essential columns (e.g., product details, pricing, ratings) and stores this refined data for further processing.
Output: Intermediate processed data stored in /tmp/amazon_data.csv.

4. Trigger Transformation Tasks
Task: Process each transformation on the dataset:
process_data_task_1: Filters products with more than 20% discount.
process_data_task_2: Calculates total discount amounts.
process_data_task_3: Sorts products based on popularity by rating_count.
Output: For merging, each task saves results as separate files (task1_output.csv, task2_output.csv, task3_output.csv).

5. Combine Transformation Results
Task: merge_results
Operation: Joins outputs from each transformation task on product_id, removing duplicates and sorting the final data by popularity.
Output: Final processed data stored in /tmp/final_amazon_sales_data.csv.

6. Load Final Output into S3
Task: upload_data_s3
Operation: The final output is uploaded to a designated S3 bucket (final-amazon-sales-data-pyspark-bucket) for storage and further analysis.
Output: Data available for analysis in S3, ready for further ingestion or querying.

# Successfully Completed Workflow
![Workflow](https://github.com/Bornarekrishna/Amazon-Sales-Data-Analysis-and-Processing-using-PySpark-and-Airflow-Project/blob/main/Workflow_task_done.png)
