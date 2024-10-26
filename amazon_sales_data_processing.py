import s3fs
import requests
import pandas as pd
from airflow import DAG
from pyspark.sql import SparkSession
from datetime import timedelta, datetime
from pyspark.sql.functions import col, regexp_replace, desc
from airflow.operators.python_operator import PythonOperator
   
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'amazon_sales_pyspark_processing_dag',
    default_args=default_args,
    description='Pyspark operation on Amazon Sales Dataset',
    schedule_interval='@daily',
)

# Task 1: Extract Data  
def extract_data():
    response = requests.get('https://krish-airflowpro.s3.ap-south-1.amazonaws.com/amazon.csv')
    response.raise_for_status()
    with open('/tmp/amazon_sales.csv', 'wb') as file:
        file.write(response.content)

# Create Spark Session
def create_spark_session():
    return SparkSession.builder \
        .appName("Amazon_Data") \
        .getOrCreate()

# Task 2: Read Amazon Data
def read_data(ti):
    spark = create_spark_session()
    df = spark.read.csv('/tmp/amazon_sales.csv', header=True, inferSchema=True)
    df = df.select(["product_id", "product_name", "category", "discounted_price", "actual_price", "discount_percentage", "rating", "rating_count"])  # Remove unwanted columns
    # Convert Spark DataFrame to Pandas and save as CSV
    df.toPandas().to_csv('/tmp/amazon_data.csv', index=False)
    ti.xcom_push(key='amazon_data_path', value='/tmp/amazon_data.csv')

# Task 3: Filter products with discount > 20
def process_task_1(ti):
    data_path = ti.xcom_pull(task_ids='read_amazon_data', key='amazon_data_path')
    spark = create_spark_session()
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    df = df.withColumn('discount_percentage', regexp_replace(col('discount_percentage'), r'%', '').cast("int"))
    result1_df = df.filter(col('discount_percentage') > 20)
   
    result1_df.toPandas().to_csv('/tmp/task1_output.csv', index=False)
    ti.xcom_push(key='task1_output_path', value='/tmp/task1_output.csv')

# Task 4: Calculate the Total Discount
def process_task_2(ti):
    data_path = ti.xcom_pull(task_ids='process_data_task_1', key='task1_output_path')
    spark = create_spark_session()
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Convert prices to numeric and calculate total discount
    result2_df = df.withColumn("actual_price_int", regexp_replace(df["actual_price"], r'[^0-9.]', '').cast("int")) \
                   .withColumn("discounted_price_int", regexp_replace(df["discounted_price"], r'[^0-9.]', '').cast("int"))

    result2_df = result2_df.withColumn("total_discount", col("actual_price_int") - col("discounted_price_int"))
    result2_df = result2_df.select("product_id", "actual_price_int", "discounted_price_int", "total_discount").toPandas()
    
    # Save to CSV
    result2_df.to_csv('/tmp/task2_output.csv', index=False)
    ti.xcom_push(key='task2_output_path', value='/tmp/task2_output.csv')

# Task 5: Sort products by popularity
def process_task_3(ti):
    data_path = ti.xcom_pull(task_ids='process_data_task_1', key='task1_output_path')
    spark = create_spark_session()
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    result3_df = df.withColumn("popular_product", regexp_replace(df["rating_count"], r'[^0-9.]', '').cast("int"))
    result3_df = result3_df.select(["product_id", "popular_product"]).sort(desc('popular_product')).toPandas()

    # Save to CSV
    result3_df.to_csv('/tmp/task3_output.csv', index=False)
    ti.xcom_push(key='task3_output_path', value='/tmp/task3_output.csv')

# Task 6: Combine all outputs
def combine_results(ti):
    spark = create_spark_session()

    # Pull file paths from XCom
    task1_output_path = ti.xcom_pull(task_ids='process_data_task_1', key='task1_output_path')
    task2_output_path = ti.xcom_pull(task_ids='process_data_task_2', key='task2_output_path')
    task3_output_path = ti.xcom_pull(task_ids='process_data_task_3', key='task3_output_path')

    # Read each CSV file
    df1 = spark.read.csv(task1_output_path, header=True, inferSchema=True)
    df2 = spark.read.csv(task2_output_path, header=True, inferSchema=True)
    df3 = spark.read.csv(task3_output_path, header=True, inferSchema=True)

    # Join on 'product_id'
    combined_df = df1.join(df2, on='product_id', how='inner').join(df3, on='product_id', how='inner')
    combined_df = combined_df.dropDuplicates()
    combined_df = combined_df.select(["product_id", "product_name", "category", "actual_price_int", "discounted_price_int", "discount_percentage", "total_discount", "rating", "popular_product"]).sort(desc('popular_product'))
    
    # Save final output to CSV
    output_file_path = '/tmp/final_amazon_sales_data.csv'
    combined_df.toPandas().to_csv(output_file_path, index=False)
    ti.xcom_push(key='final_output_path', value=output_file_path)

# Task 7: Upload to S3 using s3fs
def upload_data_s3(ti):
    data_path = ti.xcom_pull(task_ids='merge_results', key='final_output_path')
    df = pd.read_csv(data_path)
    df.to_csv("s3://final-amazon-sales-data-pyspark-bucket/final_amazon_sales_data.csv", index=False)
   

# Define Airflow Tasks
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

read_task = PythonOperator(
    task_id='read_amazon_data',
    python_callable=read_data,
    dag=dag,
)

process_task1 = PythonOperator(
    task_id='process_data_task_1',
    python_callable=process_task_1,
    dag=dag,
)

process_task2 = PythonOperator(
    task_id='process_data_task_2',
    python_callable=process_task_2,
    dag=dag,
)

process_task3 = PythonOperator(
    task_id='process_data_task_3',
    python_callable=process_task_3,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge_results',
    python_callable=combine_results,
    dag=dag,
)

upload_s3_task = PythonOperator(
    task_id='upload_data_s3',
    python_callable=upload_data_s3,
    dag=dag,
)

# Task Dependencies
extract_data >> read_task >> [process_task1, process_task2, process_task3] >> merge_task >> upload_s3_task