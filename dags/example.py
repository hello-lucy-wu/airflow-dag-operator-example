import datetime

from airflow import DAG

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#
args = {
    'start_date': datetime.datetime.utcnow(),
}
with DAG(dag_id='lesson3.exercise4', default_args=args) as dag:

    copy_trips_task = S3ToRedshiftOperator(
        task_id="copy_trips",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="trips",
        s3_bucket="udacity-dend",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
    )

    check_trips_task = HasRowsOperator(
        task_id="check_trips",
        redshift_conn_id="redshift",
        table="trips"
    )


    calculate_facts_task = FactsCalculatorOperator(
        task_id="calculate_facts",
        redshift_conn_id="redshift",
        origin_table="trips",
        destination_table="facts",
        fact_column="tripduration",
        groupby_column="bikeid",
    )

    copy_trips_task >> check_trips_task >> calculate_facts_task


   