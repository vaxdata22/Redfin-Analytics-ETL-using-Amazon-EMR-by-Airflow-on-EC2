
# Import the necessary libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor


# Define the specifications of the EMR cluster to be created for the ETL jobs
JOB_FLOW_OVERRIDES = {
    "Name": "redfin-analytics-etl-emr-cluster",
    "ReleaseLabel": "emr-7.8.0",
    "Applications": [{"Name": "Spark"}],
    "LogUri": "s3://redfin-analytics-emr-etl-bucket/emr-logs/",    # This S3 bucket should be created already
    "VisibleToAllUsers":False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",    # Spot instances are a "use as available" instances
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",    # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 3,
            },
        ],          
        "Ec2SubnetId": "subnet-xxxxxxxxxxxxxxxxx",    # One of the subnets in a VPC created for the ETL project
        "Ec2KeyName" : 'vaxdata22-etl-data-pipeline-user',    # In case there is need to SSH into the EMR cluster
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,    # Setting this as false will allow us to programmatically terminate the cluster
    },
    "JobFlowRole": "redfin-analytics-emr-ec2-service-role",    # This is an IAM Role ("EMR Role for EC2" use case) created via the AWS Console
    "ServiceRole": "redfin-analytics-emr-service-role"    # This is an IAM Role ("EMR" use case) created via the AWS Console
}


# Define the Data Extraction job step
SPARK_STEPS_EXTRACTION = [
    {
        "Name": "Extract Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://af-south-1.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [
                "s3://redfin-analytics-emr-etl-bucket/emr-etl-scripts/redfin_data_ingest_bash_script.sh",
            ],
        },
    }
   ]


# define the Data Transformation job step
SPARK_STEPS_TRANSFORMATION = [
    {
        "Name": "Transform Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
            "s3://redfin-analytics-emr-etl-bucket/emr-etl-scripts/redfin_data_transform_pyspark_script.py",
            ],
        },
    }
   ]


# Define the default agrguments and their parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 7), 
    'email': ['donatus.enebuse@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=4)
}


# Define the orchestration DAG and the tasks in the form of operators and sensors
with DAG('redfin_analytics_etl_emr_spark_job_dag',
        default_args=default_args,
        schedule_interval = '@monthly',
        catchup=False) as dag:

        # Signify the beginning of the ETL pipeline
        start_pipeline = DummyOperator(task_id="tsk_start_pipeline")

        # Create an EMR cluster that would be used for the ETL jobs
        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="tsk_create_emr_cluster",
            job_flow_overrides=JOB_FLOW_OVERRIDES,    # Pass the specifications of the EMR cluster to be created
            aws_conn_id="aws_new_conn"
        )

        # Confirm that the EMR is successfully created and is ready to be used
        is_emr_cluster_created = EmrJobFlowSensor(
        task_id="tsk_is_emr_cluster_created", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        target_states={"WAITING"},    # Specify the desired state for the sensor to report
        timeout=3600,    # This duration is quite too much but it's not that bad
        poke_interval=5,
        aws_conn_id="aws_new_conn",
        mode='poke'    # Since this is a job flow sensor, it needs to poke the target for info
        )

        # Add the Data Extraction job step to the EMR cluster
        add_extraction_step = EmrAddStepsOperator(
        task_id="tsk_add_extraction_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_new_conn",
        steps=SPARK_STEPS_EXTRACTION
        )

        # Confirm that the Data Extraction job step is successfully executed
        is_extraction_completed = EmrStepSensor(
        task_id="tsk_is_extraction_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='tsk_add_extraction_step')[0] }}",
        target_states={"COMPLETED"},    # Specify the desired state for the sensor to report
        timeout=3600,    # This duration is enough to let the task complete before time out
        aws_conn_id="aws_new_conn",
        poke_interval=5
        )

        # Add the Data Transformation job step to the EMR cluster
        add_transformation_step = EmrAddStepsOperator(
        task_id="tsk_add_transformation_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_new_conn",
        steps=SPARK_STEPS_TRANSFORMATION
        )

        # Confirm that the Data Transformation job step is successfully executed
        is_transformation_completed = EmrStepSensor(
        task_id="tsk_is_transformation_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='tsk_add_transformation_step')[0] }}",
        target_states={"COMPLETED"},    # Specify the desired state for the sensor to report
        timeout=3600,    # This duration is enough to let the task complete before time out
        aws_conn_id="aws_new_conn",
        poke_interval=10
        )

        # Terminate the EMR cluster since the ETL jobs are completed successfully
        remove_cluster = EmrTerminateJobFlowOperator(
        task_id="tsk_remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",            
        aws_conn_id="aws_new_conn"
        )

        # Confirm that the EMR cluster is successfully terminated
        is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="tsk_is_emr_cluster_terminated", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        target_states={"TERMINATED"},    # Specify the desired state for the sensor to report
        timeout=3600,    # This duration is quite too much but it's not that bad
        poke_interval=5,
        aws_conn_id="aws_new_conn",
        mode='poke'    # Since this is a job flow sensor, it needs to poke the target for info
        )
        
        # Signify the ending of the ETL pipeline
        end_pipeline = DummyOperator(task_id="tsk_end_pipeline")

        # State the DAG sequence for the orchestration
        start_pipeline >> create_emr_cluster >> is_emr_cluster_created >> add_extraction_step >> is_extraction_completed
        is_extraction_completed >> add_transformation_step >> is_transformation_completed >> remove_cluster
        remove_cluster >> is_emr_cluster_terminated >> end_pipeline
