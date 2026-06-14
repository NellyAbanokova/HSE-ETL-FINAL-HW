import datetime
import uuid

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_BUCKET = "nelly-etl-final-2026"
YC_ZONE = "ru-central1-a"

YC_SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFsBAehumnUtWMmG0n4TrQQdOgTLBcYqY2iSq5dIOkJI etl-final"
YC_SERVICE_ACCOUNT_ID = "aje2jdhlgp3nv8od76vh"
YC_SUBNET_ID = "e9b9gittjocta5tknf35"

with DAG(
    dag_id="etl_applications_dataproc_max",
    start_date=datetime.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl-final", "dataproc", "pyspark"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=f"etl-final-dp-{uuid.uuid4()}",
        cluster_description="Temporary Data Processing cluster for ETL final homework",
        cluster_image_version="2.1",
        ssh_public_keys=[YC_SSH_PUBLIC_KEY],
        subnet_id=YC_SUBNET_ID,
        service_account_id=YC_SERVICE_ACCOUNT_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_ZONE,
        services=["YARN", "SPARK"],
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-ssd",
        masternode_disk_size=100,
        datanode_count=0,
        computenode_count=1,
        computenode_resource_preset="s2.small",
        computenode_disk_type="network-ssd",
        computenode_disk_size=100,
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_applications_etl",
        main_python_file_uri=f"s3a://{YC_BUCKET}/scripts/process_applications_max.py",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_pyspark >> delete_cluster