from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'spark_etl_job',
    default_args=default_args,
    description='ETL Job com Spark',
    catchup=False,
    tags=['spark', 'etl'],
)

run_spark_etl = BashOperator(
    task_id='Agendamentos',
    bash_command="""
    echo "=== INICIANDO ETL ==="
    echo "Data: $(date)"
    
    # Garantir diretórios
    mkdir -p /opt/spark-apps/output
    mkdir -p /opt/spark-warehouse
    chmod -R 777 /opt/spark-apps
    chmod -R 777 /opt/spark-warehouse
    
    # Usar spark-submit
    spark-submit \
        --master local[*] \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.executor.heartbeatInterval=120s \
        --conf spark.network.timeout=600s \
        --conf spark.sql.warehouse.dir=/opt/spark-warehouse \
        --conf spark.driver.memory=3g \
        --conf spark.executor.memory=3g \
        --conf spark.sql.shuffle.partitions=4 \
        /opt/spark-apps/agendamentos/validados.py
    
    echo "=== ETL FINALIZADO ==="
    """,
    execution_timeout=timedelta(seconds=3600),
    dag=dag,
)