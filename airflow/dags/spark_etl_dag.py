"""
DAG para executar o job Spark de ETL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL com Spark - Dados de Vendas',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'etl', 'vendas'],
) as dag:
    
    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )
    
    # Executa o Spark Submit diretamente via Bash
    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command="""
            spark-submit \
                --master spark://spark-master:7077 \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.sql.adaptive.coalescePartitions.enabled=true \
                --conf spark.executor.cores=2 \
                --conf spark.executor.memory=1g \
                --name ETL_Vendas_Job \
                --verbose \
                /opt/spark-apps/etl_job.py
        """,
    )
    
    check_output = BashOperator(
        task_id='check_output',
        bash_command='ls -la /opt/spark-apps/output/ | head -20 || echo "Nenhum arquivo encontrado"',
        do_xcom_push=False,
    )
    
    show_summary = BashOperator(
        task_id='show_summary',
        bash_command='echo "ETL Job concluído em $(date)"',
        do_xcom_push=False,
    )
    
    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )
    
    start_pipeline >> run_spark_etl >> check_output >> show_summary >> end_pipeline