"""
DAG para executar o job Spark de ETL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

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
    
    # Task 1: Início do pipeline
    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )
    
    # Task 2: Executar ETL Job no Spark
    # No Airflow 3.x, o master deve ser configurado via spark_conn_id ou conf
    run_spark_etl = SparkSubmitOperator(
        task_id='run_spark_etl',
        application='/opt/spark-apps/etl_job.py',
        name='ETL_Vendas_Job',
        verbose=True,
        # Usar spark_conn_id (requer criar conexão no UI) OU conf
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.executor.cores': '2',
            'spark.executor.memory': '1g',
        }
    )
    
    # Task 3: Verificar se os arquivos de saída foram gerados
    check_output = BashOperator(
        task_id='check_output',
        bash_command='ls -la /opt/spark-apps/output/ | head -20 || echo "Nenhum arquivo encontrado"',
        do_xcom_push=False,
    )
    
    # Task 4: Mostrar resumo do job
    show_summary = BashOperator(
        task_id='show_summary',
        bash_command='echo "ETL Job concluído em $(date)"',
        do_xcom_push=False,
    )
    
    # Task 5: Fim do pipeline
    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )
    
    # Definir dependências
    start_pipeline >> run_spark_etl >> check_output >> show_summary >> end_pipeline