from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_SUBMIT = r"""
spark-submit \
    --master local[*] \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.executor.heartbeatInterval=120s \
    --conf spark.network.timeout=600s \
    --conf spark.sql.warehouse.dir=/opt/spark-warehouse \
    --conf spark.driver.memory=3g \
    --conf spark.executor.memory=3g \
    --conf spark.sql.shuffle.partitions=4 \
    /opt/spark-apps/agendamentos/{script} --etapa {etapa}
"""

def spark_task(task_id, script, etapa, **kwargs):
    return BashOperator(
        task_id=task_id,
        bash_command=SPARK_SUBMIT.format(script=script, etapa=etapa),
        execution_timeout=timedelta(minutes=60),
        **kwargs,
    )

with DAG(
    "spark_etl_job",
    default_args=default_args,
    description="ETL Job com Spark",
    schedule="0 6 26 * *",   # todo dia 26 às 06:00
    catchup=False,
    tags=["spark", "etl"],
) as dag:

    with TaskGroup("marque_facil") as marque_facil:
        raw = spark_task("raw", "marqueFacil.py", "raw", retries=3)  # API é a parte frágil
        silver = spark_task("silver", "marqueFacil.py", "silver")
        gold = spark_task("gold", "marqueFacil.py", "gold")
        raw >> silver >> gold

    with TaskGroup("opera") as opera:
        raw_o = spark_task("raw", "operaAdulto.py", "raw", retries=3)
        silver_o = spark_task("silver", "operaAdulto.py", "silver")
        gold_o = spark_task("gold", "operaAdulto.py", "gold")
        raw_o >> silver_o >> gold_o

    marque_facil >> opera
