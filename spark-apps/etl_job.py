
#!/usr/bin/env python3

"""
ETL Job de exemplo para testar a integração Airflow + Spark.

Este script:

1. Cria dados de exemplo (vendas de uma loja)
2. Processa e transforma os dados usando Spark SQL
3. Gera relatórios de vendas por categoria e produto
4. Salva os resultados em formato Parquet
"""

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    max as spark_max,
    to_date,
    date_format,
    datediff,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

WAREHOUSE_DIR = "/opt/spark-warehouse"
OUTPUT_DIR = "/opt/spark-apps/output"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session(app_name="ETL_Job_Example"):
    """Cria e configura a Spark Session."""

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# GERAÇÃO DOS DADOS
# ============================================================

def generate_sample_data(spark, num_records=1000):
    """
    Gera dados de exemplo de vendas.
    """

    print(f"Gerando {num_records} registros de exemplo...")

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("delivery_date", DateType(), True),
        StructField("status", StringType(), True),
    ])

    categories = [
        "Eletrônicos",
        "Roupas",
        "Alimentos",
        "Livros",
        "Esportes",
    ]

    statuses = [
        "Entregue",
        "Processando",
        "Enviado",
        "Cancelado",
    ]

    customer_names = [
        "João Silva",
        "Maria Santos",
        "Pedro Costa",
        "Ana Lima",
        "Carlos Souza",
        "Mariana Oliveira",
        "Rafael Pereira",
        "Fernanda Alves",
        "Lucas Santos",
        "Juliana Costa",
        "André Lima",
        "Patrícia Souza",
        "Roberto Oliveira",
        "Carla Pereira",
        "Ricardo Alves",
    ]

    product_names = {
        "Eletrônicos": [
            "Smartphone",
            "Notebook",
            "Tablet",
            "Fone Bluetooth",
            "Carregador",
        ],
        "Roupas": [
            "Camiseta",
            "Calça Jeans",
            "Vestido",
            "Jaqueta",
            "Sapato",
        ],
        "Alimentos": [
            "Arroz",
            "Feijão",
            "Macarrão",
            "Café",
            "Açúcar",
        ],
        "Livros": [
            "Romance",
            "Ficção",
            "Ciência",
            "História",
            "Autoajuda",
        ],
        "Esportes": [
            "Bola Futebol",
            "Raquete Tênis",
            "Luvas Boxe",
            "Bicicleta",
            "Óculos Natação",
        ],
    }

    data = []

    start_date = datetime(2024, 1, 1)

    for i in range(num_records):

        category = categories[i % len(categories)]

        product_list = product_names[category]

        order_date = start_date + timedelta(
            days=i % 365,
            hours=i % 24,
            minutes=i % 60,
        )

        delivery_date = (
            order_date + timedelta(days=(i % 7) + 1)
        ).date()

        data.append(
            (
                i + 1,                              # order_id
                (i % 50) + 1,                       # customer_id
                customer_names[i % len(customer_names)],
                (i % 20) + 1,                       # product_id
                product_list[i % len(product_list)],
                category,
                (i % 5) + 1,                        # quantity
                round(10 + (i % 1000) * 0.5, 2),   # price
                order_date,
                delivery_date,
                statuses[i % len(statuses)],
            )
        )

    df = spark.createDataFrame(data, schema)

    return df


# ============================================================
# PROCESSAMENTO
# ============================================================

def process_data(df):
    """
    Processa e transforma os dados.
    """

    print("Processando dados...")

    df_processed = (
        df
        .withColumn(
            "order_year",
            date_format(col("order_date"), "yyyy")
        )
        .withColumn(
            "order_month",
            date_format(col("order_date"), "MM")
        )
        .withColumn(
            "order_day",
            date_format(col("order_date"), "dd")
        )
        .withColumn(
            "total_amount",
            col("quantity") * col("price")
        )
        .withColumn(
            "delivery_time_days",
            datediff(
                col("delivery_date"),
                to_date(col("order_date"))
            )
        )
    )

    return df_processed


# ============================================================
# RELATÓRIOS
# ============================================================

def generate_reports(df):
    """
    Gera relatórios agregados.
    """

    print("Gerando relatórios...")

    reports = {}

    # --------------------------------------------------------
    # 1. Vendas por categoria
    # --------------------------------------------------------

    reports["vendas_por_categoria"] = (
        df
        .groupBy("category")
        .agg(
            spark_sum("total_amount").alias("total_vendas"),
            count("order_id").alias("total_pedidos"),
            avg("total_amount").alias("media_pedido"),
        )
        .orderBy(
            col("total_vendas").desc()
        )
    )

    # --------------------------------------------------------
    # 2. Vendas por mês
    # --------------------------------------------------------

    reports["vendas_por_mes"] = (
        df
        .groupBy(
            "order_year",
            "order_month",
        )
        .agg(
            spark_sum("total_amount").alias("total_vendas"),
            count("order_id").alias("total_pedidos"),
            avg("total_amount").alias("media_pedido"),
        )
        .orderBy(
            "order_year",
            "order_month",
        )
    )

    # --------------------------------------------------------
    # 3. Top 10 produtos mais vendidos
    # --------------------------------------------------------

    reports["top_produtos"] = (
        df
        .groupBy(
            "product_name",
            "category",
        )
        .agg(
            spark_sum("quantity").alias(
                "total_quantidade"
            ),
            spark_sum("total_amount").alias(
                "total_vendas"
            ),
            count("order_id").alias(
                "total_pedidos"
            ),
        )
        .orderBy(
            col("total_vendas").desc()
        )
        .limit(10)
    )

    # --------------------------------------------------------
    # 4. Top 10 clientes
    # --------------------------------------------------------

    reports["top_clientes"] = (
        df
        .groupBy(
            "customer_id",
            "customer_name",
        )
        .agg(
            spark_sum("total_amount").alias(
                "total_gasto"
            ),
            count("order_id").alias(
                "total_pedidos"
            ),
            avg("total_amount").alias(
                "media_pedido"
            ),
            spark_max("order_date").alias(
                "ultima_compra"
            ),
        )
        .orderBy(
            col("total_gasto").desc()
        )
        .limit(10)
    )

    # --------------------------------------------------------
    # 5. Status dos pedidos
    # --------------------------------------------------------

    reports["status_pedidos"] = (
        df
        .groupBy("status")
        .agg(
            count("order_id").alias(
                "quantidade"
            ),
            spark_sum("total_amount").alias(
                "valor_total"
            ),
        )
        .orderBy("status")
    )

    # --------------------------------------------------------
    # 6. Média de entrega por categoria
    # --------------------------------------------------------

    reports["entrega_por_categoria"] = (
        df
        .filter(
            col("status") == "Entregue"
        )
        .groupBy("category")
        .agg(
            avg("delivery_time_days").alias(
                "media_dias_entrega"
            ),
            count("order_id").alias(
                "total_entregues"
            ),
        )
        .orderBy("media_dias_entrega")
    )

    return reports


# ============================================================
# SALVAMENTO DOS RELATÓRIOS
# ============================================================

def save_reports(reports, output_dir):
    """
    Salva os relatórios em formato Parquet.
    """

    print(
        f"Salvando relatórios em {output_dir}..."
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    for name, df in reports.items():

        output_path = (
            f"{output_dir}/"
            f"{name}_{timestamp}"
        )

        print(
            f"Salvando {name}..."
        )

        df.write.mode("overwrite").parquet(
            output_path
        )

        print(
            f"✓ {name} salvo em {output_path}"
        )

        # Debug
        count_rows = df.count()

        print(
            f"  {name}: "
            f"{count_rows} registros"
        )

        df.show(
            5,
            truncate=False
        )


# ============================================================
# MAIN
# ============================================================

def main():
    """Função principal do ETL."""

    spark = None

    try:

        print("=" * 60)
        print("INICIANDO ETL JOB")
        print(
            f"Data/Hora: "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print("=" * 60)

        # ----------------------------------------------------
        # 1. Criar Spark Session
        # ----------------------------------------------------

        spark = create_spark_session(
            "ETL_Job_Example"
        )

        print("✓ Spark Session criada")

        # ----------------------------------------------------
        # 2. Gerar dados
        # ----------------------------------------------------

        df_raw = generate_sample_data(
            spark,
            num_records=5000,
        )

        raw_count = df_raw.count()

        print(
            f"✓ {raw_count} registros gerados"
        )

        # ----------------------------------------------------
        # 3. Processar dados
        # ----------------------------------------------------

        df_processed = process_data(
            df_raw
        )

        print(
            "✓ Dados processados"
        )

        # ----------------------------------------------------
        # 4. Salvar dados processados
        # ----------------------------------------------------

        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        processed_path = (
            f"{OUTPUT_DIR}/"
            f"processed_data_{timestamp}"
        )

        print(
            f"Salvando dados processados em "
            f"{processed_path}..."
        )

        df_processed.write.mode(
            "overwrite"
        ).parquet(
            processed_path
        )

        print(
            f"✓ Dados processados salvos em "
            f"{processed_path}"
        )

        # ----------------------------------------------------
        # 5. Gerar relatórios
        # ----------------------------------------------------

        reports = generate_reports(
            df_processed
        )

        print(
            f"✓ {len(reports)} relatórios gerados"
        )

        # ----------------------------------------------------
        # 6. Salvar relatórios
        # ----------------------------------------------------

        save_reports(
            reports,
            OUTPUT_DIR
        )

        # ----------------------------------------------------
        # 7. Resumo
        # ----------------------------------------------------

        print("\n" + "=" * 60)
        print(
            "ETL JOB CONCLUÍDO COM SUCESSO!"
        )
        print(
            f"Total de registros processados: "
            f"{df_processed.count()}"
        )
        print(
            f"Relatórios gerados: "
            f"{len(reports)}"
        )
        print("=" * 60)

    except Exception as e:

        print(
            "\n❌ ERRO no ETL Job:"
        )

        print(
            str(e)
        )

        import traceback

        traceback.print_exc()

        sys.exit(1)

    finally:

        if spark is not None:
            spark.stop()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    main()
