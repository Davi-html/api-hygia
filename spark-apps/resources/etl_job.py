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
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# Configurações
WAREHOUSE_DIR = "/opt/spark-warehouse"
OUTPUT_DIR = "/opt/spark-apps/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def create_spark_session(app_name="ETL_Job_Example"):
    """Cria e configura a Spark Session"""
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def generate_sample_data(spark, num_records=1000):
    """
    Gera dados de exemplo de vendas
    """
    print(f"Gerando {num_records} registros de exemplo...")
    
    # Schema dos dados
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
    
    # Gerar dados mockados
    categories = ["Eletrônicos", "Roupas", "Alimentos", "Livros", "Esportes"]
    statuses = ["Entregue", "Processando", "Enviado", "Cancelado"]
    customer_names = [
        "João Silva", "Maria Santos", "Pedro Costa", "Ana Lima", "Carlos Souza",
        "Mariana Oliveira", "Rafael Pereira", "Fernanda Alves", "Lucas Santos", 
        "Juliana Costa", "André Lima", "Patrícia Souza", "Roberto Oliveira", 
        "Carla Pereira", "Ricardo Alves"
    ]
    product_names = {
        "Eletrônicos": ["Smartphone", "Notebook", "Tablet", "Fone Bluetooth", "Carregador"],
        "Roupas": ["Camiseta", "Calça Jeans", "Vestido", "Jaqueta", "Sapato"],
        "Alimentos": ["Arroz", "Feijão", "Macarrão", "Café", "Açúcar"],
        "Livros": ["Romance", "Ficção", "Ciência", "História", "Autoajuda"],
        "Esportes": ["Bola Futebol", "Raquete Tênis", "Luvas Boxe", "Bicicleta", "Óculos Natação"]
    }
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        category = categories[i % len(categories)]
        product_list = product_names[category]
        
        order_date = start_date + timedelta(
            days=i % 365,
            hours=i % 24,
            minutes=i % 60
        )
        
        data.append((
            i + 1,  # order_id
            (i % 50) + 1,  # customer_id
            customer_names[i % len(customer_names)],  # customer_name
            (i % 20) + 1,  # product_id
            product_list[i % len(product_list)],  # product_name
            category,  # category
            (i % 5) + 1,  # quantity
            round(10 + (i % 1000) * 0.5, 2),  # price
            order_date,  # order_date
            (order_date + timedelta(days=(i % 7) + 1)).date(),  # delivery_date
            statuses[i % len(statuses)]  # status
        ))
    
    # Criar DataFrame
    df = spark.createDataFrame(data, schema)
    return df

def process_data(df):
    """
    Processa e transforma os dados
    """
    print("Processando dados...")
    
    # Adicionar colunas derivadas
    df_processed = (df
        .withColumn("order_year", date_format(col("order_date"), "yyyy"))
        .withColumn("order_month", date_format(col("order_date"), "MM"))
        .withColumn("order_day", date_format(col("order_date"), "dd"))
        .withColumn("total_amount", col("quantity") * col("price"))
        .withColumn("delivery_time_days", 
                   (col("delivery_date").cast("long") - col("order_date").cast("long")) / 86400)
    )
    
    return df_processed

def generate_reports(df):
    """
    Gera relatórios agregados
    """
    print("Gerando relatórios...")
    
    reports = {}
    
    # 1. Vendas por categoria
    reports['vendas_por_categoria'] = (
        df.groupBy("category")
        .agg(
            spark_sum("total_amount").alias("total_vendas"),
            count("order_id").alias("total_pedidos"),
            avg("total_amount").alias("media_pedido")
        )
        .orderBy(col("total_vendas").desc())
    )
    
    # 2. Vendas por mês
    reports['vendas_por_mes'] = (
        df.groupBy("order_year", "order_month")
        .agg(
            spark_sum("total_amount").alias("total_vendas"),
            count("order_id").alias("total_pedidos"),
            avg("total_amount").alias("media_pedido")
        )
        .orderBy("order_year", "order_month")
    )
    
    # 3. Top 10 produtos mais vendidos
    reports['top_produtos'] = (
        df.groupBy("product_name", "category")
        .agg(
            spark_sum("quantity").alias("total_quantidade"),
            spark_sum("total_amount").alias("total_vendas"),
            count("order_id").alias("total_pedidos")
        )
        .orderBy(col("total_vendas").desc())
        .limit(10)
    )
    
    # 4. Análise de clientes (top compradores)
    reports['top_clientes'] = (
        df.groupBy("customer_id", "customer_name")
        .agg(
            spark_sum("total_amount").alias("total_gasto"),
            count("order_id").alias("total_pedidos"),
            avg("total_amount").alias("media_pedido"),
            spark_max("order_date").alias("ultima_compra")
        )
        .orderBy(col("total_gasto").desc())
        .limit(10)
    )
    
    # 5. Status dos pedidos
    reports['status_pedidos'] = (
        df.groupBy("status")
        .agg(
            count("order_id").alias("quantidade"),
            spark_sum("total_amount").alias("valor_total")
        )
        .orderBy("status")
    )
    
    # 6. Média de tempo de entrega por categoria
    reports['entrega_por_categoria'] = (
        df.filter(col("status") == "Entregue")
        .groupBy("category")
        .agg(
            avg("delivery_time_days").alias("media_dias_entrega"),
            count("order_id").alias("total_entregues")
        )
        .orderBy("media_dias_entrega")
    )
    
    return reports

def save_reports(reports, output_dir):
    """
    Salva os relatórios em formato Parquet
    """
    print(f"Salvando relatórios em {output_dir}...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for name, df in reports.items():
        output_path = f"{output_dir}/{name}_{timestamp}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"  ✓ {name} salvo em {output_path}")
        
        # Mostrar algumas linhas para debug
        print(f"    {name}: {df.count()} registros")
        df.show(5, truncate=False)

def main():
    """Função principal do ETL"""
    try:
        print("=" * 60)
        print("INICIANDO ETL JOB")
        print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # 1. Criar Spark Session
        spark = create_spark_session("ETL_Job_Example")
        print("✓ Spark Session criada")
        
        # 2. Gerar dados de exemplo
        df_raw = generate_sample_data(spark, num_records=5000)
        print(f"✓ {df_raw.count()} registros gerados")
        
        # 3. Processar dados
        df_processed = process_data(df_raw)
        print("✓ Dados processados")
        
        # 4. Salvar dados processados (opcional)
        processed_path = f"{OUTPUT_DIR}/processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df_processed.write.mode("overwrite").parquet(processed_path)
        print(f"✓ Dados processados salvos em {processed_path}")
        
        # 5. Gerar relatórios
        reports = generate_reports(df_processed)
        print(f"✓ {len(reports)} relatórios gerados")
        
        # 6. Salvar relatórios
        save_reports(reports, OUTPUT_DIR)
        
        # 7. Mostrar resumo final
        print("\n" + "=" * 60)
        print("ETL JOB CONCLUÍDO COM SUCESSO!")
        print(f"Total de registros processados: {df_processed.count()}")
        print(f"Relatórios gerados: {len(reports)}")
        print("=" * 60)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ ERRO no ETL Job: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()