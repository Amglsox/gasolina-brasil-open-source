from pyspark.sql import SparkSession


def start_or_create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local")
        .appName("Processamento de Dados de Gasolina no Brasil")
        .config("spark.jars", "jars/postgresql-42.5.1.jar")
        .getOrCreate()
    )

    return spark
