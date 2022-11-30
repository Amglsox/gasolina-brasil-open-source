from pyspark.sql import SparkSession


def start_or_create_spark() -> SparkSession:
    spark = SparkSession.builder.master("local").appName("Processamento de Dados de Gasolina no Brasil").getOrCreate()
    return spark
