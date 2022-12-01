import argparse

from modules.comms_function_spark import add_input_filename
from modules.comms_function_spark import add_semestre
from modules.comms_function_spark import add_year
from modules.logging import Log4j
from modules.parse_columns import parse_columns_csv
from modules.postgresql_jdbc import get_schema_postgresql
from modules.postgresql_jdbc import write_data_postgresql
from modules.rename_columns import rename_columns
from modules.spark_create_session import start_or_create_spark


def main(path_input: str, path_output: str, formato: str, table: str, url: str) -> None:
    """**summary line, max. 79 chars including period** Do something interesting.

    **description starts after blank line above**
    Args:

        :param path_input: URI of the GCS bucket, for example, gs://bucket_name/file_name
        :param path_output: URI of the GCS bucket, for example, gs://bucket_name/file_name
        :param formato: Type format save file
        :param table_bq: Tabela do BigQuery Destino
        :return: None
    """

    try:
        spark = start_or_create_spark()
        log = Log4j(spark)
        log.info("Spark create Session")
        log.info("Get Schema BQ")
        schema_data = get_schema_postgresql(spark, url, table).collect()
        log.info(f"Read File: {path_input}")
        df = spark.read.format("csv").option("header", True).option("delimiter", ";").load(path_input)
        log.info("Data Quality Process")
        if True:
            log.info("Rename Columns")
            df_format = rename_columns(df)
            log.info("Add Semestre")
            df_format = add_semestre(df_format, "data_da_coleta")
            log.info("Add Ano")
            df_format = add_year(df_format, "data_da_coleta")
            log.info("Add Filename")
            df_format = add_input_filename(df_format)
            log.info("Parse de Colunas")
            df_format = parse_columns_csv(schema_data, df_format)
            log.info("Drop Duplicates do Dataframe")
            df_format = df_format.dropDuplicates()
            # particionamento = ["year", "semestre", "regiao_sigla", "estado_sigla"]
            log.info(f"put_gcs: {path_output}")
            # write(path_output=path_output, dataframe=df_format, formato=formato, partitions=particionamento)
            log.info("Write Postgresql")
            write_data_postgresql(spark, url, df_format, "tb_combustivel_brasil")
        else:
            raise Exception("Problema na qualidade de Dados")
            return 1
    except Exception:
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path_input",
        type=str,
        dest="path_input",
        required=True,
        help="URI of the GCS bucket, for example, gs://bucket_name/file_name",
    )

    parser.add_argument(
        "--path_output",
        type=str,
        dest="path_output",
        required=True,
        help="URI of the GCS bucket, for example, gs://bucket_name/file_name",
    )

    parser.add_argument(
        "--url",
        type=str,
        dest="url",
        required=True,
        help="URI of the PostgreSQL Database",
    )

    parser.add_argument("--formato", type=str, dest="formato", required=True, help="Type format save file")

    parser.add_argument("--table", type=str, dest="table", required=True, help="Tabela do PostgreSQL Destino")
    known_args, pipeline_args = parser.parse_known_args()

main(
    path_input=known_args.path_input,
    path_output=known_args.path_output,
    formato=known_args.formato,
    table=known_args.table,
    url=known_args.url,
)
