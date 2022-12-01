from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def get_schema_postgresql(spark: SparkSession, url: str, query: str) -> DataFrame:
    try:
        table = f"""(SELECT
            column_name,
            case
                when data_type = 'character varying' or data_type = 'character'
                    then 'STRING' else upper(data_type)  end as data_type
            FROM information_schema.columns
            WHERE
            table_name = '{query}'
            ) as tb"""
        dataframe = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table)
            .option("user", "postgres")
            .option("password", "postgres")
            .load()
        )
        return dataframe
    except Exception as ex:
        raise ex


def write_data_postgresql(spark: SparkSession, url: str, dataframe: DataFrame, table: str) -> None:
    try:
        (
            dataframe.write.mode("append")
            .format("jdbc")
            .option("url", url)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table)
            .option("user", "postgres")
            .option("password", "postgres")
            .save()
        )
    except Exception as ex:
        raise ex
