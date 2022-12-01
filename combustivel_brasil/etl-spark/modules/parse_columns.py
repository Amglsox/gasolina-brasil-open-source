from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import translate
from pyspark.sql.functions import trim


def parse_columns_csv(schema: list, dataframe: DataFrame) -> DataFrame:
    """**summary line, max. 79 chars including period** Do something interesting.

    **description starts after blank line above**
    Args:
        :param schema: schema de dados do bq para parse
        :param dataframe: conjunto de dados a serem parseados
        :return: DataFrame Spark
    """
    try:
        for item in schema:
            if item["data_type"] == "FLOAT":
                dataframe = dataframe.withColumn(
                    item["column_name"], translate(col(item["column_name"]), ",", ".").cast("double")
                )
            elif item["data_type"] == "INTEGER":
                dataframe = dataframe.withColumn(item["column_name"], col(item["column_name"]).cast("long"))
            elif item["data_type"] == "STRING":
                dataframe = dataframe.withColumn(item["column_name"], trim(col(item["column_name"])).cast("string"))
            elif item["data_type"] == "BOOLEAN":
                dataframe = dataframe.withColumn(item["column_name"], col(item["column_name"]).cast("boolean"))
            elif item["data_type"] == "TIMESTAMP" or item["data_type"] == "DATETIME":
                dataframe = dataframe.withColumn(item["column_name"], col(item["column_name"]).cast("timestamp"))
            elif item["data_type"] == "DATE":
                dataframe = dataframe.withColumn(
                    item["column_name"], to_date(col(item["column_name"]), "dd/MM/yyyy").cast("date")
                )
            elif item["data_type"] == "NUMERIC":
                dataframe = dataframe.withColumn(
                    item["column_name"], translate(col(item["column_name"]), ",", ".").cast("decimal(38,2)")
                )
        return dataframe
    except Exception as ex:
        raise (ex)
