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
            if item["type"] == "FLOAT":
                dataframe = dataframe.withColumn(item["name"], translate(col(item["name"]), ",", ".").cast("double"))
            elif item["type"] == "INTEGER":
                dataframe = dataframe.withColumn(item["name"], col(item["name"]).cast("long"))
            elif item["type"] == "STRING":
                dataframe = dataframe.withColumn(item["name"], trim(col(item["name"])).cast("string"))
            elif item["type"] == "BOOLEAN":
                dataframe = dataframe.withColumn(item["name"], col(item["name"]).cast("boolean"))
            elif item["type"] == "TIMESTAMP" or item["type"] == "DATETIME":
                dataframe = dataframe.withColumn(item["name"], col(item["name"]).cast("timestamp"))
            elif item["type"] == "DATE":
                dataframe = dataframe.withColumn(item["name"], to_date(col(item["name"]), "dd/MM/yyyy").cast("date"))
            elif item["type"] == "NUMERIC":
                dataframe = dataframe.withColumn(
                    item["name"], translate(col(item["name"]), ",", ".").cast("decimal(38,2)")
                )
        return dataframe
    except Exception as ex:
        raise (ex)
