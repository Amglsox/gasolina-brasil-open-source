from pyspark.sql import DataFrame


def rename_columns(dataframe: DataFrame) -> DataFrame:
    """**summary line, max. 79 chars including period** Do something interesting.

    **description starts after blank line above**
    Args:
        :param dataframe: Dataframe com os dados lidos
        :return: DataFrame Spark
    """
    try:
        for column in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(
                column,
                column.replace("-", "")
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
                .replace("/", "_")
                .replace("%", "")
                .replace("__", "_")
                .lower(),
            )
        return dataframe
    except Exception as ex:
        raise (ex)
