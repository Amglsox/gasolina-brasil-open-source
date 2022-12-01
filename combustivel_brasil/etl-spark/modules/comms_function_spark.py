from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import quarter
from pyspark.sql.functions import to_date
from pyspark.sql.functions import when
from pyspark.sql.functions import year


def add_semestre(dataframe: DataFrame, column: str) -> DataFrame:
    """**summary line, max. 79 chars including period** Do something interesting.

    **description starts after blank line above**
    Args:
        :param dataframe:
        :return: Dataframe
    """
    dataframe = dataframe.withColumn(
        "semestre", when(quarter(to_date(col(column), "dd/MM/yyyy")) <= 2, 1).otherwise(2)
    )
    return dataframe


def add_year(dataframe: DataFrame, column: str) -> DataFrame:
    """**summary line, max. 79 chars including period** Do something interesting.

    **description starts after blank line above**
    Args:
        :param dataframe:
        :return: Dataframe
    """
    dataframe = dataframe.withColumn("ano", year(to_date(col(column), "dd/MM/yyyy")))
    return dataframe


def add_input_filename(dataframe: DataFrame) -> DataFrame:
    """**summary line, max. 79 chars including period** Do something interesting.

    **description starts after blank line above**
    Args:
        :param dataframe:
        :return: Dataframe
    """
    dataframe = dataframe.withColumn("input_file_name", input_file_name())
    return dataframe
