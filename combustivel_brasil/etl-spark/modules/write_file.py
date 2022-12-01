from pyspark.sql import DataFrame


def write(path_output: str, dataframe: DataFrame, formato: str, partitions: list) -> None:
    """**summary line, max. 79 chars including period** Do something interesting.

    Args:
        :param path_output: path para save dos dados
        :param dataframe: conjunto de dados a serem salvos
        :param formato: tipo de arquivo a ser salvo
        :return: None
    """
    if partitions is not None:
        dataframe.write.mode("overwrite").partitionBy(partitions).format(formato).save(path_output)
    else:
        dataframe.write.mode("overwrite").format(formato).save(path_output)
