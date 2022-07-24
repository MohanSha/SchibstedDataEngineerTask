from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,
    types as T
)
import json
from pyspark.sql.functions import *
import os
import shutil


def build_schema() -> T.StructType:
    """
    Return input data schema.
    """
    query_params_schema = T.StructType([
        T.StructField("name", T.StringType(), nullable=False),
        T.StructField("value", T.StringType(), nullable=False),
    ])
    return T.StructType([
        T.StructField("row_id", T.IntegerType(), nullable=False),
        T.StructField("is_sample", T.BooleanType(), nullable=False),
        T.StructField("raw_url", T.StringType(), nullable=False),
        T.StructField("schema", T.StringType(), nullable=True),
        T.StructField("domain", T.StringType(), nullable=True),
        T.StructField("path", T.StringType(), nullable=True),
        T.StructField("fragment", T.StringType(), nullable=True),
        T.StructField("query_params_array", T.ArrayType(query_params_schema, False), nullable=True)
    ])


def read_input(spark: SparkSession, path: str, schema: T.StructType) -> DataFrame:
    return spark.read.json(path, schema=schema)


def write_output(df: DataFrame, path: str) -> None:
    df.select(
        "row_id",
        "is_sample",
        "raw_url",
        "schema",
        "domain",
        "path",
        "fragment",
        "query_params_array",
    ).repartition(
        1
    ).write.json(
        path
    )

    part_filename = next(entry for entry in os.listdir(path) if entry.startswith('part-'))
    temporary_json = os.path.join(path, part_filename)

    shutil.copyfile(temporary_json, path+".json")
    shutil.rmtree(path)



def get_spark() -> SparkSession:
    """
    Return an initialized Spark sesssion.
    """

    
    spark = SparkSession.builder.getOrCreate()

    return spark


def parse_urls(df: DataFrame) -> DataFrame:
    """
    Return the dataframe with extra fields parsed from the raw URLs.
    """
    first_split = split(col("raw_url_2"),':')
    second_split = split(col("raw_url_2"),'#')
    third_split =  split(col("raw_url_2"),'\\?')
    fourth_split = split(col('query_string'),'\\&')

    df_2 = df\
    .withColumn('raw_url_2', F.expr("reflect('java.net.URLDecoder','decode', raw_url, 'utf-8')"))\
    .withColumn('schema',first_split.getItem(0))\
    .withColumn('fragment',second_split.getItem(1))\
    .withColumn('query_string',third_split.getItem(1))\
    .selectExpr('row_id','is_sample','raw_url','raw_url_2','schema','parse_url(raw_url,"HOST") as domain','parse_url(raw_url, "PATH") as path','fragment','query_string')\
    .withColumn('querty',fourth_split)\
    .select('*',explode('querty').alias('t'))\
    .withColumn('name',split(col('t'),"=").getItem(0))\
    .withColumn('value',split(col('t'),"=").getItem(1))\
    .withColumn('combine',struct("name", "value"))\
    .groupBy('row_id','is_sample','raw_url','is_sample','schema','domain','path','fragment').agg((collect_list(col('combine'))).alias('query_params_array'))

    return df_2


def main() -> None:
    """
    Read the data, transform it, and write the output.
    """
    JSON_INPUT_PATH = "../../data/urls.json"
    JSON_OUTPUT_PATH = "../../data/urls-output"

    spark = get_spark()

    raw_df = read_input(spark, JSON_INPUT_PATH, schema=build_schema())

    processed_df = raw_df.transform(parse_urls)

    write_output(processed_df, JSON_OUTPUT_PATH)


main()