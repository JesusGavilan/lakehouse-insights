from datetime import datetime
from typing import List
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
)
from pyspark.sql.session import SparkSession
import pandas as pd

date_format = "MM-dd-yyyy"
timestamp_format = "MM-dd-yyyy HH:mm:ss"

lakehouse_formats = ['Delta', 'Iceberg', 'Hudi']


def spark():
    builder = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("local")
        .config("spark.executor.cores", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def filter_from_catalog(file_catalog: [dict]) -> dict:
    results = dict()
    if file_catalog is not None:
        results = [i for i in file_catalog if i['format'] in lakehouse_formats]
    return {'data_catalog': results}


def get_insights_dataframe(file_catalog_dict: [dict]) -> List[pd.DataFrame]:
    spark_session = spark()

    file_catalog_dict = filter_from_catalog(file_catalog_dict)

    print(file_catalog_dict)
    tables_overview = []
    for item in file_catalog_dict['data_catalog']:
        tables_overview.append(delta_overview(spark_session,
                                              path=item['path'],
                                              mandatory_cols=item['mandatory_attributes'],
                                              primary_keys=item['primary_keys']))
    return tables_overview


def delta_overview(spark: SparkSession, path: str, mandatory_cols: List[str], primary_keys: List[str]) -> pd.DataFrame:
    details_df = spark.sql("DESCRIBE DETAIL delta.`{}`".format(path))
    current_ts = datetime.now()
    total_count = spark.read.format("delta").load(str(path)).count()

    details_df = (
        details_df.select(
            col("format"),
            col("location"),
            to_date(col("createdAt"), date_format).alias("created_at"),
            to_date(col("lastModified"), date_format).alias("updated_at"),
            col("numFiles").alias("number_of_files"),
            (col("sizeInBytes") / (1000 ** 2)).alias("size_in_MB"),
            col("partitionColumns").alias("partition_cols"),
        )
        .withColumn("evaluated_at", to_date(lit(current_ts), timestamp_format))
        .withColumn("total_records", lit(total_count)))
    return details_df.toPandas()
