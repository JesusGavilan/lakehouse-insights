from datetime import datetime, date
from typing import List

from pyiceberg.catalog import load_catalog
from pyspark import sql
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
)

from deltalake import DeltaTable
from delta import configure_spark_with_delta_pip

import pandas as pd

import polars as pl

date_format = "MM-dd-yyyy"
timestamp_format = "MM-dd-yyyy HH:mm:ss"

lakehouse_formats = ['Delta', 'Iceberg', 'Hudi']
iceberg_spark_jar = 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0'


def spark_delta():
    builder = (
        sql.SparkSession.builder.master("local[1]")
        .appName("local-spark-delta")
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


def spark_iceberg(iceberg_catalog_name: str, warehouse_path: str):
    builder = (
        sql.SparkSession.builder.master("local[1]")
        .enableHiveSupport()
        .appName("local-spark-iceberg")
        .config("spark.executor.cores", "1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.jars.packages", iceberg_spark_jar)
        .config(
            f"spark.sql.catalog.{iceberg_catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{iceberg_catalog_name}.type",
            "hadoop"
        )
        .config(
            f"spark.sql.catalog.{iceberg_catalog_name}.warehouse",
            f"{warehouse_path}"
        )
        .config("spark.sql.catalog.iceberg_catalog.cache-enabled","false")
        .config("spark.sql.defaultCatalog", f"{iceberg_catalog_name}")
    )
    return builder.getOrCreate()


def filter_from_catalog(file_catalog: [dict]) -> dict:
    results = dict()
    if file_catalog is not None:
        results = [i for i in file_catalog if i['format'] in lakehouse_formats]
    return {'data_catalog': results}


def get_insights_dataframe(file_catalog_dict: [dict]) -> List[pd.DataFrame]:
    spark_session = spark_delta()

    file_catalog_dict = filter_from_catalog(file_catalog_dict)

    print(file_catalog_dict)
    tables_overview = []
    for item in file_catalog_dict['data_catalog']:
        tables_overview.append(delta_overview(spark_session,
                                              path=item['path'],
                                              name=item['name'],
                                              mandatory_cols=item['mandatory_attributes'],
                                              primary_keys=item['primary_keys']))
    return tables_overview


def delta_overview(spark: SparkSession, path: str, name:str) -> pd.DataFrame:
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
        .withColumn("name", lit(name))
        .withColumn("evaluated_at", to_date(lit(current_ts), timestamp_format))
        .withColumn("total_records", lit(total_count)))
    return details_df.toPandas()

def iceberg_overview(spark: SparkSession, path: str, name: str, mandatory_cols: List[str], primary_keys: List[str]) -> pd.DataFrame:
    history = spark.read.format("iceberg").load(f"{name}.history")
    files = spark.read.format("iceberg").load(f"{name}.files")
    created_at = history.agg({"made_current_at": "min"}).collect()[0][0]
    last_modified = history.agg({"made_current_at": "max"}).collect()[0][0]
    number_of_files = files.count()


def delta_overview_polars(path: str, name:str) -> pd.DataFrame:
    dt = DeltaTable(path)
    df = pl.read_delta(path)
    total_records = df.__len__()
    size_in_MB = df.estimated_size()  / (1000 ** 2)
    #df.sql("SELECT COUNT(*) FROM self")

    data = {'format': ["delta"],
            'location': [path],
            'created_at': [datetime.fromtimestamp(dt.metadata().created_time/1000)],
            'updated_at': [None],
            'number_of_files': [len(dt.files())],
            'size_in_MB': [size_in_MB],
            'partition_cols': [dt.metadata().partition_columns],
            'name': [name],
            'evaluated_at': [date.today().strftime(timestamp_format)],
            'total_records': [total_records],
            }
    df = pd.DataFrame(data)
    return df

def iceberg_overview_native(catalog: str, table_name: str) -> pd.DataFrame:
    catalog = load_catalog(catalog)
    table = catalog.load_table(table_name)

