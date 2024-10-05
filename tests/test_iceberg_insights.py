import pyspark
import pytest

from lakehouse_insights.utils.insights import iceberg_spark_jar

@pytest.fixture(scope="session")
def spark():
    builder = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .enableHiveSupport()
        .appName("local-iceberg-tests")
        .config("spark.executor.cores", "1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.jars.packages", iceberg_spark_jar)
        .config(
            f"spark.sql.catalog.iceberg_catalog",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.iceberg_catalog.type",
            "hadoop"
        )
        .config(
            f"spark.sql.catalog.iceberg_catalog.warehouse",
            "spark-warehouse"
        )
        .config("spark.sql.catalog.iceberg_catalog.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", "iceberg_catalog")
    )
    return builder.getOrCreate()

def test_iceberg_overview(spark):
    tmp_root_path = "/tmp"
    path = f"{tmp_root_path}/lakehouse"