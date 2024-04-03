import pyspark
from lakehouse_insights.utils.insights import delta_overview
from delta import configure_spark_with_delta_pip
import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@pytest.fixture(scope="session")
def spark():
    builder = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("local-tests")
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


def test_delta_overview(spark):
    tmp_root_path = "/tmp"
    path = f"{tmp_root_path}/lakehouse-insights/overview-test"
    sample_data = [
        (1, "Sants", "Barcelona", "08014"),
        (2, "Les Corts", "Barcelona", "08028"),
        (3, "Sant Marti", "Barcelona", "08001"),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("attribute_1", StringType(), True),
            StructField("attribute_2", StringType(), True),
            StructField("attribute_3", StringType(), True),
        ]
    )

    df = spark.createDataFrame(sample_data, schema=schema)
    df.write.mode("overwrite").format("delta").save(path)
    mandatory_fields = ["id", "attribute_1", "attribute_2", "attribute_3"]

    df_actual = delta_overview(spark, path, mandatory_fields, mandatory_fields)
    actual_columns = list(df_actual.columns)
    expected_columns = ['format', 'location', 'created_at', 'updated_at', 'number_of_files', 'size_in_MB',
                        'partition_cols', 'evaluated_at', 'total_records']

    assert actual_columns == expected_columns
    assert df_actual['format'].iloc[0] == "delta"
    assert df_actual['location'].iloc[0] == f"file:{path}"
