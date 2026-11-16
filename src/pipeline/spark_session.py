"""Builds a SparkSession wired to Iceberg + S3 + (Polaris or Glue)."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from pipeline.config import Config

# Versions are pinned so the job behaves the same on every machine.
SPARK_SCALA = "3.5_2.12"       # must match the installed pyspark (3.5.x)
ICEBERG_VERSION = "1.9.2"
HADOOP_AWS_VERSION = "3.3.4"   # must match the Hadoop version bundled with Spark 3.5


def build_spark(app_name: str, cfg: Config) -> SparkSession:
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_SCALA}:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",   # S3FileIO + Glue client
        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",          # s3a:// for reading raw files
    ]
    cat = f"spark.sql.catalog.{cfg.spark_catalog}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark_master)
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        # Raw reads through s3a use the normal AWS credential chain (AWS_PROFILE etc.).
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint.region", cfg.region)
        # The Iceberg catalog called "lake"
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.client.region", cfg.region)
    )

    if cfg.catalog_type == "polaris":
        credential = os.environ.get("POLARIS_CREDENTIAL")  # "client_id:client_secret", from .env only
        if not credential:
            raise SystemExit("POLARIS_CREDENTIAL is not set (see .env.example)")
        builder = (
            builder.config(f"{cat}.type", "rest")
            .config(f"{cat}.uri", cfg.polaris_uri)
            .config(f"{cat}.oauth2-server-uri", f"{cfg.polaris_uri}/v1/oauth/tokens")
            .config(f"{cat}.warehouse", cfg.polaris_catalog)       # the Polaris catalog name
            .config(f"{cat}.credential", credential)
            .config(f"{cat}.scope", "PRINCIPAL_ROLE:ALL")
            .config(f"{cat}.token-refresh-enabled", "true")
        )
    elif cfg.catalog_type == "glue":
        builder = (
            builder.config(f"{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"{cat}.warehouse", cfg.warehouse_uri)
        )
    else:
        raise SystemExit(f"CATALOG_TYPE must be 'polaris' or 'glue', got {cfg.catalog_type!r}")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def table_exists(spark: SparkSession, full_name: str) -> bool:
    catalog, namespace, table = full_name.split(".")
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
    return any(r.tableName == table for r in rows)


def ensure_namespace(spark: SparkSession, cfg: Config) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.spark_catalog}.{cfg.namespace}")
