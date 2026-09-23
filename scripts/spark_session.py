"""One place that builds SparkSessions, so every job gets the same pinned stack.

PRD §0 rule 6: sized for 16 GB / local[2]. Nothing here assumes a cluster.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from pipeline import versions
from pipeline.config import DB_NAME, settings


def configure_python_workers() -> None:
    """Point Spark's Python workers at this interpreter.

    Spark launches workers by running bare ``python``. On Windows that
    resolves to the Microsoft Store alias stub, which prints "Python was not
    found" and exits -- and Spark reports it as an unrelated
    "Python worker failed to connect back" socket timeout, several hundred
    lines deep. Pinning both variables to the running interpreter makes the
    venv's Python the one that is actually used.

    Called from `build()` and from the test fixtures, so no entrypoint has to
    remember it.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build(app_name: str, *, kafka: bool = False, mongo: bool = True) -> SparkSession:
    """Create a SparkSession after asserting the pinned versions (C25).

    ``assert_versions()`` runs first so a stack mismatch is reported as a named
    error rather than surfacing later as an opaque Java linkage failure.
    """
    versions.assert_versions()
    configure_python_workers()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        # Small local runs: the default 200 shuffle partitions is pure overhead.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if mongo or kafka:
        builder = builder.config(
            "spark.jars.packages", versions.spark_packages(kafka=kafka)
        )
    if mongo:
        builder = builder.config(
            "spark.mongodb.read.connection.uri", settings.mongo_uri
        ).config("spark.mongodb.write.connection.uri", settings.mongo_uri).config(
            "spark.mongodb.read.database", DB_NAME
        ).config("spark.mongodb.write.database", DB_NAME)

    if settings.storage_backend == "hdfs":
        builder = (
            builder.config("spark.hadoop.fs.defaultFS", settings.hdfs_uri)
            .config(
                # Spark runs on the host; the datanode is in a container, so
                # block access must go via the advertised hostname, not the
                # container IP.
                "spark.hadoop.dfs.client.use.datanode.hostname",
                "true",
            )
            .config(
                # Replication is chosen by the WRITING client, not the
                # namenode. Without this Spark writes at Hadoop's default of 3,
                # and on one datanode every block is permanently
                # under-replicated -- `hdfs fsck` found 73 such blocks.
                "spark.hadoop.dfs.replication",
                "1",
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
