"""Shared fixtures.

The Spark fixture is session-scoped because starting a JVM costs ~15s and the
suite would otherwise spend most of its time on process startup. It is
deliberately smaller than the production session: one core, no connectors, no
Mongo -- a unit test that needs a broker is not a unit test.
"""

from __future__ import annotations

import os

# Must run before any project module is imported: every test talks to an
# isolated database, never the one the dashboard reads.
os.environ["MONGO_DB"] = "sentiment_stream_test"

import pytest


@pytest.fixture(scope="session")
def spark():
    """A minimal local SparkSession, or skip the test if Spark cannot start."""
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    from pyspark.sql import SparkSession

    from scripts.spark_session import configure_python_workers

    configure_python_workers()

    if not os.environ.get("JAVA_HOME"):
        import shutil

        if not shutil.which("java"):
            pytest.skip("no JDK on PATH or JAVA_HOME; Spark cannot start")

    session = (
        SparkSession.builder.appName("tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def mongo():
    """A Repository against a real MongoDB, or skip if none is running.

    PRD 12.2 wants the API tested against a real instance -- mocking the
    database would test the mock, not the query path the dashboard uses.
    """
    from api.repository import Repository, RepositoryUnavailable

    repository = Repository(timeout_ms=1500)
    try:
        repository.ping()
    except RepositoryUnavailable:
        repository.close()
        pytest.skip("no MongoDB reachable; start it with `docker compose up -d mongo`")
    yield repository
    repository.close()
