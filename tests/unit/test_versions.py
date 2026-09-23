"""C25: a deliberate version mismatch produces a named error, not a stack trace.

PRD §12.1 asks for "version assertion failure messaging" to be tested. The
message is the product here: it must name the component, both versions, and
the fix, because this is the failure that cost the team days before.
"""

from __future__ import annotations

import pytest

from pipeline import versions


def test_connector_coordinates_are_derived_from_the_pins():
    assert versions.MONGO_SPARK_PACKAGE == (
        f"org.mongodb.spark:mongo-spark-connector_{versions.SCALA_VERSION}"
        f":{versions.MONGO_SPARK_CONNECTOR_VERSION}"
    )
    assert versions.SPARK_VERSION in versions.KAFKA_SQL_PACKAGE
    assert versions.KAFKA_SQL_PACKAGE not in versions.spark_packages(kafka=False)


def test_pyspark_mismatch_is_a_named_error(monkeypatch):
    import pyspark

    monkeypatch.setattr(pyspark, "__version__", "3.4.1")
    with pytest.raises(versions.VersionMismatch) as caught:
        versions.assert_versions(require_java=False)

    error = caught.value
    assert error.component == "PySpark"
    assert error.expected == versions.SPARK_VERSION
    assert error.actual == "3.4.1"
    message = str(error)
    assert "expected" in message and "actual" in message and "fix" in message
    assert f"pyspark=={versions.SPARK_VERSION}" in message


def test_python_mismatch_is_a_named_error(monkeypatch):
    monkeypatch.setattr(versions, "PYTHON_REQUIRED", (3, 9))
    with pytest.raises(versions.VersionMismatch) as caught:
        versions.assert_versions(require_java=False)
    assert caught.value.component == "Python"
    assert "venv" in caught.value.fix


def test_unsupported_java_is_a_named_error(monkeypatch):
    monkeypatch.setattr(versions, "_java_major", lambda: 21)
    with pytest.raises(versions.VersionMismatch) as caught:
        versions.assert_versions()
    assert caught.value.component == "Java"
    assert caught.value.actual == "JDK 21"


def test_missing_java_is_a_named_error(monkeypatch):
    monkeypatch.setattr(versions, "_java_major", lambda: None)
    with pytest.raises(versions.VersionMismatch) as caught:
        versions.assert_versions()
    assert caught.value.component == "Java"
    assert "Temurin" in caught.value.fix


def test_the_pinned_stack_passes():
    versions.assert_versions(require_java=False)
