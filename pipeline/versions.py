"""Single source of truth for every version this project pins (PRD C25, §4.4).

The team lost time to Spark/Scala/connector drift. Every Spark entrypoint calls
``assert_versions()`` before doing anything else, so a mismatch surfaces as a
named error naming expected, actual, and the fix -- never a stack trace from
deep inside py4j.

Maven coordinates are *derived* from these constants. They are never written
out a second time anywhere in this codebase.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys

# --- pinned versions ---------------------------------------------------------

SPARK_VERSION = "3.5.3"
SCALA_VERSION = "2.12"
HADOOP_VERSION = "3.3.6"
MONGO_SPARK_CONNECTOR_VERSION = "10.4.0"
KAFKA_VERSION = "3.7"

PYTHON_REQUIRED = (3, 11)
JAVA_SUPPORTED = (11, 17)  # Spark 3.5 runs on either; we install 17.

# --- derived Maven coordinates -----------------------------------------------

MONGO_SPARK_PACKAGE = (
    f"org.mongodb.spark:mongo-spark-connector_{SCALA_VERSION}"
    f":{MONGO_SPARK_CONNECTOR_VERSION}"
)
KAFKA_SQL_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)


def spark_packages(*, kafka: bool = True) -> str:
    """Comma-separated ``spark.jars.packages`` value.

    The Kafka connector is only pulled when a job actually needs it, so the
    socket path does not pay for a download it will never use.
    """
    packages = [MONGO_SPARK_PACKAGE]
    if kafka:
        packages.append(KAFKA_SQL_PACKAGE)
    return ",".join(packages)


class VersionMismatch(RuntimeError):
    """Raised when the runtime does not match the pinned stack."""

    def __init__(self, component: str, expected: str, actual: str, fix: str) -> None:
        super().__init__(
            f"\n"
            f"  Version mismatch: {component}\n"
            f"    expected : {expected}\n"
            f"    actual   : {actual}\n"
            f"    fix      : {fix}\n"
        )
        self.component = component
        self.expected = expected
        self.actual = actual
        self.fix = fix


def _java_major() -> int | None:
    """Major version of the java on PATH / JAVA_HOME, or None if absent."""
    java = shutil.which("java")
    java_home = os.environ.get("JAVA_HOME")
    if not java and java_home:
        candidate = os.path.join(java_home, "bin", "java")
        java = candidate if os.path.exists(candidate + ".exe") or os.path.exists(candidate) else None
    if not java:
        return None
    try:
        # `java -version` writes to stderr on every JDK in existence.
        out = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=30
        ).stderr
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', out)
    if not match:
        return None
    major = int(match.group(1))
    # Pre-9 JDKs report 1.8.x; the major is the second component.
    if major == 1 and match.group(2):
        return int(match.group(2))
    return major


def assert_versions(*, require_java: bool = True) -> None:
    """Fail fast and legibly if the runtime stack is not the pinned one."""
    actual_python = f"{sys.version_info.major}.{sys.version_info.minor}"
    expected_python = f"{PYTHON_REQUIRED[0]}.{PYTHON_REQUIRED[1]}"
    if sys.version_info[:2] != PYTHON_REQUIRED:
        raise VersionMismatch(
            "Python",
            expected_python,
            actual_python,
            f"PySpark {SPARK_VERSION} does not support Python {actual_python}. "
            f"Recreate the venv on {expected_python}: "
            f"py -{expected_python} -m venv .venv",
        )

    try:
        import pyspark
    except ImportError as exc:  # pragma: no cover - environment failure
        raise VersionMismatch(
            "PySpark",
            SPARK_VERSION,
            "not installed",
            "pip install -r requirements.txt",
        ) from exc

    if pyspark.__version__ != SPARK_VERSION:
        raise VersionMismatch(
            "PySpark",
            SPARK_VERSION,
            pyspark.__version__,
            f"The Mongo connector {MONGO_SPARK_CONNECTOR_VERSION} and Kafka "
            f"connector are pinned to Spark {SPARK_VERSION}. "
            f"pip install pyspark=={SPARK_VERSION}",
        )

    if require_java:
        major = _java_major()
        if major is None:
            raise VersionMismatch(
                "Java",
                f"JDK {' or '.join(str(v) for v in JAVA_SUPPORTED)}",
                "no java found on PATH or JAVA_HOME",
                "winget install EclipseAdoptium.Temurin.17.JDK, then set JAVA_HOME",
            )
        if major not in JAVA_SUPPORTED:
            raise VersionMismatch(
                "Java",
                f"JDK {' or '.join(str(v) for v in JAVA_SUPPORTED)}",
                f"JDK {major}",
                f"Spark {SPARK_VERSION} does not support JDK {major}. "
                f"Install Temurin 17 and point JAVA_HOME at it.",
            )


def version_report() -> dict[str, str]:
    """Everything the Pipeline Health page shows about the runtime stack."""
    try:
        import pyspark

        pyspark_version = pyspark.__version__
    except ImportError:
        pyspark_version = "not installed"
    java = _java_major()
    return {
        "python": f"{sys.version_info.major}.{sys.version_info.minor}."
        f"{sys.version_info.micro}",
        "python_required": f"{PYTHON_REQUIRED[0]}.{PYTHON_REQUIRED[1]}",
        "pyspark": pyspark_version,
        "spark_required": SPARK_VERSION,
        "scala": SCALA_VERSION,
        "hadoop": HADOOP_VERSION,
        "java": str(java) if java else "not found",
        "mongo_spark_connector": MONGO_SPARK_CONNECTOR_VERSION,
        "kafka": KAFKA_VERSION,
    }


if __name__ == "__main__":
    for key, value in version_report().items():
        print(f"{key:24} {value}")
