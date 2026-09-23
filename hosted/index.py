"""Vercel entry point: the FastAPI backend as one serverless function.

Only the read-only API runs here. It serves a snapshot of the pipeline's
MongoDB (MongoDB Atlas, via MONGO_URI) with HOSTED_SNAPSHOT=1 set; Spark, Kafka
and HDFS are not part of the hosted deployment.
"""

import sys
from pathlib import Path

# The api/, pipeline/ and storage/ packages live at the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.main import app  # noqa: E402,F401  (Vercel serves the ASGI `app`)
