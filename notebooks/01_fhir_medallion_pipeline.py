# Fabric notebook source. Attach the target Lakehouse as the default Lakehouse.
# Mark the parameters below as the Notebook parameter cell in Fabric.
lookback_days = 3
page_size = 100
base_url = "https://hapi.fhir.org/baseR4"

# COMMAND ----------
import sys
from pathlib import Path

PACKAGE_PATH = "/lakehouse/default/Files/code"
if PACKAGE_PATH not in sys.path:
    sys.path.insert(0, PACKAGE_PATH)

from fhir_fabric.config import FhirConfig
from fhir_fabric.orchestrator import FhirIngestionOrchestrator
from fhir_fabric.logging_utils import get_logger

logger = get_logger("fabric_fhir_notebook")

config = FhirConfig(
    base_url=base_url,
    resources=("Patient", "Encounter", "Observation", "Condition"),
    page_size=int(page_size),
    lookback_days=int(lookback_days),
)

try:
    orchestrator = FhirIngestionOrchestrator(spark, config)
    pipeline_run_id = orchestrator.run()
    logger.info("Pipeline successful. run_id=%s", pipeline_run_id)
except Exception:
    logger.exception("FHIR medallion pipeline failed")
    raise
