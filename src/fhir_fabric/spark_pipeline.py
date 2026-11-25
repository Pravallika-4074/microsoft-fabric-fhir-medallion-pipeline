from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from .logging_utils import get_logger
from .record_parser import parse_bundle_entries


class SparkMedallionProcessor:
    """Spark transformations and Delta persistence. One instance per notebook run."""
    def __init__(self, spark: SparkSession, bronze_prefix: str = "bronze_fhir_",
                 silver_prefix: str = "silver_fhir_", audit_table: str = "etl_api_audit") -> None:
        self.spark = spark
        self.bronze_prefix = bronze_prefix
        self.silver_prefix = silver_prefix
        self.audit_table = audit_table
        self.logger = get_logger(f"{__name__}.SparkMedallionProcessor")

    @staticmethod
    def _entry_schema() -> T.StructType:
        return T.StructType([
            T.StructField("resource_type", T.StringType(), True),
            T.StructField("resource_id", T.StringType(), True),
            T.StructField("last_updated", T.TimestampType(), True),
            T.StructField("resource_json", T.StringType(), False),
            T.StructField("resource_hash", T.StringType(), False),
            T.StructField("extraction_timestamp", T.TimestampType(), False),
            T.StructField("api_url_or_params", T.StringType(), False),
            T.StructField("source_file", T.StringType(), False),
            T.StructField("ingestion_run_id", T.StringType(), False),
        ])

    def bundle_to_records(self, bundle: dict[str, Any], resource_type: str,
                          extracted_at: datetime, api_params: str,
                          source_file: str, run_id: str) -> list[dict[str, Any]]:
        return parse_bundle_entries(bundle, resource_type, extracted_at,
                                    api_params, source_file, run_id)

    def write_bronze(self, resource_type: str, records: list[dict[str, Any]]) -> int:
        if not records:
            self.logger.info("No records to write to Bronze for %s", resource_type)
            return 0
        df = self.spark.createDataFrame(records, schema=self._entry_schema())
        table = f"{self.bronze_prefix}{resource_type.lower()}"
        (df.dropDuplicates(["resource_id", "resource_hash", "ingestion_run_id"])
           .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table))
        count = len(records)
        self.logger.info("Bronze write completed table=%s records=%s", table, count)
        return count

    def _silver_projection(self, bronze_table: str) -> DataFrame:
        df = self.spark.table(bronze_table)
        # Retain the full resource JSON for extensibility while exposing common FHIR fields.
        return (df.withColumn("fhir_version", F.lit("R4"))
            .withColumn("status", F.get_json_object("resource_json", "$.status"))
            .withColumn("subject_reference", F.get_json_object("resource_json", "$.subject.reference"))
            .withColumn("patient_reference", F.get_json_object("resource_json", "$.patient.reference"))
            .withColumn("encounter_reference", F.get_json_object("resource_json", "$.encounter.reference"))
            .withColumn("effective_datetime", F.coalesce(
                F.get_json_object("resource_json", "$.effectiveDateTime"),
                F.get_json_object("resource_json", "$.effectivePeriod.start"),
                F.get_json_object("resource_json", "$.issued"),
                F.get_json_object("resource_json", "$.recordedDate")))
            .withColumn("patient_gender", F.get_json_object("resource_json", "$.gender"))
            .withColumn("patient_birth_date", F.get_json_object("resource_json", "$.birthDate"))
            .withColumn("encounter_class_code", F.get_json_object("resource_json", "$.class.code"))
            .withColumn("period_start", F.get_json_object("resource_json", "$.period.start"))
            .withColumn("period_end", F.get_json_object("resource_json", "$.period.end"))
            .withColumn("code_text", F.coalesce(
                F.get_json_object("resource_json", "$.code.text"),
                F.get_json_object("resource_json", "$.code.coding[0].display")))
            .withColumn("observation_value", F.get_json_object("resource_json", "$.valueQuantity.value"))
            .withColumn("observation_unit", F.get_json_object("resource_json", "$.valueQuantity.unit"))
            .withColumn("condition_clinical_status", F.get_json_object("resource_json", "$.clinicalStatus.coding[0].code"))
            .withColumn("condition_verification_status", F.get_json_object("resource_json", "$.verificationStatus.coding[0].code"))
            .withColumn("is_current", F.lit(True))
            .withColumn("valid_from", F.coalesce(F.col("last_updated"), F.col("extraction_timestamp")))
            .withColumn("valid_to", F.lit(None).cast(T.TimestampType()))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp()))

    def apply_scd2(self, resource_type: str) -> int:
        bronze_table = f"{self.bronze_prefix}{resource_type.lower()}"
        silver_table = f"{self.silver_prefix}{resource_type.lower()}"
        if not self.spark.catalog.tableExists(bronze_table):
            self.logger.info("Bronze table does not exist: %s", bronze_table)
            return 0
        source = self._silver_projection(bronze_table)
        # A single deterministic latest version per resource/hash is processed per run.
        from pyspark.sql.window import Window
        window = Window.partitionBy("resource_id").orderBy(
            F.col("last_updated").desc_nulls_last(), F.col("extraction_timestamp").desc())
        source = (source.withColumn("_rn", F.row_number().over(window))
                        .filter(F.col("_rn") == 1).drop("_rn"))
        if not self.spark.catalog.tableExists(silver_table):
            (source.write.format("delta").mode("overwrite").saveAsTable(silver_table))
            count = source.count()
            self.logger.info("Initial SCD2 table created table=%s rows=%s", silver_table, count)
            return count

        target = DeltaTable.forName(self.spark, silver_table)
        current = self.spark.table(silver_table).filter(F.col("is_current") == True).select(
            "resource_id", F.col("resource_hash").alias("target_hash"))
        changes = source.alias("s").join(current.alias("t"), "resource_id", "left")
        changed_ids = changes.filter(F.col("target_hash").isNotNull() &
                                     (F.col("resource_hash") != F.col("target_hash"))) \
                             .select("resource_id").distinct()
        now = F.current_timestamp()
        # Close previous versions first. The incoming version is inserted in the next step.
        (target.alias("t").merge(changed_ids.alias("c"),
             "t.resource_id = c.resource_id AND t.is_current = true")
             .whenMatchedUpdate(set={"is_current": F.lit(False), "valid_to": now,
                                     "updated_at": now}).execute())

        current_after_close = self.spark.table(silver_table).filter(F.col("is_current") == True) \
            .select("resource_id")
        new_or_changed = source.join(current_after_close, "resource_id", "left_anti")
        # Insert-only MERGE makes reruns idempotent for the current resource hash.
        (DeltaTable.forName(self.spark, silver_table).alias("t")
            .merge(new_or_changed.alias("s"),
                   "t.resource_id = s.resource_id AND t.is_current = true")
            .whenNotMatchedInsertAll().execute())
        inserted = new_or_changed.count()
        self.logger.info("SCD2 merge completed table=%s inserted_versions=%s", silver_table, inserted)
        return inserted

    def ensure_audit_table(self) -> None:
        schema = T.StructType([
            T.StructField("ingestion_run_id", T.StringType(), False),
            T.StructField("resource_type", T.StringType(), False),
            T.StructField("api_call_started_at", T.TimestampType(), False),
            T.StructField("api_call_completed_at", T.TimestampType(), False),
            T.StructField("data_saved_at", T.TimestampType(), True),
            T.StructField("request_window_start", T.TimestampType(), True),
            T.StructField("request_window_end", T.TimestampType(), True),
            T.StructField("pages_fetched", T.LongType(), False),
            T.StructField("records_fetched", T.LongType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("error_message", T.StringType(), True),
        ])
        if not self.spark.catalog.tableExists(self.audit_table):
            self.spark.createDataFrame([], schema).write.format("delta").saveAsTable(self.audit_table)

    def write_audit(self, record: dict[str, Any]) -> None:
        self.ensure_audit_table()
        schema = self.spark.table(self.audit_table).schema
        self.spark.createDataFrame([record], schema=schema).write.format("delta").mode("append").saveAsTable(self.audit_table)

    def create_gold_views(self, resources: tuple[str, ...]) -> None:
        for resource in resources:
            table = f"{self.silver_prefix}{resource.lower()}"
            view = f"gold_{resource.lower()}"
            if self.spark.catalog.tableExists(table):
                self.spark.sql(f"""CREATE OR REPLACE VIEW {view} AS
                    SELECT * FROM {table} WHERE is_current = true""")
                self.logger.info("Gold view created view=%s", view)
        if all(self.spark.catalog.tableExists(f"{self.silver_prefix}{r}")
               for r in ("patient", "encounter", "observation", "condition")):
            self.spark.sql(f"""CREATE OR REPLACE VIEW gold_observation_reporting AS
                SELECT o.resource_id AS observation_id,
                       o.status AS observation_status,
                       o.subject_reference AS patient_reference,
                       o.encounter_reference,
                       o.code_text,
                       o.observation_value,
                       o.observation_unit,
                       o.effective_datetime,
                       o.last_updated,
                       o.resource_json
                FROM {self.silver_prefix}observation o
                WHERE o.is_current = true""")
            self.logger.info("Gold reporting view created: gold_observation_reporting")
