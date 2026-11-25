# FHIR API Ingestion & Analytics — Microsoft Fabric

Production-oriented reference implementation for the supplied assignment. It ingests FHIR R4 resources in the required order **Patient → Encounter → Observation → Condition**, follows FHIR Bundle `link[relation=next]` pagination, stores original Bundle JSON pages in Lakehouse Files, writes Bronze Delta tables, creates Silver current/history records using SCD Type 2, and exposes Gold reporting views.

## 1. Architecture

```text
HAPI FHIR R4 REST API
       |
       | _lastUpdated=ge<UTC start>, _count, server-provided next links
       v
Fabric Spark Notebook / Orchestrator
       |
       +--> Raw: Files/raw/fhir/<Resource>/YYYY/MM/DD/run_id=<uuid>/page_00001.json
       |    Original Bundle payload; immutable page-level landing files
       |
       +--> Bronze Delta: bronze_fhir_patient / encounter / observation / condition
       |    One row per FHIR resource version; JSON retained in resource_json
       |
       +--> Silver Delta: silver_fhir_<resource>
       |    Common searchable columns + complete resource_json + SCD2 validity columns
       |
       +--> Gold views: gold_patient / gold_encounter / gold_observation / gold_condition
       |
       +--> Audit Delta: etl_api_audit
            API start/end, data save timestamp, window, page/record counts and status
```

### Layer definitions
- **Raw**: immutable API Bundle responses, date-bucketed and grouped by resource/run. The complete response is persisted before parsing.
- **Bronze**: append-only resource-version records with `extraction_timestamp`, `api_url_or_params`, `source_file`, `ingestion_run_id`, `last_updated`, and a SHA-256 content hash.
- **Silver**: deduplicated current resource state plus historical versions. A changed resource closes its previous current row (`is_current=false`, `valid_to`) and inserts a new current version.
- **Gold**: current-state reporting views. Full JSON remains available for resource-specific attributes without prematurely discarding fields from the FHIR specification.

## 2. Deploy to Fabric

1. Create a Fabric **Lakehouse** and a Spark **Notebook**. Attach the Lakehouse as the notebook's default Lakehouse.
2. Upload the `src/fhir_fabric` folder contents to the Lakehouse `Files/code/fhir_fabric/` path. The resulting import path must be `/lakehouse/default/Files/code/fhir_fabric`.
3. Upload `notebooks/01_fhir_medallion_pipeline.py` as a notebook or copy its cells into a new Fabric notebook. Set the first cell (`lookback_days`, `page_size`, `base_url`) as the **parameter cell**.
4. Run interactively once. The notebook creates the Delta tables and Gold views. Verify `Files/raw/fhir/` and the Lakehouse Tables section.
5. Create a Fabric Data Pipeline and add a Notebook activity pointing to this notebook. Set `lookback_days=3` and `page_size=100` under Base parameters. Schedule it daily.
6. The orchestrator executes resources in the required order. A resource failure is audited and logged; remaining resources are attempted, then the notebook fails at the end so the Fabric pipeline run is marked failed.

Fabric notebook parameters are supported through a designated parameter cell; see Microsoft Learn: https://learn.microsoft.com/en-us/fabric/data-engineering/author-execute-notebook

## 3. Local development

Python 3.10+:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt pytest
PYTHONPATH=src pytest -q
```

The orchestration/raw writer is Fabric-runtime dependent (`notebookutils` and Spark). Unit-test pure config logic locally; integration-test the full pipeline in a Fabric workspace.

## 4. Incremental strategy and versioning

- The default run uses a rolling 3-day UTC window with `_lastUpdated=ge<start>` and server-driven pagination. The overlap intentionally catches late-arriving updates and makes daily retries safe.
- HAPI's server-side paging links are followed exactly; the client does not construct offsets. `_count` is a requested page size, not a guarantee that the server returns that exact count.
- Each resource is hashed from canonicalized JSON. Silver compares the incoming latest hash with the current hash for the same resource ID. Equal hashes do not create a new SCD2 version.
- The public HAPI test server is shared and mutable. It is suitable for assignment/demo validation, not a production healthcare source. For a production deployment, configure an approved FHIR endpoint, authentication, data governance, PHI controls, network policy, and retention policy.

## 5. Table relationships

FHIR references are stored as native FHIR reference strings (for example `Patient/123`) in `subject_reference`, `patient_reference`, and `encounter_reference` where present. These references are not assumed to be valid foreign keys: FHIR resources can be missing, versioned, or externally referenced. Resolve them with controlled joins in downstream Gold models.

| Table | Grain | Key | Versioning |
|---|---|---|---|
| `bronze_fhir_patient` etc. | One row per extracted resource version | `resource_id + resource_hash + ingestion_run_id` | Append-only |
| `silver_fhir_patient` etc. | One row per resource version | `resource_id + valid_from + resource_hash` | SCD Type 2 |
| `etl_api_audit` | One row per resource per pipeline run | `ingestion_run_id + resource_type` | Append-only |
| `gold_patient` etc. | One row per current resource | Resource ID | Current-only view |

## 6. Operational considerations

- **Retries**: transient HTTP failures (429/5xx/network errors) are retried with exponential backoff and `Retry-After` support.
- **Timeouts**: every HTTP request has a finite timeout.
- **Pagination safety**: next links are restricted to the configured HTTPS host to prevent an API response from redirecting the client to an untrusted host.
- **Observability**: structured log fields include run ID, resource, page, counts, and failures; audit data is persisted in Delta.
- **Idempotency**: Bronze may contain repeated observations across overlapping windows, but Silver deduplicates by resource ID and content hash.
- **Schema evolution**: the complete JSON is retained; Silver exposes stable common columns. Add typed columns through a reviewed migration rather than relying on uncontrolled schema inference.
- **Security**: no secrets are required for the public demo endpoint. For authenticated endpoints, retrieve credentials from Fabric connections/Key Vault and never hardcode them.
- **Scale**: for large production extracts, checkpoint page-level ingestion state and use bounded parallelism per resource only if the source API permits it. Keep one ordered orchestration chain for this assignment.

## 7. Known scope notes

1. The assignment requests JSON or XML; this implementation uses JSON. XML is an optional extension and is not included.
2. Gold objects are Spark SQL views in the notebook's Lakehouse catalog. If the workspace uses a schema-enabled Lakehouse, adapt the table naming/catalog qualification to that Lakehouse's schema conventions.
3. The code persists raw responses via Fabric `notebookutils.fs`; run it in a Fabric notebook with the default Lakehouse attached.
4. HAPI server retention and test data can change. A successful API response may legitimately contain zero entries for a resource/time window.
