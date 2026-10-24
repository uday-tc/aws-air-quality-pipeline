# India Air Quality Data Pipeline (AWS)

An end-to-end **batch data engineering pipeline**: it ingests hourly air-quality readings for 7 Indian cities, cleans them, validates them with a data-quality gate, loads a **star-schema warehouse**, and serves a dashboard. Infrastructure on AWS is defined with **Terraform**; orchestration is an **Airflow DAG**.

```
 Open-Meteo API ──► RAW (S3, Parquet, partitioned by ingest_date, immutable)
                      │  transform.py  (dedupe, range checks, gap-fill, AQI category)
                      ▼
                   CLEANED (Parquet)
                      │  quality.py    (13 checks; blocking failures stop the run)
                      ▼
                   WAREHOUSE (star schema: dim_city, dim_time, fact_air_quality)
                      │  load.py       (idempotent rebuild + curated marts)
                      ▼
                   CURATED (Parquet marts) ──► Athena / QuickSight / Streamlit
```

Orchestrated by Airflow: `ingest_raw → transform_clean → quality_gate → load_warehouse → publish_to_s3`.

## Tech stack
Python · pandas · PyArrow/Parquet · DuckDB (local warehouse) · SQL (star schema, window functions) · Apache Airflow · AWS S3 / Glue Catalog / Athena / IAM / Budgets · Terraform · Streamlit · pytest · GitHub Actions

## Quick start (no AWS account needed)
```bash
pip install -r requirements.txt && pip install -e .
make run          # offline run on deterministic sample data
make run-api      # real data from the free Open-Meteo API (needs internet)
make test         # 10 tests
make dashboard    # http://localhost:8501
```

## Design decisions (what interviewers ask about)
| Decision | Why |
|---|---|
| **Medallion layers** (raw / cleaned / curated) | Raw is immutable, so any bug can be fixed and replayed without re-calling the API. |
| **Parquet, partitioned by `ingest_date`** | Columnar and compressed, and Athena only scans what it needs, which keeps cost low. |
| **Star schema** with `dim_city`, `dim_time`, `fact_air_quality` | Standard analytics model. The fact grain is one row per (city, hour) and is enforced by a primary key plus a test. |
| **Idempotent load** | Re-running a day never creates duplicates. Tested in `test_pipeline_is_idempotent`. |
| **Quality gate before the warehouse** | Bad data fails the run and never reaches consumers. Freshness is a non-blocking warning. |
| **No retries on the quality gate** | Retrying cannot fix bad data. Ingest and load retry with exponential back-off. |
| **One failed city doesn't kill ingestion** | Per-city try/except, and the run fails only if nothing was ingested. |
| **Cost guardrails in Terraform** | Athena 1 GB scan cutoff, S3 lifecycle rules, and a monthly budget alert. |
| **Least-privilege IAM** | The pipeline role gets only the lake bucket and the Athena/Glue calls it needs. |

## Data cleaning rules (counted and logged on every run)
Unparseable timestamps dropped · duplicates on `(city, observed_at)` removed keeping the latest ingest · physically impossible values (negative, or above range) set to null · gaps of up to 3 hours linearly interpolated per city · CPCB-style AQI category derived from PM2.5.

Sample run: `rows_in=518 → rows_out=504`, with `duplicates_removed=14`, `out_of_range_nulled=9`, `nulls_filled_by_interpolation=23`.

## Data quality checks (`quality.py`)
Non-empty · no null keys · unique `(city, hour)` · value ranges for 5 pollutants · null % ≤ 5% per pollutant · all configured cities present · freshness ≤ 72h (warning only). The report is written to `data/quality_report.json` and shown in the dashboard.

## Example insights (from the sample data)
```sql
SELECT * FROM mart_city_ranking;   -- Delhi 90.98 | Kolkata 61.80 | Mumbai 49.98 ... Hassan 21.20 (avg PM2.5)
SELECT * FROM mart_hourly_pattern; -- PM2.5 peaks at 9 AM (~78) and 7 PM (~78), lowest at night (~31)
```
Sample data is synthetic with realistic rush-hour patterns. Use `make run-api` for real readings.

## Deploying on AWS
```bash
cd terraform
terraform init
terraform apply -var="alert_email=you@example.com"   # S3 lake, Glue DB, Athena workgroup, IAM, budget
aws s3 sync ../data s3://$(terraform output -raw lake_bucket)/ --exclude warehouse.duckdb
# ... query with Athena, then clean up to avoid charges:
terraform destroy -var="alert_email=you@example.com"
```
Free-tier friendly: Athena over Parquet instead of a running Redshift cluster. `force_destroy` is enabled on the bucket so `terraform destroy` removes everything (dev setting).

To run the DAG, copy `dags/air_quality_dag.py` to your Airflow `dags/` folder (or an MWAA bucket), install this package, and set the Airflow variables `pipeline_bucket` and `pipeline_data_dir`.

## Testing
`pytest` covers cleaning rules, each quality check, the star-schema integrity (no orphan keys, unique grain), idempotency, and that a failing quality gate prevents the warehouse from being created. CI runs the tests, a smoke run of the pipeline, and `terraform fmt` / `validate` on every push.

## Project structure
```
src/pipeline/   config · ingest · transform · quality · load · run_pipeline
dags/           Airflow DAG
sql/            star-schema DDL
terraform/      S3, Glue, Athena, IAM, Budgets
dashboard/      Streamlit app
tests/          pytest suite
.github/        CI workflow
```

## Possible extensions
Kinesis/Kafka streaming ingestion · dbt models on top of the warehouse · Great Expectations · partitioned Athena tables with partition projection · SCD2 on `dim_city` · Slack/SNS alerts on quality failure.

## Author
Udaya Shankar T C · [GitHub](https://github.com/uday-tc) · [LinkedIn](https://linkedin.com/in/uday03)
