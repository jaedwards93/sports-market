**Sports Market Analytics Platform**

End-to-end sports odds analytics platform built with modern data engineering 
practices, system design, and production-style data modeling. The project ingests 
sportsbook odds data, normalizes it using dbt, orchestrates workflows with Dagster, 
and exposes analytics-ready gold tables for downstream analysis.

This project is designed like a real production system: deterministic batch runs, 
clear layer boundaries, replayability, and domain scalability.

------------------------------------------------------------------------------------
**High-Level Architecture**

**1. Bronze Layer (Ingestion (Python))**
   1. Pulls raw JSON from The Odds API
   2. Assigns a deterministic batch_id per run
   3. Stores meta data (Create Date)
   4. Persists raw payloads unchanged for auditability 
**2. Silver Layer (Staging / Flattening(dbt))**
   1. dbt models extract, flatten and type cast JSON payloads
   2. Create primary keys generated with dbt_utils
   3. Preserve raw/staging ids
   4. Individualized staging table for each endpoint
**3. Gold Layer (Analysis/Normalization (dbt))**
   1. Conformed, analytics-ready entities shared across domains
   2. Designed for BI tools, notebooks, or further modeling
**4. Orchestration (Dagster)**
   1. Asset-based dagster orchestration
   2. Clear lineage mapping and dependencies from ingestion to dbt models

------------------------------------------------------------------------------------
**Tech Stack**

- **Python**: ingestion and utilities
- **Postgres**: warehouse
- **dbt Core**: transformations, testing, documentation
- **Dagster**: orchestration and lineage
- **pytest**: ingestion-level testing

------------------------------------------------------------------------------------
**Repository Structure**

```
sports-market/
├── ingest/          # API ingestion logic
├── orchestration/   # Dagster assets and jobs
├── dbt/             # dbt models (bronze/silver/gold)
├── util/            # shared utilities (DB, batching, config)
├── tests/           # pytest-based tests
├── init_db/         # database bootstrap scripts
├── scratch.py       # local experimentation (non-prod)
```

------------------------------------------------------------------------------------
**Running Locally (High Level)**

1. Run init_db/bootstrap.py to initialize database
2. Run dagster assets to ingest raw data and build dbt models
