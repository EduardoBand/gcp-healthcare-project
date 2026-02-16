/* ============================================================================================
   File: audit_table_ddl.sql
   Project: GCP Healthcare Data Platform
   Layer: Temp Dataset (Operational Metadata)
   Description:
       This script creates the audit_log table responsible for tracking ingestion
       and transformation execution metadata across the pipeline.

       The audit table is used to monitor:
         - Source system processed
         - Table loaded
         - Load type (Full / Incremental)
         - Number of records processed
         - Execution timestamp
         - Load status (Success / Failed)

       This table supports observability, troubleshooting, and data governance
       within the Medallion architecture.

   ============================================================================================ */

-- =============================================================================================
-- AUDIT TABLE CREATION
-- =============================================================================================

CREATE TABLE IF NOT EXISTS `healthcare-project-487014.temp_dataset.audit_log` (
    
    -- Source system identifier (e.g., hospital-a, hospital-b, claims)
    data_source STRING,
    
    -- Logical table name processed in the pipeline
    tablename STRING,
    
    -- Type of ingestion (FULL / INCREMENTAL)
    load_type STRING,
    
    -- Total number of records processed during execution
    record_count INT64,
    
    -- Timestamp of pipeline execution
    load_timestamp TIMESTAMP,
    
    -- Execution status (SUCCESS / FAILED)
    status STRING

);
