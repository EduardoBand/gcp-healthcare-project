/* =====================================================================================================
   FILE: bronze.sql
   LAYER: Bronze (Raw Layer)
   PLATFORM: Google BigQuery
   PROJECT: healthcare-project-487014

   DESCRIPTION:
   --------------------------------------------------------------------------------------
   This script creates EXTERNAL TABLES in the Bronze dataset pointing to raw JSON files
   stored in Google Cloud Storage (GCS).

   The Bronze layer represents the RAW ingestion layer in the Medallion Architecture.
   No transformations are applied here. Data is queried directly from GCS.

   IMPORTANT:
   - Replace the bucket name if deploying to another environment.
   - External tables DO NOT store data in BigQuery.
   - Data is read on-demand from GCS.
   - Schema is auto-detected from JSON structure.

   STORAGE PATH STRUCTURE:
   gs://<bucket>/landing/<hospital>/<entity>/*.json
===================================================================================================== */

*/
-- ============================================================================================
-- HOSPITAL A - EXTERNAL TABLES
-- ============================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.departments_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.encounters_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.patients_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.providers_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.transactions_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-a/transactions/*.json']
);


-- ============================================================================================
-- HOSPITAL B - EXTERNAL TABLES
-- ============================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.departments_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.encounters_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.patients_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.providers_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-project-487014.bronze_dataset.transactions_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket15041994/landing/hospital-b/transactions/*.json']
);
