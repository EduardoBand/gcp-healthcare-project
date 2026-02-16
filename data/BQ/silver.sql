/* =====================================================================================================
   FILE: silver.sql
   LAYER: Silver (Cleansed & Conformed Layer)
   PLATFORM: Google BigQuery
   PROJECT: healthcare-project-487014

   DESCRIPTION:
   --------------------------------------------------------------------------------------
   The Silver layer applies:

   1. Data Cleansing
   2. Data Standardization
   3. Source Conformation (Hospital A & B)
   4. Data Quality Checks (Quarantine Flag)
   5. SCD Type 2 implementation for historization

   DESIGN PRINCIPLES:
   - Unified business keys (SourceID + datasource)
   - Quarantine logic for invalid records
   - is_current flag for SCD2
   - inserted_date / modified_date for auditability
   - Full refresh for simple dimensions
   - MERGE-based SCD Type 2 for historized entities
===================================================================================================== */



/* ============================================================================================
   SECTION 1: DEPARTMENTS (Full Refresh + Conformed Dimension)
============================================================================================ */

CREATE TABLE IF NOT EXISTS `healthcare-project-487014.silver_dataset.departments` (
    Dept_Id STRING,
    SRC_Dept_Id STRING,
    Name STRING,
    datasource STRING,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `healthcare-project-487014.silver_dataset.departments`;

INSERT INTO `healthcare-project-487014.silver_dataset.departments`
SELECT DISTINCT 
    CONCAT(deptid, '-', datasource) AS Dept_Id,
    deptid AS SRC_Dept_Id,
    Name,
    datasource,
    CASE 
        WHEN deptid IS NULL OR Name IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `healthcare-project-487014.bronze_dataset.departments_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `healthcare-project-487014.bronze_dataset.departments_hb`
);



/* ============================================================================================
   SECTION 2: PROVIDERS (Full Refresh + Conformed Dimension)
============================================================================================ */

CREATE TABLE IF NOT EXISTS `healthcare-project-487014.silver_dataset.providers` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI INT64,
    datasource STRING,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `healthcare-project-487014.silver_dataset.providers`;

INSERT INTO `healthcare-project-487014.silver_dataset.providers`
SELECT DISTINCT 
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    CAST(NPI AS INT64) AS NPI,
    datasource,
    CASE 
        WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `healthcare-project-487014.bronze_dataset.providers_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `healthcare-project-487014.bronze_dataset.providers_hb`
);



/* ============================================================================================
   SECTION 3: PATIENTS (SCD TYPE 2)
============================================================================================ */

CREATE TABLE IF NOT EXISTS `healthcare-project-487014.silver_dataset.patients` (
    Patient_Key STRING,
    SRC_PatientID STRING,
    FirstName STRING,
    LastName STRING,
    MiddleName STRING,
    SSN STRING,
    PhoneNumber STRING,
    Gender STRING,
    DOB INT64,
    Address STRING,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

-- Temporary quality validation layer
CREATE OR REPLACE TABLE `healthcare-project-487014.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_PatientID IS NULL 
          OR DOB IS NULL 
          OR FirstName IS NULL 
          OR LOWER(FirstName) = 'null'
        THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        PatientID AS SRC_PatientID,
        FirstName,
        LastName,
        MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosa' AS datasource
    FROM `healthcare-project-487014.bronze_dataset.patients_ha`
    
    UNION ALL

    SELECT DISTINCT 
        ID AS SRC_PatientID,
        F_Name AS FirstName,
        L_Name AS LastName,
        M_Name AS MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosb' AS datasource
    FROM `healthcare-project-487014.bronze_dataset.patients_hb`
);

-- SCD TYPE 2 MERGE
MERGE INTO `healthcare-project-487014.silver_dataset.patients` AS target
USING `healthcare-project-487014.silver_dataset.quality_checks` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = TRUE 

WHEN MATCHED AND (
    target.SRC_PatientID <> source.SRC_PatientID OR
    target.FirstName <> source.FirstName OR
    target.LastName <> source.LastName OR
    target.MiddleName <> source.MiddleName OR
    target.SSN <> source.SSN OR
    target.PhoneNumber <> source.PhoneNumber OR
    target.Gender <> source.Gender OR
    target.DOB <> source.DOB OR
    target.Address <> source.Address OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED 
THEN INSERT (
    Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Patient_Key,
    source.SRC_PatientID,
    source.FirstName,
    source.LastName,
    source.MiddleName,
    source.SSN,
    source.PhoneNumber,
    source.Gender,
    source.DOB,
    source.Address,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
);

DROP TABLE IF EXISTS `healthcare-project-487014.silver_dataset.quality_checks`;

