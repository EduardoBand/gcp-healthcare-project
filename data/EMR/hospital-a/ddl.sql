/* ============================================================================================
   Description:
       This script defines the OLTP schema for Hospital A source system.
       These tables represent the operational data extracted from the MySQL
       transactional database and ingested into the landing zone before
       being processed in the Medallion Architecture (Bronze → Silver → Gold).

       Entities Covered:
         - Departments
         - Encounters
         - Patients
         - Providers
         - Transactions

       All tables are designed with primary keys to enforce entity integrity
       at the source system level.

   ============================================================================================ */


-- =============================================================================================
-- 1. DEPARTMENTS TABLE
-- Stores hospital department master data.
-- =============================================================================================

CREATE TABLE departments (
    DeptID NVARCHAR(50) NOT NULL,
    Name NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_departments PRIMARY KEY (DeptID)
);


-- =============================================================================================
-- 2. ENCOUNTERS TABLE
-- Represents patient visits and clinical interactions within departments.
-- =============================================================================================

CREATE TABLE encounters (
    EncounterID NVARCHAR(50) NOT NULL,
    PatientID NVARCHAR(50) NOT NULL,
    EncounterDate DATE NOT NULL,
    EncounterType NVARCHAR(50) NOT NULL,
    ProviderID NVARCHAR(50) NOT NULL,
    DepartmentID NVARCHAR(50) NOT NULL,
    ProcedureCode INT NOT NULL,
    InsertedDate DATE NOT NULL,
    ModifiedDate DATE NOT NULL,
    CONSTRAINT PK_encounters PRIMARY KEY (EncounterID)
);


-- =============================================================================================
-- 3. PATIENTS TABLE
-- Contains patient demographic and identification information.
-- =============================================================================================

CREATE TABLE patients (
    PatientID NVARCHAR(50) NOT NULL,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    MiddleName NVARCHAR(50) NOT NULL,
    SSN NVARCHAR(50) NOT NULL,
    PhoneNumber NVARCHAR(50) NOT NULL,
    Gender NVARCHAR(50) NOT NULL,
    DOB DATE NOT NULL,
    Address NVARCHAR(100) NOT NULL,
    ModifiedDate DATE NOT NULL,
    CONSTRAINT PK_hospital1_patient_data PRIMARY KEY (PatientID)
);


-- =============================================================================================
-- 4. PROVIDERS TABLE
-- Stores provider master data including specialization and department mapping.
-- =============================================================================================

CREATE TABLE providers (
    ProviderID NVARCHAR(50) NOT NULL,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Specialization NVARCHAR(50) NOT NULL,
    DeptID NVARCHAR(50) NOT NULL,
    NPI BIGINT NOT NULL,
    CONSTRAINT PK_providers PRIMARY KEY (ProviderID)
);


-- =============================================================================================
-- 5. TRANSACTIONS TABLE
-- Financial and billing records associated with patient encounters.
-- Includes claim, payor, and diagnostic information.
-- =============================================================================================

CREATE TABLE transactions (
    TransactionID NVARCHAR(50) NOT NULL,
    EncounterID NVARCHAR(50) NOT NULL,
    PatientID NVARCHAR(50) NOT NULL,
    ProviderID NVARCHAR(50) NOT NULL,
    DeptID NVARCHAR(50) NOT NULL,
    VisitDate DATE NOT NULL,
    ServiceDate DATE NOT NULL,
    PaidDate DATE NOT NULL,
    VisitType NVARCHAR(50) NOT NULL,
    Amount FLOAT NOT NULL,
    AmountType NVARCHAR(50) NOT NULL,
    PaidAmount FLOAT NOT NULL,
    ClaimID NVARCHAR(50) NOT NULL,
    PayorID NVARCHAR(50) NOT NULL,
    ProcedureCode INT NOT NULL,
    ICDCode NVARCHAR(50) NOT NULL,
    LineOfBusiness NVARCHAR(50) NOT NULL,
    MedicaidID NVARCHAR(50) NOT NULL,
    MedicareID NVARCHAR(50) NOT NULL,
    InsertDate DATE NOT NULL,
    ModifiedDate DATE NOT NULL,
    CONSTRAINT PK_transactions PRIMARY KEY (TransactionID)
);
