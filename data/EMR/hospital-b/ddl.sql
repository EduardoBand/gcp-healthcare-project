/* ============================================================================================

   Description:
       This script defines the OLTP schema for Hospital B source system.
       These tables represent the operational database extracted into the
       landing layer before transformation within the Medallion Architecture
       (Bronze → Silver → Gold).

       Note:
       Hospital B contains structural differences compared to Hospital A,
       particularly in the Patients table (different column names), which
       requires transformation and standardization in the Silver layer.

       Entities Covered:
         - Departments
         - Encounters
         - Patients
         - Providers
         - Transactions
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
-- Contains patient demographic information.
-- Column naming differs from Hospital A and must be standardized downstream.
-- =============================================================================================

CREATE TABLE patients (
    ID NVARCHAR(50) NOT NULL,
    F_Name NVARCHAR(50) NOT NULL,
    L_Name NVARCHAR(50) NOT NULL,
    M_Name NVARCHAR(50) NOT NULL,
    SSN NVARCHAR(50) NOT NULL,
    PhoneNumber NVARCHAR(50) NOT NULL,
    Gender NVARCHAR(50) NOT NULL,
    DOB DATE NOT NULL,
    Address NVARCHAR(100) NOT NULL,
    ModifiedDate DATE NOT NULL,
    CONSTRAINT PK_patients PRIMARY KEY (ID)
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
