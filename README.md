# PySpark-DBT Lakehouse Project

## Overview

This project demonstrates the implementation of a modern Lakehouse Data Engineering pipeline using:

- PySpark
- Databricks
- Delta Lake
- Databricks Lakeflow Jobs
- dbt Core
- SQL
- Medallion Architecture

The project ingests raw CRM and ERP datasets into a Databricks Lakehouse environment, processes them through Bronze and Silver layers using PySpark, validates data quality, and finally creates a Gold layer star schema using dbt.

The pipeline is fully orchestrated using Databricks Lakeflow Jobs and includes:

- Streaming ingestion
- Incremental processing
- SCD Type 1 upserts
- SCD Type 2 snapshots
- Data validation framework
- Dimensional modeling
- dbt testing
- Enterprise-style orchestration

---

# Project Architecture

## Data Architecture Diagram

<img width="1100" height="850" alt="data architecture" src="https://github.com/user-attachments/assets/ca666569-bbb4-4476-8a9f-056483dae42e" />

---

## Data Lineage Diagram

<img width="1174" height="720" alt="data lineage" src="https://github.com/user-attachments/assets/10b792e3-5247-4df4-8a60-33ad810db6f5" />

---

## Data Integration Diagram

<img width="1225" height="825" alt="relationship" src="https://github.com/user-attachments/assets/4cc656b5-5e15-4f1a-af90-f34363b2f987" />

---

## Data Model: Star Schema

<img width="1100" height="713" alt="data model" src="https://github.com/user-attachments/assets/fcee9a4e-9b49-48ab-9751-f14e351c1c60" />

---

# Medallion Architecture

The project follows the Medallion Architecture pattern:

## Bronze Layer
- Raw data ingestion
- Streaming ingestion using PySpark Structured Streaming
- Delta table creation
- Minimal transformation
- Preserves source data
- Fault-tolerant ingestion using checkpointing

## Silver Layer
- Data cleaning
- Standardization
- Transformation
- Validation preparation
- Business rule implementation
- SCD Type 1 upserts
  
## Gold Layer
- Dimensional modeling
- Star schema creation
- Business-ready analytical tables
- dbt transformations
- dbt tests
- SCD Type 2 snapshots

---

# Tech Stack

## Technology----------------> Purpose
- PySpark------------------------>Distributed data processing
- SQL---------------------------->Transformations and validation
- Databricks--------------------->Lakehouse platform
- Delta Lake--------------------->Storage layer
- dbt Core----------------------->Gold layer transformation
- Databricks Lakeflow Jobs----->Pipeline orchestration
- Git & GitHub------------------>Version control

---

# Source System

The project integrates data from two enterprise systems:

## CRM System
### Customer Information
Contains customer master data.

### Product Information
Contains product master data.

### Sales Details
Contains transactional sales records.


## ERP System
### Customer Location
Contains customer geographic information.

### Extra Customer Information
Contains additional customer-related attributes.

### Product Categories
Contains product category and subcategory information.

---

# Pipeline Workflow

The entire pipeline is orchestrated using Databricks Lakeflow Jobs.

## Workflow Execution Order
- Lakehouse Initialization
- Bronze Layer Ingestion
- Parallel Silver Layer Transformations
- Parallel Validation Checks
- dbt Gold Layer Transformation
- dbt Tests
- Snapshot Generation

## Pipeline DAG
<img width="1575" height="729" alt="pipeline" src="https://github.com/user-attachments/assets/abe7a1d8-059d-4f7f-98bc-444415f45bf8" />


## Infrastructure Initialization

The first task initializes the Lakehouse environment.

### Responsibilities
- Create Catalogs
- Create Schemas
- Create Volumes
- Create Checkpoint Storage
- Prepare Lakehouse Environment

### Schemas created:

- bronze
- silver
- gold
- snapshots
  
## Bronze Layer

The Bronze layer performs raw ingestion of source data into Delta tables.

### Bronze Layer Objectives
- Preserve raw source data
- Enable scalable ingestion
- Support incremental processing
- Maintain fault tolerance

#### Streaming Ingestion

The Bronze layer uses PySpark Structured Streaming with readStream().

### Features Implemented
- Config-driven ingestion
- Streaming ingestion
- Delta Lake storage
- Schema inference
- Checkpointing
- Trigger-once batch processing


---

## Silver Layer

The Silver layer is responsible for cleaning, transforming, validating, and standardizing data.
The layer converts raw Bronze data into clean datasets.

### Transformation Framework

A reusable transformation framework was implemented using custom Python packages.

### Utility Package

Custom transformation logic is implemented inside:
- utils/custom_utils.py
This modular design improves:
- Reusability
- Maintainability
- Scalability
- Code organization

## Silver Layer Transformations

The following transformations were implemented:

### 1. Null Handling
Detection of null values
Replacement/removal of invalid records

### 2. Duplicate Removal

Duplicate records are removed using business keys.

### 3. Whitespace Removal

Leading and trailing whitespaces are cleaned.

### 4. Normalization

Text standardization and normalization logic was implemented.

Examples:

Standardized gender values
Standardized marital status values
Consistent formatting

### 5. Date Handling and Cleaning

Date columns were:

- standardized
- validated
- cleaned
- converted into proper date format
  
### 6. Business Logic Implementation

Custom business rules were implemented.

#### Sales Value Recovery

Missing sales values were calculated using:

- sales = quantity * price

This ensured consistency in transactional records.

### 7. SCD Type 1 Upserts

Master/reference tables were loaded using SCD Type 1 logic.

#### Implemented Using
Delta MERGE
Upsert logic

#### Applied To
Customers
Products
ERP reference tables

---

## Validation Layer

A dedicated validation framework was implemented before loading data into the Gold layer.

Validation logic was separated into reusable modules.

### Validation Package
- validation_utils/test_utils.py
 
## Validation Checks Implemented
### 1. Null Validation

Checks for null values in critical columns.

### 2. Duplicate Validation

Ensures no duplicate records exist.

### 3. Date Validation

Ensures dates are:
- valid
- standardized
- logically correct
- 
### 4. Referential Integrity Validation

Checks the presence of matching records across related tables.

Examples:

- Matching customers
- Matching products
- Valid relationships
  
### 5. Business Rule Validation

Validation of:

- sales = quantity * price

Ensures transactional consistency.

## Validation Failure Handling

The pipeline follows a fail-fast approach.

If validation fails:

- Exceptions are raised
- Workflow execution stops
- Downstream tasks do not execute

This ensures only trusted data reaches the Gold layer.

---

## Gold Layer (dbt)

The Gold layer was implemented using dbt Core.

The goal of the Gold layer is to create business-ready analytical models.

### Star Schema Modeling

The Gold layer follows a Star Schema design.

The six cleaned Silver tables were modeled into three analytical tables.
## Gold Layer Tables
### 1. dim_customers

Customer dimension table.

#### Features
- Integrated CRM + ERP customer data
- Cleaned and standardized attributes
- Surrogate keys
- Historical tracking support

### 2. dim_products

Product dimension table.

#### Features
- Product enrichment
- Category integration
- Surrogate keys
- Standardized product attributes

### 3. fact_sales

Transactional fact table.

#### Features
- Sales measures
- Quantity metrics
- Product relationships
- Customer relationships
- Foreign key references

---

# dbt Tests

dbt tests were implemented to ensure Gold layer data quality.

## Tests Implemented
### Null Tests

Ensures critical columns do not contain null values.

### Unique Tests

Ensures uniqueness of keys.

### Accepted values Tests
Ensures only the accepted values are allowed in a particular column.

### Singleton Business Logic Test

Custom test to validate:
- sales = price * quantity
This validates transactional consistency.

---

## SCD Type 2 Snapshots

dbt snapshots were implemented for historical tracking.

### Snapshot Tables
- customer_snapshot
- product_snapshot

Snapshots are stored inside the:
- snapshots schema

---

# Orchestration

The entire workflow is orchestrated using Databricks Lakeflow Jobs.

## Orchestration Features
### Parallel Execution

Silver transformations execute in parallel.

### Parallel Validation

CRM and ERP validation tasks execute simultaneously.

### Dependency Management

Tasks execute only after upstream dependencies succeed.

### Fault Tolerance

Failures stop downstream execution.

### Incremental Processing

The pipeline supports incremental processing using:

- Structured Streaming
- Delta Lake
- Checkpointing
- Upsert logic

