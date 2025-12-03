---

# Governance Documentation — Step 1: Source Registration

## Overview
Source registration is the beginning of all lineage, compliance, and reproducibility in the PULSE ecosystem. Step 1 produces formal governance artifacts, ensures metadata consistency, and establishes the ingestion contract for every data source.

---

## 1. Governance Tables Involved

### 1.1 governance.source_registry
**Purpose:**  
The authoritative record of every onboarded data source, its metadata, ingest method, update frequency, and active status.

**Key Fields:**
- source_id  
- source_name  
- system_type  
- update_frequency  
- data_owner  
- ingest_method  
- expected_schema_version  
- pii_classification  
- active  
- created_at  
- updated_at  

**Behavior:**
- Rows written only by `register_source()`
- Controlled vocab validation enforced
- Insert vs update logic based on `source_id`

---

### 1.2 governance.audit_log
**Purpose:**  
Captures structured governance and lineage events.

**Step 1 Behavior:**
- Event type: `source_registration`
- Includes create/update distinction
- Contains full metadata as JSON
- Required for compliance and provenance

**Key Fields:**
- audit_id (UUID)  
- event_type  
- event_time  
- actor  
- details (JSON)  

---

### 1.3 governance.pipeline_step
**Purpose:**  
Records execution details of each pipeline step for a given ingest event.

**Key Fields:**
- ingest_id  
- step_name  
- status  
- executed_at  
- metadata (JSON)  

**Step 1 Behavior:**
- Written by `run_step1_register_source()`
- Status set to `"success"` or `"error"`

---

## 2. Governance Outputs Produced by Step 1
- Valid row in `source_registry`
- Audit log entry documenting registration
- Pipeline step record noting execution metadata
- Folder tree created according to `directory_structure.yml`

---

## 3. Why This Matters
- Ensures transparency for regulators and auditors
- Establishes permanent source metadata
- Provides lineage for ingestion and transformation steps
- Forms the foundation for schema validation and harmonization

---

## 4. Compliance Notes
- Controlled vocabularies must remain stable or versioned
- All metadata changes must go through `register_source()`
- Audit log cannot be backfilled or edited outside governance-approved processes

---
