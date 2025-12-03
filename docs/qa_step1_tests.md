# QA & Testing — Step 1: Source Registration

## Overview
Step 1 quality assurance ensures that new source onboarding is correct, reproducible, and compliant. Tests are divided into unit tests for functions and integration tests for pipeline execution.

---

## 1. Unit Tests

### 1.1 Coverage Expectations
- Required fields validation
- Controlled vocabulary validation
- Acceptance of valid entries
- Folder creation logic
- Audit logging behavior
- Insert/update logic in `source_registry`
- Pipeline step recording through the Step 1 wrapper

### 1.2 Unit Test Files
- `tests/testthat/test_step1_register_source.R`

### 1.3 Expected Behavior
- All tests pass with no warnings
- Side-effects (folder creation, DB writes) occur only where expected
- Errors are clear, structured, and actionable

---

## 2. Integration Tests

### 2.1 Purpose
Verify Step 1 executes correctly when run inside the pipeline.

### 2.2 Test File
- `tests/testthat/test_step1_integration.R`

### 2.3 Coverage
- Pipeline loads settings
- Pipeline loads source parameters
- `run_pipeline()` executes Step 1 automatically
- `register_source()` performs full workflow
- Folder structure created correctly
- `source_registry`, `audit_log`, and `pipeline_step` updated

### 2.4 Expected Output
- Step 1 recorded in `pipeline_step` with status `"success"`
- One or more audit log rows referencing the onboarded source
- Source registry contains the new source_id

---

## 3. Test Execution Commands

### Run all Step 1 tests:

testthat::test_dir("tests/testthat")

### Run specific tests:
testthat::test_file("tests/testthat/test_step1_register_source.R")
testthat::test_file("tests/testthat/test_step1_integration.R")

## 4. QA Operational Guidelines
- Any modifications to Step 1 functions require re-running the entire test suite
- Changing vocabulary or directory structure requires updating tests and docs
- CI/CD should block merges when Step 1 tests fail
- Local development should always run tests before onboarding a real source

## 5. When QA Declares Step 1 “Complete”
- All unit tests pass
- All integration tests pass
- Folder creation behaves consistently across OSes
- DB writes match expectations
- Pipeline execution is stable and repeatable

