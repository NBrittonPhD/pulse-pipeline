# PULSE PIPELINE — STYLE GUIDE  
**Author:** Noel  
**Purpose:** Define the strict coding, documentation, and structural standards used across the PRIME-AI PULSE Data Lake Pipeline.

This guide governs how **every script, function, SQL file, documentation artifact, and metadata object** must be written moving forward. It ensures consistency, reproducibility, traceability, and long-term maintainability across all pipeline steps.

---

# ============================================================
# OVERARCHING PHILOSOPHY
# ============================================================

1. **One function per script. Always.**  
2. **Extremely verbose, narrative-style annotation throughout every file.**  
3. **Clear USER INPUT SECTION at the top of user-facing scripts.**  
4. **All behavior must be metadata‑driven.**  
5. **Strict separation of concerns:**  
   - Wrappers = user interaction  
   - Step functions = orchestration  
   - Action functions = pure ingestion logic  
6. **All functions fail loudly, deterministically, and safely.**

---

# ============================================================
# DIRECTORY STRUCTURE
# ============================================================

```
r/
  scripts/
  steps/
  action/
  utilities/

sql/
  ddl/
  inserts/

config/
reference/
raw/
docs/
```

---

# ============================================================
# FUNCTION DESIGN STANDARDS
# ============================================================

Every function file must begin with:

```
# =============================================================================
# <Function Name>
# Purpose:
# Inputs:
# Outputs:
# Side Effects:
# Author:
# Last Updated:
# =============================================================================
```

---

# ============================================================
# TESTING STANDARDS
# ============================================================

All tests must verify:
- Correct DB writes  
- Error control  
- Deterministic behavior  
- Full coverage of allowed/blocked paths  

---

# ============================================================
# DOCUMENTATION REQUIRED PER PIPELINE STEP
# ============================================================

### ✔ Step README  
### ✔ SOP Document  
### ✔ JSON Snapshot  
### ✔ Function Dependency Diagram  
### ✔ SQL DDL Archive  

---

# ============================================================
# CODE ANNOTATION RULES
# ============================================================

Use:
- **Loud Section Headers**
- **Inline explanations**
- **Verbose messages**

---

# ============================================================
# NAMING CONVENTIONS
# ============================================================

| Object | Convention | Example |
|--------|------------|---------|
| source_id | lowercase | `cisir2026_test` |
| source_type | ALL CAPS | `CISIR` |
| ingest_id | ING_<source_id>_<timestamp> | `ING_tr2026_test_20251209_120000` |
| lake_table | lowercase | `cisir_vitals_minmax` |
| variable names | snake_case | `admit_date` |

---

# ============================================================
# HARD RULES
# ============================================================

1. **One function per script**  
2. **Metadata‑driven logic only**  
3. **Strict source_type enforcement**  
4. **Extremely verbose annotation**  
5. **User input section always present when needed**  
6. **No mixing responsibilities between layers**  
7. **No guessing logic**  
8. **Always produce the full documentation pack at the end of each step**  

---

# END OF STYLE GUIDE
