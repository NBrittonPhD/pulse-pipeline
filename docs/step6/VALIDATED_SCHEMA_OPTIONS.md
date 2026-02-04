# Validated Schema Design Options

**Purpose:** Propose different approaches for harmonizing staging tables into validated tables.
**Decision needed by:** [Your collaborators]
**Author:** Noel
**Date:** 2026-02-03

---

## Current State

**47 staging tables across 3 sources:**

| Source | Tables | Notable Content |
|--------|--------|-----------------|
| CISIR | 13 | Encounters, labs, meds, procedures, vitals, blood, diagnoses, trauma scores |
| CLARITY | 7 | Encounters, labs, meds, flowsheets, microbiology |
| TRAUMA_REGISTRY | 27 | Demographics+scores, labs (15 year tables), blood, injuries, EMS, insurance, PMH |

---

## Key Design Questions

1. **How much consolidation?** Combine similar tables (e.g., all labs → one table) or keep domain distinctions?
2. **Wide vs long?** Blood products have 250+ columns in crosstab format — keep wide or unpivot to long?
3. **What's the unit of analysis?** Encounter-level? Patient-level? Event-level?
4. **Source-specific fields?** Include all columns or only those common across sources?

---

## Option A: Minimal (5 Tables)

**Philosophy:** Maximum consolidation. One table per major analytical domain. Best for cross-source queries but loses granularity.

| Table | Sources Combined | Row Unit |
|-------|-----------------|----------|
| `validated.encounters` | cisir_encounter, tr_demo_scores, clarity_pat_enc + patient | One row per admission |
| `validated.clinical_events` | Labs, vitals, meds, procedures (all sources) | One row per event |
| `validated.diagnoses` | dx, complications, injuries (all sources) | One row per diagnosis/injury |
| `validated.blood_products` | cisir_blood, tr_blood (unpivoted to long) | One row per product per hour |
| `validated.prehospital` | EMS tables (TR only) | One row per EMS event |

**Pros:**
- Simple to query across sources
- Fewer tables to maintain
- Forces standardization

**Cons:**
- `clinical_events` becomes very heterogeneous (labs mixed with vitals mixed with meds)
- Loses source-specific detail
- Wide tables must be unpivoted (complex transformation)

---

## Option B: Domain-Aligned (11 Tables)

**Philosophy:** One table per clinical domain. Balances consolidation with domain clarity. This was the original proposal.

| Table | Sources Combined | Notes |
|-------|-----------------|-------|
| `validated.encounters` | cisir_encounter, tr_demo_scores, clarity_pat_enc | Demographics, admission, discharge |
| `validated.vitals` | cisir_vitals_minmax, tr_pta_vitals, clarity_pat_enc_flw | Keep wide or unpivot? |
| `validated.labs` | cisir_labs, tr_labs_*, clarity_lab_results | All lab results |
| `validated.medications` | cisir_meds, clarity_order_med | TR has no meds table |
| `validated.procedures` | cisir_proc, cisir_or_time, tr_operations, tr_non_op_procs | Surgical + non-surgical |
| `validated.diagnoses` | cisir_dx | ICD codes only |
| `validated.complications` | cisir_complications, tr_complications | Complication events |
| `validated.injuries` | cisir_di_aisiss, cisir_trauma_score, tr_injury_* | AIS/ISS trauma scoring |
| `validated.blood_products` | cisir_blood, tr_blood | Keep wide or unpivot? |
| `validated.ems` | tr_ems_procs, tr_ems_times | TR only (prehospital) |
| `validated.pmh` | cisir_preexisting, tr_pmh | Past medical history |

**Not included (no cross-source equivalent):**
- `clarity_order_micro_*` (microbiology) — Clarity only
- `tr_insurance` — TR only

**Pros:**
- Clear domain boundaries
- Easier to understand what's in each table
- Matches clinical workflows

**Cons:**
- 11 tables is still a lot
- Some tables only have 1-2 sources

---

## Option C: Research-Question-Driven (Custom)

**Philosophy:** Design around your specific research questions rather than data domains.

**Example research questions and implied tables:**

| Research Question | Implied Table(s) |
|-------------------|------------------|
| "What predicts mortality after trauma?" | `validated.trauma_cohort` (encounters + scores + outcomes) |
| "How do lab values change over time?" | `validated.lab_trajectories` (labs with time series structure) |
| "What blood products are given in first 24h?" | `validated.resuscitation` (blood + vitals in early window) |
| "What procedures happen and when?" | `validated.interventions` (all procedures with timing) |

**Pros:**
- Directly supports analysis
- Can optimize structure for common queries

**Cons:**
- Requires knowing research questions upfront
- May need to redesign if questions change
- Less general-purpose

---

## Option D: Hybrid (7 Tables)

**Philosophy:** Consolidate where it makes sense, keep separate where domains are distinct.

| Table | Content | Sources |
|-------|---------|---------|
| `validated.encounters` | Demographics, admission, discharge, trauma scores | All 3 |
| `validated.labs` | All laboratory results | All 3 |
| `validated.medications` | Medication orders/administrations | CISIR, Clarity |
| `validated.procedures` | All procedures (surgical + non-surgical + OR times) | CISIR, TR |
| `validated.injuries_diagnoses` | AIS/ISS injuries + ICD diagnoses + complications | CISIR, TR |
| `validated.blood_products` | Transfusion data (consider unpivoting) | CISIR, TR |
| `validated.vitals_flowsheets` | Vital signs and flowsheet data | All 3 |

**Unique/excluded:**
- EMS data (TR only) → keep as `staging.trauma_registry_ems_*` or add `validated.ems`
- Microbiology (Clarity only) → keep as staging or add `validated.microbiology`
- Insurance (TR only) → keep as staging
- PMH → merge into encounters or separate table?

---

## Blood Products: Wide vs Long

Both CISIR and TR have ~250 column crosstab tables for blood products:

**Current (Wide):**
```
account | prbc_0 | ffp_0 | plat_0 | prbc_1 | ffp_1 | plat_1 | ...
--------|--------|-------|--------|--------|-------|--------|
12345   | 2      | 1     | 0      | 1      | 2     | 1      |
```

**Alternative (Long):**
```
account | hour | product_type | units
--------|------|--------------|------
12345   | 0    | prbc         | 2
12345   | 0    | ffp          | 1
12345   | 1    | prbc         | 1
12345   | 1    | ffp          | 2
12345   | 1    | plat         | 1
```

**Recommendation:** Consider storing long format in `validated` (easier to query, aggregate, visualize) but this is a significant transformation.

---

## Vitals: Wide vs Long

`cisir_vitals_minmax` has 124 columns (min/max values for many vital signs).

Similar question: keep wide or unpivot to long format?

---

## Questions for Collaborators

1. **Which option (A/B/C/D) best fits your research workflow?**

2. **What are your top 3-5 research questions?** (This helps prioritize which tables matter most)

3. **Wide vs long for blood products and vitals?**
   - Wide = faster for single-patient queries, matches source format
   - Long = easier for aggregation, visualization, time-series analysis

4. **What to do with source-unique tables?**
   - Microbiology (Clarity only)
   - EMS (TR only)
   - Insurance (TR only)
   - Keep in staging? Create validated tables anyway?

5. **Should `validated.encounters` include trauma scores (ISS, TRISS)?**
   - Option: Include them (convenient for trauma research)
   - Option: Separate `validated.trauma_scores` table (cleaner separation)

6. **Patient vs Encounter level?**
   - Some sources have patient tables (clarity_patient_ustc)
   - Should validated schema be encounter-centric or have a separate patient dimension?

---

## Next Steps

1. Circulate this document for feedback
2. Schedule brief meeting to discuss preferences
3. Finalize schema design
4. Implement harmonization mappings

---

## Appendix: Source Table Inventory

### CISIR (13 tables)
- cisir_encounter (37 cols) — demographics, admission, discharge
- cisir_vitals_minmax (124 cols) — vital signs min/max
- cisir_labs_wth_grp (15 cols) — lab results
- cisir_meds_wth_grp (17 cols) — medications
- cisir_blood_crosstab (251 cols) — blood products by hour
- cisir_proc (7 cols) — procedures
- cisir_or_time (18 cols) — OR timing
- cisir_px (9 cols) — procedure codes
- cisir_dx (10 cols) — diagnoses
- cisir_complications (10 cols) — complications
- cisir_trauma_score (19 cols) — ISS, AIS, TRISS
- cisir_di_aisiss_final (11 cols) — detailed AIS injuries
- cisir_preexisting_conditions (9 cols) — PMH

### CLARITY (7 tables)
- clarity_pat_enc_ustc (14 cols) — encounters
- clarity_patient_ustc (5 cols) — patient demographics
- clarity_lab_results_ustc (17 cols) — labs
- clarity_order_med_ustc (26 cols) — medications
- clarity_pat_enc_flw_ustc (21 cols) — flowsheets/vitals
- clarity_order_micro_result_ustc (13 cols) — microbiology results
- clarity_order_micro_sensitivity_ustc (15 cols) — antibiotic sensitivities

### TRAUMA_REGISTRY (27 tables)
- trauma_registry_demo_scores (86 cols) — demographics + trauma scores
- trauma_registry_pta_vitals (15 cols) — prehospital vitals
- trauma_registry_labs_20XX (12 cols × 15 years) — labs by year
- trauma_registry_blood (248 cols) — blood products
- trauma_registry_operations (12 cols) — surgical procedures
- trauma_registry_non_op_procs (9 cols) — non-operative procedures
- trauma_registry_injury_aisv05 (9 cols) — AIS version 2005
- trauma_registry_injury_aisv90 (7 cols) — AIS version 1990
- trauma_registry_complications (12 cols) — complications
- trauma_registry_ems_procs_all (14 cols) — EMS procedures
- trauma_registry_ems_times_all (9 cols) — EMS timestamps
- trauma_registry_insurance (6 cols) — insurance info
- trauma_registry_pmh (10 cols) — past medical history
