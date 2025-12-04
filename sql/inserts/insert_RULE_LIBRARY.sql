-- =============================================================================
-- RULE_LIBRARY seed rules
-- =============================================================================

INSERT INTO governance.rule_library (
    rule_id,
    rule_name,
    rule_description,
    rule_category,
    rule_severity,
    rule_expression
)
VALUES
-- Example: Age must be >= 0
('RANGE_AGE_001', 'Age non-negative', 
 'Age must be zero or positive', 
 'range', 'error', 
 'age >= 0'),

-- Example: No missing patient IDs
('REQ_PATID_001', 'Required patient_id',
 'patient_id must not be NULL',
 'required', 'error',
 'NOT ISNULL(patient_id)'),

-- Example: Heart rate reasonable
('RANGE_HR_001', 'Heart rate reasonable',
 'HR must be between 20 and 250',
 'range', 'warning',
 'heart_rate BETWEEN 20 AND 250');