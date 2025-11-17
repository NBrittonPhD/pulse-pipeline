INSERT INTO rule_execution_map (
    map_id,
    rule_id,
    lake_table,
    lake_variable,
    rule_params,
    enabled
)
VALUES

-- Missingness check: sex
('MAP_001', 'RULE_MISSING_001', 'patients', 'sex',
 '{"threshold": 0.1}', TRUE),

-- Allowed values check: sex
('MAP_002', 'RULE_ALLOWED_001', 'patients', 'sex',
 '{"allowed": ["M","F","U"]}', TRUE),

-- Range check: age
('MAP_003', 'RULE_RANGE_001', 'patients', 'age',
 '{"min": 0, "max": 120}', TRUE),

-- ID uniqueness: patient_id
('MAP_004', 'RULE_ID_001', 'patients', 'patient_id',
 '{}', TRUE),

-- Date validity check: admit_date
('MAP_005', 'RULE_DATE_001', 'patients', 'admit_date',
 '{}', TRUE),

-- Future date warning: admit_date
('MAP_006', 'RULE_DATE_002', 'patients', 'admit_date',
 '{}', TRUE);
