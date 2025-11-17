DELETE FROM rule_library;

INSERT INTO rule_library (
  rule_id, rule_name, rule_description, rule_category,
  rule_type, rule_severity, rule_sql, enabled
) VALUES
-- Missing values
('RULE_MISSING_001', 'Missingness Check',
 'Checks if a variable is NULL.',
 'completeness', 'SQL', 'warning',
 'SELECT COUNT(*) AS failing_rows FROM {table} WHERE {variable} IS NULL;',
 TRUE),

-- Allowed values
('RULE_ALLOWED_001', 'Allowed Values Check',
 'Values must fall in an approved list.',
 'validity', 'SQL', 'error',
 'SELECT COUNT(*) AS failing_rows FROM {table} WHERE {variable} NOT IN ({allowed});',
 TRUE),

-- Numeric range
('RULE_RANGE_001', 'Range Check',
 'Numeric values must fall between min and max.',
 'validity', 'SQL', 'error',
 'SELECT COUNT(*) AS failing_rows FROM {table} WHERE ({variable} < {min} OR {variable} > {max});',
 TRUE),

-- Uniqueness
('RULE_ID_001', 'Uniqueness Check',
 'ID variable must have no duplicates.',
 'uniqueness', 'SQL', 'error',
 'SELECT COUNT(*) AS failing_rows FROM (SELECT {variable} FROM {table} GROUP BY {variable} HAVING COUNT(*) > 1) t;',
 TRUE),

-- Date format (text regex)
('RULE_DATE_001', 'Date Format Check',
 'Value must match YYYY-MM-DD.',
 'consistency', 'SQL', 'error',
 'SELECT COUNT(*) AS failing_rows FROM {table} WHERE ({variable} IS NULL OR {variable} !~ ''^\\d{4}-\\d{2}-\\d{2}$'');',
 TRUE),

-- Future dates
('RULE_DATE_002', 'Future Date Check',
 'Date must not occur in the future.',
 'consistency', 'SQL', 'warning',
 'SELECT COUNT(*) AS failing_rows FROM {table} WHERE ({variable} ~ ''^\\d{4}-\\d{2}-\\d{2}$'' AND {variable}::timestamp > CURRENT_TIMESTAMP);',
 TRUE);
