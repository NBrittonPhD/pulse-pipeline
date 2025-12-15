INSERT INTO governance.pipeline_step (
    step_id,
    step_order,
    step_name,
    step_description,
    step_type,
    code_snippet,
    enabled
)
SELECT
    'STEP_001',
    1,
    'register_source',
    'Registers or updates source metadata, creates folders, writes audit event.',
    'R',
    'run_step1_register_source',
    TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM governance.pipeline_step WHERE step_id = 'STEP_001'
);
