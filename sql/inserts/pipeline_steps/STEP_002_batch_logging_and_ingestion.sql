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
    'STEP_002',
    2,
    'batch_logging_and_ingestion',
    'Logs batch, logs files, and appends raw data into lake tables.',
    'R',
    'run_step2_batch_logging',
    TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM governance.pipeline_step WHERE step_id = 'STEP_002'
);
