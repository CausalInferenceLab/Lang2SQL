WITH StepScores AS (
    SELECT
        m.name,
        m.version,
        m.step,
        m.L1_model,
        s.model,
        s.test_score
    FROM model m
    JOIN model_score s ON m.name = s.name AND m.version = s.version AND m.step = s.step
),
MaxScores AS (
    SELECT
        name,
        version,
        step,
        MAX(CASE WHEN model != 'Stack' THEN test_score ELSE NULL END) AS max_non_stack_test_score,
        MAX(CASE WHEN model = 'Stack' THEN test_score ELSE NULL END) AS stack_test_score
    FROM StepScores
    GROUP BY name, version, step
),
ModelStatus AS (
    SELECT
        name,
        version,
        CASE
            WHEN MAX(CASE WHEN max_non_stack_test_score < stack_test_score THEN 1 ELSE 0 END) = 1 THEN 'strong'
            WHEN MAX(CASE WHEN max_non_stack_test_score = stack_test_score THEN 1 ELSE 0 END) = 1 THEN 'soft'
            ELSE NULL
        END AS status
    FROM MaxScores
    GROUP BY name, version
),
L1ModelStatus AS (
    SELECT
        s.name,
        s.version,
        s.L1_model,
        ms.status
    FROM StepScores s
    JOIN ModelStatus ms ON s.name = ms.name AND s.version = ms.version
    WHERE ms.status IS NOT NULL
),
CountByL1ModelStatus AS (
    SELECT
        status,
        L1_model,
        COUNT(*) AS cnt
    FROM L1ModelStatus
    GROUP BY status, L1_model
),
MaxCountByStatus AS (
    SELECT
        status,
        MAX(cnt) AS max_cnt
    FROM CountByL1ModelStatus
    GROUP BY status
)
SELECT
    c.status,
    c.L1_model,
    c.cnt
FROM CountByL1ModelStatus c
JOIN MaxCountByStatus m
ON c.status = m.status AND c.cnt = m.max_cnt
ORDER BY c.status DESC;
