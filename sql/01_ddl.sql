-- =============================================================
-- Thammasat Workshop — DDL
-- Run once to initialise the database schema
-- =============================================================

-- ── Schema ──────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS workshop;

-- ── 1. Batch Audit Log ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS workshop.batch_audit (
    id               SERIAL PRIMARY KEY,
    run_id           TEXT        NOT NULL,
    business_date    DATE        NOT NULL,
    start_time       TIMESTAMPTZ NOT NULL,
    end_time         TIMESTAMPTZ,
    status           TEXT,                       -- pending | success | failed
    input_file_name  TEXT,
    sheet_name       TEXT,
    input_file_checksum TEXT,
    source_count     INTEGER,
    loaded_count     INTEGER,
    rejected_count   INTEGER,
    error_message    TEXT
);

-- ── 2. Raw Layer ────────────────────────────────────────────
-- Verbatim copy of every source row; never modified after insert
CREATE TABLE IF NOT EXISTS workshop.raw_workshop_data (
    id                    SERIAL PRIMARY KEY,
    -- batch metadata
    run_id                TEXT,
    business_date         DATE,
    input_file_name       TEXT,
    input_file_checksum   TEXT,
    load_timestamp        TIMESTAMPTZ DEFAULT NOW(),
    -- all source columns stored as TEXT to preserve original values
    record_type           TEXT,
    entity_id             TEXT,
    snapshot_date         TEXT,
    student_no            TEXT,
    citizen_id            TEXT,
    email                 TEXT,
    mobile                TEXT,
    student_name          TEXT,
    level                 TEXT,
    faculty_or_school     TEXT,
    program_id            TEXT,
    program_name          TEXT,
    discipline_cluster    TEXT,
    campus                TEXT,
    year_of_study         TEXT,
    admission_channel     TEXT,
    status                TEXT,
    gpa                   TEXT,
    credit_earned         TEXT,
    is_international      TEXT,
    fact_category         TEXT,
    metric_or_item        TEXT,
    value                 TEXT,
    unit                  TEXT,
    year_or_as_of         TEXT,
    doc_title             TEXT,
    doc_section           TEXT,
    doc_text              TEXT,
    doc_type              TEXT,
    batch_date            TEXT,
    source_system         TEXT,
    protection_classification TEXT,
    required_action       TEXT,
    task_hint             TEXT,
    source_url            TEXT,
    source_row_no         TEXT,
    behavior_profile      TEXT,
    teamwork_style        TEXT,
    learning_preference   TEXT,
    career_interest       TEXT,
    expected_salary_thb   TEXT,
    salary_expectation_note TEXT,
    internship_interest   TEXT,
    mock_interview_note   TEXT,
    rag_document_title    TEXT,
    rag_document_text     TEXT,
    rag_keywords          TEXT,
    rag_sample_question   TEXT,
    rag_expected_answer_hint TEXT
);

-- ── 3. Staging Layer ────────────────────────────────────────
-- Type-cast and standardised; one row per source row per run
CREATE TABLE IF NOT EXISTS workshop.stg_student_snapshot (
    id                    SERIAL PRIMARY KEY,
    run_id                TEXT,
    business_date         DATE,
    input_file_name       TEXT,
    input_file_checksum   TEXT,
    load_timestamp        TIMESTAMPTZ DEFAULT NOW(),
    record_status         TEXT DEFAULT 'staged',
    -- typed columns
    record_type           TEXT,
    entity_id             TEXT,
    snapshot_date         DATE,
    student_no            TEXT,
    citizen_id            TEXT,
    email                 TEXT,
    mobile                TEXT,
    student_name          TEXT,
    level                 TEXT,
    faculty_or_school     TEXT,
    program_id            TEXT,
    program_name          TEXT,
    discipline_cluster    TEXT,
    campus                TEXT,
    year_of_study         INTEGER,
    admission_channel     TEXT,
    status                TEXT,
    gpa                   NUMERIC(4,2),
    credit_earned         INTEGER,
    is_international      BOOLEAN,
    career_interest       TEXT,
    expected_salary_thb   NUMERIC(12,2),
    internship_interest   TEXT,
    behavior_profile      TEXT,
    teamwork_style        TEXT,
    learning_preference   TEXT,
    source_row_no         INTEGER,
    source_url            TEXT,
    rag_document_title    TEXT,
    rag_document_text     TEXT,
    rag_keywords          TEXT,
    rag_sample_question   TEXT,
    rag_expected_answer_hint TEXT
);

-- ── 4. Trusted Layer ────────────────────────────────────────
-- Deduplicated; upserted by (entity_id, snapshot_date)
CREATE TABLE IF NOT EXISTS workshop.trusted_student_snapshot (
    entity_id             TEXT        NOT NULL,
    snapshot_date         DATE        NOT NULL,
    student_no            TEXT,
    student_name          TEXT,
    level                 TEXT,
    faculty_or_school     TEXT,
    program_id            TEXT,
    program_name          TEXT,
    discipline_cluster    TEXT,
    campus                TEXT,
    year_of_study         INTEGER,
    admission_channel     TEXT,
    status                TEXT,
    gpa                   NUMERIC(4,2),
    credit_earned         INTEGER,
    is_international      BOOLEAN,
    career_interest       TEXT,
    expected_salary_thb   NUMERIC(12,2),
    internship_interest   TEXT,
    behavior_profile      TEXT,
    teamwork_style        TEXT,
    learning_preference   TEXT,
    source_row_no         INTEGER,
    source_url            TEXT,
    -- technical metadata
    source_file_name      TEXT,
    input_file_checksum   TEXT,
    business_date         DATE,
    run_id                TEXT,
    load_timestamp        TIMESTAMPTZ DEFAULT NOW(),
    record_status         TEXT DEFAULT 'trusted',
    PRIMARY KEY (entity_id, snapshot_date)
);

-- ── 5. RAG Document Chunk ───────────────────────────────────
CREATE TABLE IF NOT EXISTS workshop.rag_excel_document_chunk (
    chunk_id              TEXT        PRIMARY KEY,
    source_row_no         INTEGER,
    entity_id             TEXT,
    student_no            TEXT,
    rag_document_title    TEXT,
    rag_document_text     TEXT,
    rag_keywords          TEXT,
    rag_sample_question   TEXT,
    rag_expected_answer_hint TEXT,
    source_url            TEXT,
    business_date         DATE,
    run_id                TEXT,
    load_timestamp        TIMESTAMPTZ DEFAULT NOW()
);

-- ── 6. Analytics Summary ────────────────────────────────────
CREATE TABLE IF NOT EXISTS workshop.analytics_student_summary (
    id                    SERIAL PRIMARY KEY,
    business_date         DATE,
    run_id                TEXT,
    generated_at          TIMESTAMPTZ DEFAULT NOW(),
    total_students        INTEGER,
    active_count          INTEGER,
    leave_count           INTEGER,
    exchange_count        INTEGER,
    withdrawn_count       INTEGER,
    graduated_count       INTEGER,
    rangsit_count         INTEGER,
    tha_phra_chan_count    INTEGER,
    undergraduate_count   INTEGER,
    postgraduate_count    INTEGER,
    international_count   INTEGER,
    gpa_min               NUMERIC(4,2),
    gpa_max               NUMERIC(4,2),
    gpa_avg               NUMERIC(5,2),
    gpa_sum               NUMERIC(10,2),
    credit_min            INTEGER,
    credit_max            INTEGER,
    credit_avg            NUMERIC(6,1),
    credit_sum            INTEGER,
    salary_min            NUMERIC(12,2),
    salary_max            NUMERIC(12,2),
    salary_avg            NUMERIC(12,2)
);

-- ── Indexes ─────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_trusted_status       ON workshop.trusted_student_snapshot(status);
CREATE INDEX IF NOT EXISTS idx_trusted_campus       ON workshop.trusted_student_snapshot(campus);
CREATE INDEX IF NOT EXISTS idx_trusted_career       ON workshop.trusted_student_snapshot(career_interest);
CREATE INDEX IF NOT EXISTS idx_trusted_cluster      ON workshop.trusted_student_snapshot(discipline_cluster);
CREATE INDEX IF NOT EXISTS idx_rag_entity           ON workshop.rag_excel_document_chunk(entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_date           ON workshop.batch_audit(business_date);
