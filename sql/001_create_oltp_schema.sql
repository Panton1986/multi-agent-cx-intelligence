-- Initial OLTP schema for cx_intelligence on PostgreSQL 16.
-- Repeated execution preserves existing tables; it does not reconcile schema drift.
BEGIN;

CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Source tables retain original data. Airflow masks PII after extraction.
CREATE TABLE IF NOT EXISTS source.customers (
    customer_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL,
    country_code TEXT NOT NULL,
    preferred_language TEXT NOT NULL,
    customer_segment TEXT NOT NULL,
    account_status TEXT NOT NULL,
    CONSTRAINT customers_id_check CHECK (customer_id ~ '^cus_.+'),
    CONSTRAINT customers_name_check CHECK (btrim(full_name) <> ''),
    CONSTRAINT customers_email_check CHECK (btrim(email) <> ''),
    CONSTRAINT customers_country_check CHECK (country_code ~ '^[A-Z]{2}$'),
    CONSTRAINT customers_language_check CHECK (btrim(preferred_language) <> ''),
    CONSTRAINT customers_segment_check
        CHECK (customer_segment IN ('new', 'regular', 'premium')),
    CONSTRAINT customers_status_check
        CHECK (account_status IN ('active', 'inactive', 'closed'))
);

CREATE TABLE IF NOT EXISTS source.product_releases (
    release_id TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    platform TEXT NOT NULL,
    released_at TIMESTAMPTZ NOT NULL,
    release_notes TEXT NOT NULL,
    known_issues JSONB NOT NULL,
    rollout_percentage INTEGER NOT NULL,
    CONSTRAINT product_releases_platform_version_key UNIQUE (platform, version),
    CONSTRAINT product_releases_id_check CHECK (release_id ~ '^rel_.+'),
    CONSTRAINT product_releases_version_check CHECK (btrim(version) <> ''),
    CONSTRAINT product_releases_platform_check CHECK (platform IN ('android', 'ios', 'web')),
    -- Python validates semantic versions and each known-issues array element.
    CONSTRAINT product_releases_issues_check CHECK (jsonb_typeof(known_issues) = 'array'),
    CONSTRAINT product_releases_rollout_check CHECK (rollout_percentage BETWEEN 1 AND 100)
);

CREATE TABLE IF NOT EXISTS source.customer_journey_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    session_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    error_code TEXT,
    release_id TEXT,
    -- External SeaweedFS relationship: intentionally no PostgreSQL foreign key.
    ticket_id TEXT,
    properties JSONB,
    CONSTRAINT journey_events_customer_fk FOREIGN KEY (customer_id)
        REFERENCES source.customers (customer_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT journey_events_release_fk FOREIGN KEY (release_id)
        REFERENCES source.product_releases (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT journey_events_id_check CHECK (event_id ~ '^evt_.+'),
    CONSTRAINT journey_events_customer_check CHECK (btrim(customer_id) <> ''),
    CONSTRAINT journey_events_session_check CHECK (btrim(session_id) <> ''),
    CONSTRAINT journey_events_device_check CHECK (btrim(device_id) <> ''),
    CONSTRAINT journey_events_release_check CHECK (release_id IS NULL OR btrim(release_id) <> ''),
    CONSTRAINT journey_events_ticket_check CHECK (ticket_id IS NULL OR btrim(ticket_id) <> ''),
    CONSTRAINT journey_events_channel_check CHECK (channel IN ('web', 'mobile')),
    CONSTRAINT journey_events_type_check CHECK (event_type IN (
        'session_started', 'login_attempt', 'product_view', 'checkout_started',
        'payment_attempt', 'app_crash', 'support_contact', 'journey_completed'
    )),
    CONSTRAINT journey_events_outcome_check CHECK (outcome IN ('success', 'failure')),
    CONSTRAINT journey_events_mobile_release_check
        CHECK (channel <> 'mobile' OR release_id IS NOT NULL),
    -- Explicit null tests prevent CHECK's null semantics from admitting missing failure codes.
    CONSTRAINT journey_events_error_check CHECK (
        (outcome = 'failure' AND error_code IS NOT NULL AND btrim(error_code) <> '')
        OR (outcome = 'success' AND error_code IS NULL)
    ),
    CONSTRAINT journey_events_crash_check
        CHECK (event_type <> 'app_crash' OR outcome = 'failure'),
    CONSTRAINT journey_events_completion_check
        CHECK (event_type <> 'journey_completed' OR outcome = 'success'),
    CONSTRAINT journey_events_properties_check
        CHECK (properties IS NULL OR jsonb_typeof(properties) = 'object')
);

CREATE TABLE IF NOT EXISTS source.mobile_application_logs (
    log_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    session_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    os_version TEXT NOT NULL,
    log_level TEXT NOT NULL,
    log_type TEXT NOT NULL,
    message TEXT NOT NULL,
    event_id TEXT,
    error_code TEXT,
    stack_trace TEXT,
    CONSTRAINT mobile_logs_customer_fk FOREIGN KEY (customer_id)
        REFERENCES source.customers (customer_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT mobile_logs_release_fk FOREIGN KEY (release_id)
        REFERENCES source.product_releases (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT mobile_logs_event_fk FOREIGN KEY (event_id)
        REFERENCES source.customer_journey_events (event_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT mobile_logs_id_check CHECK (log_id ~ '^log_.+'),
    CONSTRAINT mobile_logs_customer_check CHECK (btrim(customer_id) <> ''),
    CONSTRAINT mobile_logs_session_check CHECK (btrim(session_id) <> ''),
    CONSTRAINT mobile_logs_device_check CHECK (btrim(device_id) <> ''),
    CONSTRAINT mobile_logs_release_check CHECK (btrim(release_id) <> ''),
    CONSTRAINT mobile_logs_event_check CHECK (event_id IS NULL OR btrim(event_id) <> ''),
    CONSTRAINT mobile_logs_platform_check CHECK (platform IN ('android', 'ios')),
    CONSTRAINT mobile_logs_os_check CHECK (btrim(os_version) <> ''),
    CONSTRAINT mobile_logs_level_check CHECK (log_level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR')),
    CONSTRAINT mobile_logs_type_check
        CHECK (log_type IN ('payment_failure', 'app_crash', 'login_problem', 'activity')),
    CONSTRAINT mobile_logs_error_check CHECK (
        log_type = 'activity' OR (error_code IS NOT NULL AND btrim(error_code) <> '')
    )
);

CREATE TABLE IF NOT EXISTS source.survey_responses (
    survey_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    survey_type TEXT NOT NULL,
    csat_score INTEGER NOT NULL,
    comment TEXT,
    -- Tickets and calls are primary S3 records; Python validates these references.
    ticket_id TEXT,
    call_id TEXT,
    event_id TEXT,
    session_id TEXT,
    device_id TEXT,
    CONSTRAINT survey_responses_customer_fk FOREIGN KEY (customer_id)
        REFERENCES source.customers (customer_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT survey_responses_event_fk FOREIGN KEY (event_id)
        REFERENCES source.customer_journey_events (event_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT survey_responses_id_check CHECK (survey_id ~ '^sur_.+'),
    CONSTRAINT survey_responses_customer_check CHECK (btrim(customer_id) <> ''),
    CONSTRAINT survey_responses_ticket_check CHECK (ticket_id IS NULL OR btrim(ticket_id) <> ''),
    CONSTRAINT survey_responses_call_check CHECK (call_id IS NULL OR btrim(call_id) <> ''),
    CONSTRAINT survey_responses_event_check CHECK (event_id IS NULL OR btrim(event_id) <> ''),
    CONSTRAINT survey_responses_session_check
        CHECK (session_id IS NULL OR btrim(session_id) <> ''),
    CONSTRAINT survey_responses_device_check CHECK (device_id IS NULL OR btrim(device_id) <> ''),
    CONSTRAINT survey_responses_type_check
        CHECK (survey_type IN ('post_support', 'post_journey', 'general')),
    CONSTRAINT survey_responses_csat_check CHECK (csat_score BETWEEN 1 AND 5),
    CONSTRAINT survey_responses_support_check
        CHECK (survey_type <> 'post_support' OR ticket_id IS NOT NULL OR call_id IS NOT NULL),
    CONSTRAINT survey_responses_journey_check
        CHECK (survey_type <> 'post_journey' OR event_id IS NOT NULL),
    CONSTRAINT survey_responses_session_device_check
        CHECK (session_id IS NULL OR device_id IS NOT NULL)
);

-- Manifest membership stays outside business payloads; batch UUIDs are producer-supplied.
CREATE TABLE IF NOT EXISTS pipeline.ingestion_batches (
    batch_id UUID PRIMARY KEY,
    producer_key TEXT NOT NULL UNIQUE,
    batch_kind TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    processing_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'loading',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ready_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    source_manifest JSONB NOT NULL DEFAULT '{}'::JSONB,
    output_manifest JSONB,
    claim_token UUID,
    airflow_run_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    failed_stage TEXT,
    error_summary TEXT,
    CONSTRAINT ingestion_batches_producer_check CHECK (btrim(producer_key) <> ''),
    CONSTRAINT ingestion_batches_schema_version_check CHECK (btrim(schema_version) <> ''),
    CONSTRAINT ingestion_batches_processing_version_check CHECK (btrim(processing_version) <> ''),
    CONSTRAINT ingestion_batches_kind_check CHECK (batch_kind IN ('initial', 'incremental')),
    CONSTRAINT ingestion_batches_status_check
        CHECK (status IN ('loading', 'ready', 'processing', 'completed', 'failed')),
    CONSTRAINT ingestion_batches_attempt_check CHECK (attempt_count >= 0),
    CONSTRAINT ingestion_batches_source_manifest_check
        CHECK (jsonb_typeof(source_manifest) = 'object'),
    CONSTRAINT ingestion_batches_output_manifest_check
        CHECK (output_manifest IS NULL OR jsonb_typeof(output_manifest) = 'object'),
    CONSTRAINT ingestion_batches_ready_check
        CHECK (status NOT IN ('ready', 'processing', 'completed') OR ready_at IS NOT NULL),
    CONSTRAINT ingestion_batches_processing_check CHECK (
        status <> 'processing' OR (
            started_at IS NOT NULL AND claim_token IS NOT NULL
            AND airflow_run_id IS NOT NULL AND lease_expires_at IS NOT NULL
            AND lease_expires_at > started_at
        )
    ),
    CONSTRAINT ingestion_batches_completed_check CHECK (
        status <> 'completed' OR (
            started_at IS NOT NULL AND completed_at IS NOT NULL AND output_manifest IS NOT NULL
        )
    ),
    CONSTRAINT ingestion_batches_ready_time_check
        CHECK (ready_at IS NULL OR ready_at >= created_at),
    CONSTRAINT ingestion_batches_started_time_check
        CHECK (started_at IS NULL OR ready_at IS NULL OR started_at >= ready_at),
    CONSTRAINT ingestion_batches_completed_time_check
        CHECK (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at)
    -- State transitions, ownership tokens, and frozen manifests require future application logic.
);

-- PK and UNIQUE constraints already index their columns; do not duplicate those indexes.
CREATE INDEX IF NOT EXISTS customers_created_at_idx ON source.customers (created_at);
CREATE INDEX IF NOT EXISTS product_releases_released_at_idx
    ON source.product_releases (released_at);

CREATE INDEX IF NOT EXISTS journey_events_customer_time_idx
    ON source.customer_journey_events (customer_id, occurred_at);
CREATE INDEX IF NOT EXISTS journey_events_session_time_idx
    ON source.customer_journey_events (session_id, occurred_at);
CREATE INDEX IF NOT EXISTS journey_events_release_idx
    ON source.customer_journey_events (release_id);
CREATE INDEX IF NOT EXISTS journey_events_occurred_at_idx
    ON source.customer_journey_events (occurred_at);

CREATE INDEX IF NOT EXISTS mobile_logs_customer_time_idx
    ON source.mobile_application_logs (customer_id, occurred_at);
CREATE INDEX IF NOT EXISTS mobile_logs_session_time_idx
    ON source.mobile_application_logs (session_id, occurred_at);
CREATE INDEX IF NOT EXISTS mobile_logs_release_idx
    ON source.mobile_application_logs (release_id);
CREATE INDEX IF NOT EXISTS mobile_logs_event_idx
    ON source.mobile_application_logs (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS mobile_logs_occurred_at_idx
    ON source.mobile_application_logs (occurred_at);

CREATE INDEX IF NOT EXISTS survey_responses_customer_time_idx
    ON source.survey_responses (customer_id, submitted_at);
CREATE INDEX IF NOT EXISTS survey_responses_session_time_idx
    ON source.survey_responses (session_id, submitted_at) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS survey_responses_event_idx
    ON source.survey_responses (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS survey_responses_submitted_at_idx
    ON source.survey_responses (submitted_at);

-- Partial indexes support future ready-batch polling and expired-lease discovery.
CREATE INDEX IF NOT EXISTS ingestion_batches_ready_idx
    ON pipeline.ingestion_batches (ready_at, created_at) WHERE status = 'ready';
CREATE INDEX IF NOT EXISTS ingestion_batches_lease_idx
    ON pipeline.ingestion_batches (lease_expires_at) WHERE status = 'processing';

COMMIT;
