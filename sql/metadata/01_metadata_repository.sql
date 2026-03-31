-- Metadata repository DDL

-- Table: report_registry
CREATE TABLE report_registry (
    report_id SERIAL PRIMARY KEY, -- Unique identifier for each report
    report_name VARCHAR(255) NOT NULL, -- Name of the report
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp of creation
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Timestamp of last update
);

-- Table: report_dependencies
CREATE TABLE report_dependencies (
    dependency_id SERIAL PRIMARY KEY, -- Unique identifier for the dependency
    report_id INT REFERENCES report_registry(report_id), -- Associated report
    dependency_report_id INT REFERENCES report_registry(report_id), -- Report dependency
    UNIQUE(report_id, dependency_report_id) -- Composite unique constraint to prevent duplicate entries
);

-- Table: report_snapshots
CREATE TABLE report_snapshots (
    snapshot_id SERIAL PRIMARY KEY, -- Unique identifier for each snapshot
    report_id INT REFERENCES report_registry(report_id), -- Associated report
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp of snapshot
    data JSONB, -- Snapshot data in JSONB format
    UNIQUE(report_id, snapshot_timestamp) -- Unique constraint on report and timestamp
);

-- Table: column_mappings
CREATE TABLE column_mappings (
    mapping_id SERIAL PRIMARY KEY, -- Unique identifier for the mapping
    report_id INT REFERENCES report_registry(report_id), -- Associated report
    source_column VARCHAR(255) NOT NULL, -- Source column name
    target_column VARCHAR(255) NOT NULL, -- Target column name
    UNIQUE(report_id, source_column, target_column) -- Unique constraint to prevent duplicates
);

-- Table: category_mappings
CREATE TABLE category_mappings (
    category_id SERIAL PRIMARY KEY, -- Unique identifier for the category
    report_id INT REFERENCES report_registry(report_id), -- Associated report
    category_name VARCHAR(255) NOT NULL, -- Name of the category
    UNIQUE(report_id, category_name) -- Unique constraint to prevent duplicate categories
);

-- Table: report_audit_log
CREATE TABLE report_audit_log (
    audit_id SERIAL PRIMARY KEY, -- Unique identifier for the audit log entry
    report_id INT REFERENCES report_registry(report_id), -- Associated report
    change_description TEXT NOT NULL, -- Description of the change
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp of the change
);

-- Table: documentation_embeddings
CREATE TABLE documentation_embeddings (
    embedding_id SERIAL PRIMARY KEY, -- Unique identifier for each embedding
    report_id INT REFERENCES report_registry(report_id), -- Associated report
    embedding_data BYTEA, -- Byte array for embedding data
    UNIQUE(report_id, embedding_id) -- Unique constraint on report and embedding
);