-- CCOD Properties table
CREATE TABLE IF NOT EXISTS ccod_properties (
    title_number VARCHAR(20) PRIMARY KEY,
    property_address TEXT,
    company_name TEXT NOT NULL,
    company_number VARCHAR(10),
    tenure VARCHAR(20),
    date_proprietor_added DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ccod_company_number ON ccod_properties(company_number);
CREATE INDEX IF NOT EXISTS idx_ccod_company_name ON ccod_properties(company_name);

-- Enable trigram extension for fuzzy matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_ccod_company_name_trgm ON ccod_properties USING gin(company_name gin_trgm_ops);
