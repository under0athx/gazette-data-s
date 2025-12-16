-- CCOD Properties table
-- Land Registry title numbers follow pattern: 2-3 letters followed by numbers
-- e.g., DN12345, ESX123456, K123456
CREATE TABLE IF NOT EXISTS ccod_properties (
    title_number VARCHAR(20) PRIMARY KEY
        CONSTRAINT chk_title_number_format CHECK (title_number ~ '^[A-Z]{1,4}[0-9]+$'),
    property_address TEXT,
    company_name TEXT NOT NULL
        CONSTRAINT chk_company_name_not_empty CHECK (LENGTH(TRIM(company_name)) > 0),
    company_number VARCHAR(10)
        CONSTRAINT chk_company_number_format CHECK (
            company_number IS NULL OR company_number ~ '^[A-Z0-9]{6,8}$'
        ),
    tenure VARCHAR(20)
        CONSTRAINT chk_tenure_valid CHECK (
            tenure IS NULL OR tenure IN ('Freehold', 'Leasehold', 'Unknown')
        ),
    date_proprietor_added DATE
        CONSTRAINT chk_date_not_future CHECK (
            date_proprietor_added IS NULL OR date_proprietor_added <= CURRENT_DATE
        ),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ccod_company_number ON ccod_properties(company_number);
CREATE INDEX IF NOT EXISTS idx_ccod_company_name ON ccod_properties(company_name);

-- Composite unique constraint: a company can only own a title once
-- (prevents duplicate entries from data quality issues)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ccod_company_title_unique
    ON ccod_properties(company_number, title_number)
    WHERE company_number IS NOT NULL;

-- Enable trigram extension for fuzzy matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_ccod_company_name_trgm
    ON ccod_properties USING gin(company_name gin_trgm_ops);

-- Partial index for companies with properties (common filter)
CREATE INDEX IF NOT EXISTS idx_ccod_has_company_number
    ON ccod_properties(company_number)
    WHERE company_number IS NOT NULL;
