# Distress Signal

Automated UK property distress detection pipeline. Monitors The Gazette for newly insolvent UK companies, enriches records with Companies House and Land Registry (CCOD) data, and delivers actionable property leads via email.

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL database (Neon, Supabase, or local)

### Installation

```bash
# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

### Database Setup

1. Create a PostgreSQL database (e.g., on [Neon](https://neon.tech))

2. Apply the schema:
```bash
psql $DATABASE_URL -f src/db/schema.sql
```

Or run via Python:
```python
import psycopg
schema = open('src/db/schema.sql').read()
with psycopg.connect(DATABASE_URL) as conn:
    conn.execute(schema)
    conn.commit()
```

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `COMPANIES_HOUSE_API_KEY` | Yes | [Companies House API](https://developer.company-information.service.gov.uk/) |
| `ANTHROPIC_API_KEY` | Yes | [Anthropic Console](https://console.anthropic.com/) |
| `RESEND_API_KEY` | Yes | [Resend](https://resend.com/) for email delivery |
| `GMAIL_CREDENTIALS_JSON` | Yes | OAuth 2.0 credentials from Google Cloud Console |
| `CLIENT_EMAIL` | Yes | Recipient email for enriched reports |
| `GMAIL_TOKEN_JSON` | No | Auto-populated after first OAuth flow |
| `LLM_MODEL` | No | Defaults to `claude-sonnet-4-5` |
| `CCOD_GOV_UK_CREDENTIALS` | No | For CCOD data sync |

### Verify Setup

```bash
# Run tests
pytest tests/ -v

# Check database connection
python -c "
from src.db.connection import check_connectivity
print('Database connected:', check_connectivity())
"
```

## Usage

### Services

```bash
# Sync CCOD property data (run periodically)
ccod-sync

# Start email watcher (polls Gmail for Gazette emails)
email-watcher

# Run enrichment on records
enrichment
```

## Development

```bash
# Run tests
pytest tests/ -v

# Lint
ruff check .

# Type check
mypy src/
```

## Architecture

See [.claude/project-context.md](.claude/project-context.md) for detailed architecture documentation.
