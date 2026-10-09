# Development Instructions

## 1. Project Overview

`multi-agent-cx-intelligence` is a Multi-Agent Customer Experience Intelligence system
for turning customer experience data into actionable insights. Its modular design supports
collecting, storing, preparing, and analyzing data, with agent-based workflows planned for
future stages.

The development environment uses Python 3.13, VS Code, Windows, and Docker Compose.
The current focus is Stage I: Data Engineering.

The technology stack includes Python, PostgreSQL, SeaweedFS (S3-compatible storage),
SQLAlchemy, Alembic, Pandas, Pydantic, Pytest, and Ruff. Qdrant and LangGraph are planned
for future stages and must not be implemented until explicitly requested.

## 2. Project Architecture

Use the modular source layout under `src/cx_multiagent/`:

- `config/`: application settings and environment-based configuration.
- `storage/`: S3-compatible object storage integration with SeaweedFS.
- `database/`: PostgreSQL connections, SQLAlchemy models, and Alembic migration support.
- `ingestion/`: collection and loading of customer experience data.
- `preprocessing/`: data cleaning, normalization, and validation.
- `chunking/`: splitting prepared data into units for downstream processing.
- `generators/`: synthetic data generation for development and testing.

Keep responsibilities separate and expose functionality through clear module interfaces.
Place automated tests in the root-level `tests/` directory. Use `pyproject.toml` for project
metadata, package discovery, and dependencies. Package directories currently provide the
foundation for implementation; their intended responsibilities do not imply completed features.

## 3. Coding Standards

- Target Python 3.13.
- Use type hints for function parameters, return values, and other declarations where useful.
- Maintain a modular architecture with focused modules and functions.
- Follow PEP 8.
- Limit line length to 100 characters.
- Use meaningful function and variable names.

## 4. Testing Requirements

- Use Pytest for automated tests.
- Write tests for new functionality.
- Use Ruff for linting and code quality checks.
- Run relevant tests after code changes.
- Report the checks run and their outcomes accurately.

## 5. Security

- Never hardcode credentials.
- Use environment variables for credentials and environment-specific settings.
- Never commit secrets.
- Do not access files outside the project workspace.

## 6. Development Workflow

- Implement only the explicitly requested task.
- Do not implement future stages prematurely.
- Preserve existing functionality.
- Do not modify unrelated files.
- Explain changes after implementation.
- Report errors and failed tests.
- Never claim tests passed without running them.
- Do not execute Git commit or push commands unless explicitly requested.

## 7. Infrastructure Principles

- Use Docker Compose for infrastructure services.
- Prefer local development.
- Avoid paid cloud services.
- Keep infrastructure configuration reproducible.
