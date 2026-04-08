# Agent Instructions

## Development Environment

**CRITICAL: Run `nix develop` BEFORE any commands.**

This project uses Nix for reproducible development environments. The dev shell provides Python, ruff, uv, and all necessary tools.

## Quality Gates

Every change must pass these two checks:

```bash
nix develop -c 'ruff check --fix && ruff format'   # Lint + format
nix develop -c 'coverage run && coverage report'  # Tests + coverage (must be >=80%)
```

Ruff configuration is in `pyproject.toml`. Run these commands after every code change to ensure quality.
