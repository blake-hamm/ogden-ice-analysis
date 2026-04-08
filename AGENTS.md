# Agent Instructions

## Development Environment

**CRITICAL: Run `nix develop` BEFORE any commands.**

This project uses Nix for reproducible development environments. The dev shell provides Python, ruff, uv, and all necessary tools.

## Dependencies

**ALWAYS** use `uv add` to add new packages. Never edit `pyproject.toml` directly.

```bash
# Add a runtime dependency
uv add package-name

# Add a dev dependency
uv add --dev package-name
```

## Quality Gates

Every change must pass these two checks:

```bash
# Lint + format (run separately to avoid shell escaping issues)
nix develop -c ruff check --fix
nix develop -c ruff format

# Tests + coverage (must be >=80%)
.venv/bin/pytest --cov=src --cov-report=term-missing
```

Ruff configuration is in `pyproject.toml`. Run these commands after every code change to ensure quality.

**Note:** The chained commands (`&&`) don't work properly in the nix develop shell due to how arguments are passed. Run each command separately or use the `.venv/bin/` paths for pytest commands.

## Code Style

### Docstrings

**ALWAYS** use simple docstrings without Args/Returns/Raises sections. The type signatures are visible in the code - repeating them in docstrings is redundant noise.

**Correct:**
```python
def process_data(
    data: list[dict[str, Any]],
    *,
    validate: bool = True,
) -> DataFrame:
    """Process raw data into a structured format.

    Validates and transforms incoming data records into a normalized
    DataFrame for downstream analysis.

    Examples:
        >>> df = process_data(records)
        >>> df.filter(pl.col("status") == "active")

        >>> df = process_data(records, validate=False)

    """
```

**WRONG - Do NOT do this:**
```python
def process_data(
    data: list[dict[str, Any]],
    *,
    validate: bool = True,
) -> DataFrame:
    """Process raw data into a structured format.

    Validates and transforms incoming data records into a normalized
    DataFrame for downstream analysis.

    Args:
        data: List of dictionary records to process.
        validate: Whether to validate data before processing (default True).

    Returns:
        DataFrame containing the processed data.

    Raises:
        ValueError: If validation fails and validate=True.

    Examples:
        >>> df = process_data(records)
        >>> df.filter(pl.col("status") == "active")

    """
```

### Error Handling Philosophy

**Prefer simplicity over defensiveness.** Let exceptions bubble up rather than wrapping everything in try-catch blocks with custom exception classes.

**Handle exceptions only at boundaries.** Catch exceptions at the edge of interfaces (API endpoints, CLI commands, file I/O boundaries), not hidden deep in internal functions. Internal code should let exceptions flow naturally to the boundary.

**Avoid defensive if statements.** Don't check if something exists before using it - just use it and let it fail if it's not there. Python's "easier to ask forgiveness than permission" (EAFP) style is preferred over "look before you leap" (LBYL).

**Correct:**
```python
def fetch_data(url: str) -> dict:
    """Fetch data from URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def process_file(path: Path) -> str:
    """Process a file."""
    with path.open() as f:  # Let it fail if file doesn't exist
        return f.read()
```

**WRONG - Do NOT do this:**
```python
def fetch_data(url: str) -> dict:
    """Fetch data from URL."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout as e:
        raise DataFetchError(f"Request timed out: {e}") from e
    except requests.exceptions.HTTPError as e:
        raise DataFetchError(f"HTTP error: {e}") from e
    except ValueError as e:
        raise DataFetchError(f"Invalid JSON: {e}") from e

def process_file(path: Path) -> str | None:
    """Process a file."""
    if not path.exists():  # Defensive check - unnecessary!
        return None
    if not path.is_file():  # Another defensive check!
        return None
    with path.open() as f:
        return f.read()
```


### Zen of Python

*Always do your best to follow pep 20:*

```
Beautiful is better than ugly.
Explicit is better than implicit.
Simple is better than complex.
Complex is better than complicated.
Flat is better than nested.
Sparse is better than dense.
Readability counts.
Special cases aren't special enough to break the rules.
Although practicality beats purity.
Errors should never pass silently.
Unless explicitly silenced.
In the face of ambiguity, refuse the temptation to guess.
There should be one-- and preferably only one --obvious way to do it.
Although that way may not be obvious at first unless you're Dutch.
Now is better than never.
Although never is often better than *right* now.
If the implementation is hard to explain, it's a bad idea.
If the implementation is easy to explain, it may be a good idea.
Namespaces are one honking great idea -- let's do more of those!
```
