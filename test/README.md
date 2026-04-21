# istSOS4 Test Suite

This README covers everything you need to know to run, write, and maintain tests in this repository — whether you're touching the codebase for the first time or adding tests to an existing module.

---

## Table of Contents

1. [Directory Structure](#directory-structure)
2. [How to Run Tests](#how-to-run-tests)
3. [How the Test Infrastructure Works](#how-the-test-infrastructure-works)
4. [Writing New Tests](#writing-new-tests)
5. [What to Keep in Mind](#what-to-keep-in-mind)
6. [Maintaining Structure and Readability](#maintaining-structure-and-readability)
7. [Common Mistakes](#common-mistakes)

---

## Directory Structure

```
istSOS4/
├── conftest.py                    # Root path bootstrap — adds project root and api/ to sys.path
├── pytest.ini                     # Root config: asyncio_mode=auto, testpaths=test
└── test/
    ├── __init__.py
    ├── conftest.py                # Re-applies path bootstrap when pytest is run from test/
    ├── pytest.ini                 # asyncio_mode=auto for test/-rooted runs
    ├── database/
    │   ├── __init__.py
    │   ├── conftest.py            # ALL shared database fixtures and helpers live here
    │   ├── test_schema.py         # Tests for istsos_schema.sql (base schema)
    │   ├── test_schema_versioning.py  # Tests for istsos_schema_versioning.sql
    │   └── test_auth_sql.py       # Tests for istsos_auth.sql (RLS, users, policies)
    └── unit/
        ├── __init__.py
        ├── test_sta2rest.py
        ├── test_utils_helper.py
        └── test_create_functions.py
```

**Rule of thumb:** database integration tests go in `test/database/`, pure Python unit tests go in `test/unit/`. Don't mix them.

---

## How to Run Tests

### Prerequisites

You need Python 3.10+, a running PostgreSQL instance (default: `localhost:5432`, superuser `postgres`), and the project dependencies installed. The database tests will create and destroy their own test databases automatically — you don't need to create anything manually.

### Setting up a virtual environment

Always use a virtual environment. Skipping this is the single most common cause of confusing import errors and dependency conflicts.

**Linux / macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r api/requirements.txt -r requirements-test.txt
```

**Windows (Command Prompt):**

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r api/requirements.txt -r requirements-test.txt
```

**Windows (PowerShell):**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r api/requirements.txt -r requirements-test.txt
```

> If PowerShell blocks the activation script with an execution policy error, run:
> `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

Verify you're using the venv's Python before running anything:

```bash
which python   # Linux/macOS — should show .venv/bin/python
```

```bash
where python   # Windows — should show .venv\Scripts\python.exe
```

### Running the full test suite

From the **repo root** (recommended):

```bash
pytest
```

OR

```bash
pytest -v      # For verbose outputs
```

This picks up `pytest.ini` at the root, which sets `testpaths = test` and `asyncio_mode = auto`.

### Running a specific subset

```bash
# All database tests
pytest test/database/ -v

# One file
pytest test/database/test_schema.py -v

# One test class
pytest test/database/test_schema.py::TestSchema -v

# One test by name
pytest test/database/test_schema.py::TestSchema::test_selflink_thing -v

# All unit tests
pytest test/unit/ -v

# Tests matching a keyword
pytest -k "selflink" -v
```

### Running from inside `test/`

This also works, because `test/conftest.py` re-applies the path bootstrap:

```bash
cd test
pytest database/test_schema.py -v
```

### Useful flags

| Flag | What it does |
|------|-------------|
| `-v` | Verbose — shows each test name |
| `-x` | Stop on first failure |
| `--tb=short` | Compact tracebacks (already set in `addopts`) |
| `--tb=long` | Full tracebacks for debugging |
| `-s` | Show `print()` output (disabled by default) |
| `--no-header` | Cleaner output for CI logs |

---

## How the Test Infrastructure Works

Understanding this section will save you hours of confusion.

### Path bootstrap (`conftest.py`)

The root `conftest.py` inserts the project root and `api/` into `sys.path`. This is why `from app.sta2rest import ...` works in tests without installing the package. `test/conftest.py` does the same thing as a fallback when pytest is invoked from inside `test/`.

**You never need to manipulate `sys.path` manually in a test file.** If you find yourself writing `sys.path.insert(...)` in a new test, stop — the conftest already handles it. (The existing `sys.path` manipulation in `test_utils_helper.py` is a legacy pattern that predates the current structure and should not be copied.)

### `asyncio_mode = auto`

Both `pytest.ini` files set `asyncio_mode = auto`. This means **all `async def` test functions are automatically treated as async tests** — you do not need `@pytest.mark.asyncio` on individual tests. Adding it manually is harmless but redundant.

### Database test lifecycle

Each database test class follows a fixed pattern:

1. **`schema` fixture** (`scope="class"`, `autouse=True`) — runs once for the whole class. It calls `recreate_database()` to drop and recreate the test DB from scratch, loads the appropriate SQL layers via the loader functions, opens a connection with `autocommit=False`, and yields that connection.

2. **`rollback` fixture** (`autouse=True`) — runs around every individual test. It yields, then calls `schema.rollback()`. This means every test starts from a clean state without touching the database at all — no re-loading SQL, no re-creating tables.

3. **Helper methods** — `_insert_commit`, `_insert_minimal_thing`, etc. are thin wrappers on the shared helpers from `test/database/conftest.py`. They exist on the class only when layer-specific behavior is needed (e.g. versioning's `_insert_commit` has no `user_id`; auth's does).

### Shared helpers in `test/database/conftest.py`

These are module-level functions, not class methods, and are imported directly:

```python
from test.database.conftest import (
    recreate_database, get_raw_conn, make_dsn,
    load_base_schema, load_auth_schema, load_versioning_schema,
    insert_minimal_thing, insert_minimal_location,
    insert_minimal_sensor, insert_minimal_observed_property,
    insert_minimal_datastream, insert_minimal_foi,
    get_id,
)
```

All `insert_minimal_*` helpers accept an optional `commit_id` keyword argument:

- **Omit it** (default `None`) for base schema tests — the column doesn't exist there.
- **Pass it** for versioning and auth tests — the column is `NOT NULL` in those layers.

This single design decision is what makes the same helper work across all three test files.

### `recreate_database()` — why it's written the way it is

PostgreSQL roles are cluster-level. If a previous test run crashed and left a role owning objects in another database, a naive `DROP ROLE` will raise `DependentObjectsStillExist`. The function prevents this by connecting to every surviving database and running `REASSIGN OWNED` + `DROP OWNED BY ... CASCADE` before dropping the role. You don't need to call this manually — each test class's `schema` fixture calls it.

---

## Writing New Tests

### Adding a test to an existing file

1. Open the relevant test class (`TestSchema`, `TestSchemaVersioning`, or `TestAuth`).
2. Write an `async def test_*` or `def test_*` method. The `schema` and `rollback` fixtures are `autouse=True` so they apply automatically.
3. Use the shared helpers for setup. Don't write raw `INSERT` SQL for entities that already have a helper.

Example — adding a test to `TestSchema`:

```python
def test_thing_name_is_stored(self, schema):
    """Inserted Thing name must round-trip through the database unchanged."""
    with schema.cursor() as cur:
        thing_id = insert_minimal_thing(cur, "my-thing")
        cur.execute(
            'SELECT "name" FROM sensorthings."Thing" WHERE id = %s',
            (thing_id,),
        )
        name = cur.fetchone()[0]
    assert name == "my-thing"
```

No teardown needed — `rollback` handles it.

### Adding a test to the versioning or auth layer

These layers require a `commit_id` on most inserts. Use the class's `_insert_commit` helper:

```python
def test_sensor_commit_id_is_stored(self, schema):
    with schema.cursor() as cur:
        commit_id = self._insert_commit(cur)
        sensor_id = insert_minimal_sensor(cur, "my-sensor", commit_id=commit_id)
        cur.execute(
            'SELECT "commit_id" FROM sensorthings."Sensor" WHERE id = %s',
            (sensor_id,),
        )
        assert cur.fetchone()[0] == commit_id
```

### Adding a new database test file

If you're testing a new SQL layer or a new schema file:

1. Create `test/database/test_<your_layer>.py`.
2. Define a `TEST_DB` constant and a `DSN` from `make_dsn(TEST_DB)`.
3. Write a class with a `schema` fixture that calls `recreate_database` and the appropriate loader(s).
4. Add a `rollback` fixture.
5. Import helpers from `test.database.conftest` — do not re-implement them.

### Adding a unit test

Unit tests in `test/unit/` don't need any database setup. Just write standard pytest classes and functions:

```python
# test/unit/test_my_module.py
from app.my_module import my_function

class TestMyFunction:
    def test_returns_expected_value(self):
        assert my_function(1, 2) == 3
```

---

## What to Keep in Mind

### The `schema` connection is shared across the whole class

The connection yielded by the `schema` fixture is class-scoped. All tests in the class share the same connection object. This is fine because `rollback` resets state after each test. But it means:

- **Never close the connection inside a test.** That will break every subsequent test in the class.
- **Never set `autocommit = True` on the `schema` connection.** If you need DDL during a test, open a separate connection with `get_raw_conn()` and close it when done.
- **Never call `schema.commit()`.** Committing bypasses the rollback and will leave data behind for the next test.

### Each test must be fully self-contained

A test must not rely on data created by another test. The `rollback` fixture enforces this for DML, but it's still your responsibility to insert everything your test needs within its own body (or via a helper called from that body).

### Cursor scope

Always open cursors inside a `with schema.cursor() as cur:` block. This ensures the cursor is closed even if the test raises. Leaving cursors open can cause silent hangs on the next `rollback()` call in some psycopg2 versions.

### `commit_id` is the load-bearing difference between layers

The three test files test three progressive SQL layers:

| File | `commit_id` on entities? | `user_id` on Commit? |
|------|--------------------------|----------------------|
| `test_schema.py` | No | No |
| `test_schema_versioning.py` | Yes (NOT NULL) | No |
| `test_auth_sql.py` | Yes (NOT NULL) | Yes (NOT NULL) |

If you add a helper that inserts an entity, it must accept `commit_id=None` as a keyword argument so it works in all three contexts. Look at any existing `insert_minimal_*` function in `conftest.py` for the exact pattern.

### Parametrized tests and the `schema` fixture

When using `@pytest.mark.parametrize`, pytest creates a separate test node for each parameter set. The `rollback` fixture runs after each node, so each parametrized case gets a clean slate. This works correctly with no extra effort on your part.

### Test naming

Test names are documentation. Write the name as a sentence describing what the system should do, not what the test does:

```python
# Good — describes a behavior
def test_delete_location_cascades_historical_location(self, schema): ...

# Bad — describes the test mechanics
def test_cascade_check(self, schema): ...
```

---

## Maintaining Structure and Readability

### Section comments

Every test file is divided into numbered sections with block comments. When adding tests, place them in the right section. If your tests don't fit any existing section, add a new one:

```python
# ------------------------------------------------------------------
# 5. Your new section title
# ------------------------------------------------------------------
```

### One assertion concept per test

Each test should verify exactly one behavior. If you find yourself writing `assert x` and `assert y` for two unrelated things, split it into two tests. Multiple assertions that verify the same logical claim (e.g. checking both fields of a returned object) are fine.

### Docstrings on every test

Every test method must have a one-line docstring explaining what it verifies and why the expected outcome is correct. Look at the existing tests for the convention — the docstring explains the *contract*, not the mechanics.

### Helper methods belong on the class only if they're layer-specific

If a helper would work the same way in all three test files, it belongs in `test/database/conftest.py`. If it's specific to one layer (e.g. because it calls `_insert_commit` without `user_id`), it belongs as a private method on that test class. Don't duplicate logic across classes.

### Keep SQL in tests readable

For multi-line SQL inside a test, use a triple-quoted string with consistent indentation. Don't concatenate SQL strings. Don't use f-strings for SQL values — always use psycopg2 parameter placeholders (`%s`) to prevent injection and quoting bugs.

---

## Common Mistakes

### Not activating the virtual environment

Symptoms: `ModuleNotFoundError: No module named 'psycopg2'`, or tests pass on one machine and fail on another. Fix: always activate the venv before running pytest. Check with `which python` / `where python`.

### Running pytest without a venv at all

If you install dependencies globally and then create a venv later, the venv won't have those packages. Always create the venv first, activate it, then install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r api/requirements.txt -r requirements-test.txt
```

### Installing dependencies outside the venv

Running `pip install` without activating the venv installs to your system Python. The next time you activate the venv, the packages won't be there. If this happens, activate the venv and re-run `pip install -r api/requirements.txt -r requirements-test.txt`.

### Forgetting `asyncio_mode = auto` means you don't need `@pytest.mark.asyncio`

Both `pytest.ini` files already set `asyncio_mode = auto`. Adding `@pytest.mark.asyncio` to every async test is unnecessary noise. If you're seeing a warning about it, you're likely running with a different pytest-asyncio version — check that your venv has the right version from `requirements.txt`.

### Using a different event loop policy on Windows

On Windows, Python 3.10+ defaults to `ProactorEventLoop`, which can cause issues with some async database drivers. If you see `RuntimeError: Event loop is closed` on Windows, add this to the root `conftest.py`:

```python
import asyncio
import sys
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

### Calling `schema.commit()` inside a test

This permanently commits your test data, so the `rollback` fixture can no longer clean it up. Subsequent tests will see leftover rows and fail in confusing, non-deterministic ways. Never commit on the shared `schema` connection.

### Opening a raw connection with `autocommit=True` and forgetting to close it

`get_raw_conn()` is for setup (DDL, schema loading). Always close it explicitly when done:

```python
setup_conn = get_raw_conn(DSN)
load_base_schema(setup_conn)
setup_conn.close()  # Don't skip this
```

### Passing `commit_id` to `insert_minimal_*` in base schema tests

The base schema (`test_schema.py`) doesn't have a `commit_id` column. Passing one will cause a `psycopg2.errors.UndefinedColumn`. Use the helpers without `commit_id` in `TestSchema`, and with it in `TestSchemaVersioning` and `TestAuth`.

### Leaking state by not using the rollback pattern

If you write a test that creates a separate autocommit connection and inserts data directly (bypassing the `schema` connection), `rollback` won't clean it up. Either use the `schema` connection for all DML, or manually clean up anything you insert on a separate connection.

### Putting shared helpers inside a test class

Helpers defined as class methods are only accessible within that class. If you write a helper that two test files could use, put it in `test/database/conftest.py` as a module-level function instead.

### Running tests from the wrong directory without a `conftest.py`

If pytest can't find `test.database.conftest` in your import, you're likely running from a directory where neither the root nor `test/conftest.py` has been picked up. Always run from the repo root, or from `test/` (both are supported). Running from `test/database/` directly is not supported.

### Importing from `conftest` using a relative import

Always use the absolute import:

```python
# Correct
from test.database.conftest import insert_minimal_thing

# Wrong — will break depending on invocation directory
from .conftest import insert_minimal_thing
```