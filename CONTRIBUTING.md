# Contributing to istSOS4

Thank you for your interest in contributing to istSOS4! This document will help
you get started with setting up the project, running tests, following code style
guidelines, and submitting pull requests.

If you have questions, feel free to open an issue or reach out on the
[OSGeo Discourse](https://discourse.osgeo.org).

---

## Setting Up the Project

### Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop) and Docker Compose
- [Git](https://git-scm.com/)
- Python 3.10+

### Steps

1. **Fork** the repository on GitHub and clone your fork:

   ```sh
   git clone https://github.com/<your-username>/istSOS4.git
   cd istSOS4
   ```

2. Add the upstream remote:

   ```sh
   git remote add upstream https://github.com/istSOS/istSOS4.git
   ```

3. Copy the example environment file and fill in your values:

   ```sh
   cp .env.example .env
   ```

   At minimum, set `SECRET_KEY` before running the stack:

   ```sh
   SECRET_KEY=your_generated_secret_key
   ```

4. Start the development environment:

   ```sh
   docker compose -f dev_docker-compose.yml up -d
   ```

5. The SensorThings API will be available at:

   ```
   http://127.0.0.1:8018/istsos4/v1.1
   ```

6. To stop the services:

   ```sh
   docker compose -f dev_docker-compose.yml down
   ```

---

## Running the Tests

istSOS4 uses [pytest](https://docs.pytest.org/) for testing.

### Run all tests

```sh
pytest
```

### Run a specific test file

```sh
pytest tests/test_auth.py
```

### Run tests with verbose output

```sh
pytest -v
```

Make sure the Docker services are running before executing tests that require
a database connection.

---

## Code Style

istSOS4 follows consistent Python code style using **black** and **pylint**.

### Formatting with black

To automatically format your code:

```sh
black .
```

To check formatting without making changes:

```sh
black --check .
```

### Linting with pylint

To check your code for issues:

```sh
pylint app/
```

Please make sure your code passes both `black` and `pylint` checks before
submitting a pull request. These checks will also run automatically on your
pull request via CI.

---

## Forking and Pull Requests

We use a **fork and pull request** workflow for all contributions.

### Steps to submit a contribution

1. **Fork** the repository on GitHub.

2. **Clone** your fork and add the upstream remote (see Setup above).

3. **Create a new branch** for your change:

   ```sh
   git checkout -b fix/your-fix-description
   ```

4. **Make your changes** and commit them with a clear message:

   ```sh
   git add .
   git commit -m "fix: short description of what was fixed"
   ```

5. **Keep your branch up to date** with upstream:

   ```sh
   git fetch upstream
   git rebase upstream/main
   ```

6. **Push** your branch to your fork:

   ```sh
   git push origin fix/your-fix-description
   ```

7. Open a **Pull Request** on GitHub from your fork to the main istSOS4
   repository. Describe what your PR does and reference any related issues
   (e.g. `Closes #80`).

### Pull request checklist

- [ ] Code is formatted with `black`
- [ ] Code passes `pylint` checks
- [ ] Tests are added or updated where relevant
- [ ] PR description clearly explains the change
- [ ] Related issue is referenced in the PR description

---

## Reporting Issues

Found a bug or have a feature request? Please open an issue at:

```
https://github.com/istSOS/istSOS4/issues
```

Provide as much detail as possible including steps to reproduce,
expected behaviour, and actual behaviour.