# Contributor Guide

## Setup

### Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv): install with `pipx install uv`
- Optional:
  - Graphviz (for DOT/GEXF output)
    - macOS: `brew install graphviz`
    - Linux: `sudo apt install graphviz`
    - Windows: [graphviz.org/download](https://graphviz.org/download/)

### Installation

Install project dependencies:

```bash
uv sync
```

## Branching & Contribution Guidelines

- All contributions should be made in a **feature branch** off of `develop`
- Open a **pull request into `develop`**, not `main`
- The `main` branch is reserved for releases and deploys
- Releases are automatically generated from `develop` using [`release-please`](https://github.com/googleapis/release-please)

## Commit Conventions

We use [Conventional Commits](https://www.conventionalcommits.org/) to ensure consistent history and changelog generation via `release-please`.

Use one of the following prefixes in your commit messages:

- `feat:` — new user-facing feature
- `fix:` — bug fix
- `docs:` — documentation-only change
- `style:` — formatting, whitespace, etc.
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `perf:` — performance improvements
- `test:` — add or fix tests
- `build:` — changes to build tools, dependencies, CI/CD
- `chore:` — other changes that don’t modify app logic

### Examples

```bash
git commit -m "feat: add agency merge strategy"
git commit -m "fix: resolve CLI argument conflict"
git commit -m "docs: clarify Graphviz usage in README"
```


## Development Tasks

### Manual

Run the tests:

```bash
uv run pytest
```

Run static analysis:

```bash
uv run black --check .
uv run isort --check .
uv run mypy allusgov tests
uv run pylint allusgov tests --rcfile=.pylint.ini
uv run pydocstyle allusgov tests
```

Run the CLI:

```bash
uv run allusgov
```

### Continuous Integration

The CI pipeline runs the following:

- Lint checks (black, isort, mypy, pylint, pydocstyle)
- Tests with coverage
- Smoke test for CLI startup
- See `.github/workflows/main.yml`

## Optional: Releasing

> Releases are automatically generated from `develop` by `release-please`.

To build locally:

```bash
uv run python -m build
```
