# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-unhcr-microdata** connects to the [UNHCR microdata API](https://microdata.unhcr.org/index.php/api/catalog/) and creates a dataset in HDX for each UNHCR-authored study it finds, dataset by dataset.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.unhcr.microdata
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_unhcr.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `pipeline.py`:

1. **`get_dataset_info`** — Queries the UNHCR catalog API and filters to studies with an `idno` prefixed `UNHCR`.
2. **`generate_dataset`** — Fetches a study's metadata, builds an HDX `Dataset` (title, methodology, tags, country locations, time period), and attaches two resources: a login-gated download link and the study's codebook PDF.

`__main__.py` drives the pipeline: it lists dataset info, then for each one calls `generate_dataset` and creates the dataset in HDX, collecting per-dataset errors via `ErrorsOnExit` rather than failing the whole run.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `EXTRA_PARAMS`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-unhcr-microdata` entry.
