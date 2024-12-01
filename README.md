# Dataform Bootstrap

## Description
A robust, scalable Python tool for analysing BigQuery environments and automatically generating Dataform configurations. 
This tool helps migrate existing BigQuery Environments and workflows to Dataform by analysing table metadata, query history, and generating corresponding Dataform configurations.

## IMPORTANT
This project is designed to bring the power of Dataform to existing BigQuery environments. It is not a replacement for Dataform, nor is it a standalone tool. It is designed to be used in conjunction with Dataform to streamline the migration and adoption process. The tool simply captures all metadata and sql queries from BigQuery and generates Dataform configurations based on the analysis.

**It will get you 90% of the way there, but you will still need to manually review and refine the generated Dataform configurations to ensure they meet your specific requirements.**

## DISCLAIMER
This project is still in the early stages of development, and as such, there may be bugs, incomplete features, and other issues. Please use with caution and report any issues you encounter.

## Table of Contents
- [Dataform Bootstrap](#dataform-bootstrap)
  - [Description](#description)
  - [IMPORTANT](#important)
  - [DISCLAIMER](#disclaimer)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
    - [Analysis and Metadata Collection](#analysis-and-metadata-collection)
    - [Dataform Configuration Generation](#dataform-configuration-generation)
    - [Query Deduplication](#query-deduplication)
  - [Repository Structure](#repository-structure)
  - [Requirements](#requirements)
    - [Google Cloud](#google-cloud)
    - [Development Environment](#development-environment)
    - [Configuration Options](#configuration-options)
      - [Command-Line Interface Options](#command-line-interface-options)
        - [Required Arguments](#required-arguments)
        - [Optional Arguments](#optional-arguments)
      - [Output Modes](#output-modes)
      - [Configuration Precedence](#configuration-precedence)
  - [Example Usage](#example-usage)
    - [Python Script Examples](#python-script-examples)
      - [Single Project, Single Location](#single-project-single-location)
      - [Single Project, Multiple Locations](#single-project-multiple-locations)
      - [Multiple Projects, Multiple Locations](#multiple-projects-multiple-locations)
    - [Shell Script Examples](#shell-script-examples)
      - [Single Project, Single Location](#single-project-single-location-1)
      - [Single Project, Multiple Locations](#single-project-multiple-locations-1)
      - [Multiple Projects, Multiple Locations](#multiple-projects-multiple-locations-1)
      - [Environment Variables Configuration](#environment-variables-configuration)
  - [Output Structure](#output-structure)
  - [References](#references)
    - [Dataform](#dataform)
    - [Google Cloud](#google-cloud-1)
  - [License](#license)
  - [Contributing](#contributing)
  - [Roadmap](#roadmap)
  - [Author](#author)

## Features
### Analysis and Metadata Collection

*   Analyses BigQuery table metadata, including schema, partitioning, clustering, and labels.
*   Examines query history to understand data dependencies and transformation logic.
*   Collects job metadata such as creation time, query details, and referenced tables.

### Dataform Configuration Generation

*   Supports multi-project and multi-location migrations, generating separate configurations for each unique pair.
*   Constructs `workflow_config.yaml` with project-level settings for Dataform at each location level.
*   Creates SQL files representing the transformation logic for each table.
*   Generates `actions.yaml` file defining Dataform actions for each table and view, which contains metadata and relevant SQL query paths.

### Query Deduplication

*   Identifies similar queries using a configurable similarity threshold.
*   Deduplicates queries to minimise redundancy in generated Dataform actions.
*   Logs deduplication decisions for transparency and review.

## Repository Structure

```bash
.
├── CONTRIBUTING.md
├── LICENSE
├── README.md
├── ROADMAP.md
├── requirements.txt
└── src
    ├── cli               # CLI implementation
    ├── collectors        # Data collection modules (BigQuery only for now)
    ├── generators        # Dataform config and SQL generators
    ├── models            # Core data models
    └── utils             # Utility functions
```

## Requirements
### Google Cloud
- Have an active Google Cloud BigQuery project
- Have the necessary permissions to access the BigQuery API

### Development Environment
1. Install [Python](https://www.python.org/downloads/) (v3.10 or higher)
2. Install [Node.js](https://nodejs.org/en/download/) (v20 or higher)
3. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs)
4. Install [Dataform CLI](https://cloud.google.com/dataform/docs/use-dataform-cli) (v3.0.8 or higher, it is recommended to install it globally)
5. Authenticate with Google Cloud SDK using the following commands:
   1. `gcloud auth login` (This will open a browser window to authenticate with your Google Account)
   2. `gcloud config set project <PROJECT_ID>` (replace `<PROJECT_ID>` with your Google Cloud Project ID you created earlier)
   3. `gcloud auth application-default login` (This sets up the application default credentials for your project)
   4. `gcloud auth application-default set-quota-project <PROJECT_ID>` (This sets the quota project for your project)
---

### Configuration Options

Configuration can be provided through either command-line arguments or environment variables.

#### Command-Line Interface Options

```bash
python -m src.cli.main [OPTIONS]
```

##### Required Arguments

| Argument    | Description                               | Type | Default  | Environment Variable |
| ----------- | ----------------------------------------- | ---- | -------- | -------------------- |
| `--project` | Single project ID or comma-separated list | str  | Required | `DATAFORM_PROJECTS`  |

##### Optional Arguments

| Argument                 | Description                              | Type  | Default    | Environment Variable            |
| ------------------------ | ---------------------------------------- | ----- | ---------- | ------------------------------- |
| `--location`             | BigQuery location(s)                     | str   | "US"       | `DATAFORM_LOCATIONS`            |
| `--days`                 | Days of history to analyse               | int   | 30         | `DATAFORM_HISTORY_DAYS`         |
| `--similarity-threshold` | Query similarity threshold               | float | 0.9        | `DATAFORM_SIMILARITY_THRESHOLD` |
| `--output-dir`           | Output directory path                    | Path  | "output"   | `DATAFORM_OUTPUT_DIR`           |
| `--disable-incremental`  | Disable incremental detection            | flag  | False      | `DATAFORM_ENABLE_INCREMENTAL`   |
| `--output-mode`          | Output verbosity (minimal/detailed/json) | str   | "detailed" | `DATAFORM_OUTPUT_MODE`          |

#### Output Modes

The tool supports three output modes (eventually :smile:):
- `minimal`: Single character status (✓/✗)
- `detailed`: Comprehensive report with per-project status
- `json`: JSON-formatted output with complete status information

#### Configuration Precedence

1. Command-line arguments (highest priority)
2. Environment variables
3. Default values (lowest priority)

## Example Usage

### Python Script Examples

The following example scripts demonstrate different usage patterns:

#### Single Project, Single Location
```python
"""
Example of migrating a single project in a single location.
"""

from pathlib import Path
from src.cli.main import run_cli

def main():
    args = [
        "--project", "my-project-id",
        "--location", "US",
        "--days", "30",
        "--similarity-threshold", "0.9",
        "--output-dir", str(Path("output/single_single")),
        "--output-mode", "detailed"
    ]
    return run_cli(args)

if __name__ == "__main__":
    main()
```

#### Single Project, Multiple Locations
```python
"""
Example of migrating a single project across multiple locations.
"""

from pathlib import Path
from src.cli.main import run_cli

def main():
    args = [
        "--project", "my-project-id",
        "--location", "US,EU,ASIA",
        "--days", "30",
        "--output-dir", str(Path("output/single_multi"))
    ]
    return run_cli(args)

if __name__ == "__main__":
    main()
```

#### Multiple Projects, Multiple Locations
```python
"""
Example of migrating multiple projects across multiple locations.
"""

from pathlib import Path
from src.cli.main import run_cli

def main():
    args = [
        "--project", "project-1,project-2,project-3",
        "--location", "US,EU,ASIA",
        "--output-dir", str(Path("output/multi_multi")),
        "--output-mode", "json"
    ]
    return run_cli(args)

if __name__ == "__main__":
    main()
```

### Shell Script Examples

#### Single Project, Single Location
```bash
#!/bin/bash

python -m src.cli.main \
    --project "my-project-id" \
    --location "US" \
    --days 30 \
    --similarity-threshold 0.9 \
    --output-dir "output/single_single" \
    --output-mode detailed
```

#### Single Project, Multiple Locations
```bash
#!/bin/bash

python -m src.cli.main \
    --project "my-project-id" \
    --location "US,EU,ASIA" \
    --days 30 \
    --output-dir "output/single_multi"
```

#### Multiple Projects, Multiple Locations
```bash
#!/bin/bash

python -m src.cli.main \
    --project "project-1,project-2,project-3" \
    --location "US,EU,ASIA" \
    --output-dir "output/multi_multi" \
    --output-mode json
```

#### Environment Variables Configuration
```bash
#!/bin/bash

export DATAFORM_PROJECTS="project-1,project-2,project-3"
export DATAFORM_LOCATIONS="US,EU,ASIA"
export DATAFORM_HISTORY_DAYS="30"
export DATAFORM_SIMILARITY_THRESHOLD="0.9"
export DATAFORM_OUTPUT_DIR="output/env_vars"
export DATAFORM_ENABLE_INCREMENTAL="true"
export DATAFORM_OUTPUT_MODE="detailed"

python -m src.cli.main
```

## Output Structure
```bash
.
└── output                                    # default output directory
    └── <output-dir>                          # specified output directory
        └── my-project-id                     # repeats for each project specified
            └── <location>                    # repeats for each location in project specified
                ├── definitions
                │   ├── actions.yaml
                │   └── <dataset>              # repeats for each dataset in project identified
                │       └── <table>.sql        # repeats for each table in dataset identified
                ├── logs
                │   └── <table_log>.ndjson     # repeats for each table in project identified
                ├── raw
                │   ├── jobs_US.ndjson
                │   └── tables_US.ndjson
                └── workflow_config.yaml      # project-level configuration file - Each project + location combination will have a separate file as Dataform does not support multi-location projects
```

## References
### Dataform
- [Documentation](https://cloud.google.com/dataform/docs/overview)
- [Best Practices](https://cloud.google.com/dataform/docs/best-practices)
- [Troubleshooting](https://cloud.google.com/dataform/docs/troubleshooting)
- [Core Github](https://github.com/dataform-co/dataform)
- [API Reference](https://cloud.google.com/dataform/reference/rest)
- [Core Reference](https://cloud.google.com/dataform/docs/reference/dataform-core-reference)
- [CLI Reference](https://cloud.google.com/dataform/docs/reference/dataform-cli-reference)
- [Dataform Core - VSCode Extension](https://marketplace.visualstudio.com/items?itemName=dataform.dataform)

### Google Cloud
- [BigQuery](https://cloud.google.com/bigquery/docs)
- [BigQuery Python Client](https://googleapis.dev/python/bigquery/latest/index.html)
- [BigQuery Python SDK](https://cloud.google.com/python/docs/reference/bigquery/latest)

## License
This repository is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on my code of conduct, and the process for submitting pull requests.

## Roadmap
Please read [ROADMAP.md](ROADMAP.md) for a list of planned features and enhancements.

## Author
[Benjamin Western](https://benjaminwestern.io)