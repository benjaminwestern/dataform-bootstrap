"""
Argument parsing for Dataform migration CLI.
Provides flexible command-line interface supporting both single and multi-project operations.
"""

import argparse
from pathlib import Path
from typing import List

from .config import OutputFormat

def parse_comma_separated(value: str) -> List[str]:
    """Parse comma-separated string into list of strings."""
    return [item.strip() for item in value.split(',') if item.strip()]

def create_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description="Dataform Migration Tool - Supports single or multiple projects",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--project",
        help="Single project ID or comma-separated list of project IDs",
        type=str,
        required=True
    )
    
    parser.add_argument(
        "--location",
        help="Single location or comma-separated list of locations (default: US)",
        type=str,
        default="US"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Days of history to analyse (default: 30)"
    )
    
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=0.9,
        help="Query similarity threshold (default: 0.9)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Output directory for migration artifacts"
    )
    
    parser.add_argument(
        "--disable-incremental",
        action="store_false",
        dest="enable_incremental",
        help="Disable incremental table detection"
    )
    
    parser.add_argument(
        "--output-mode",
        type=lambda x: OutputFormat[x.upper()],
        choices=list(OutputFormat),
        default=OutputFormat.DETAILED,
        help="Output verbosity level"
    )
    
    return parser