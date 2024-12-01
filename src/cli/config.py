"""
Configuration management for Dataform migration CLI.
Handles both environment-based and argument-based configuration.
"""

from dataclasses import dataclass
from pathlib import Path
from .formatters import OutputFormat
from typing import List
import os

@dataclass
class CLIConfig:
    """Configuration container for CLI arguments with validation."""
    projects: List[str]
    locations: List[str]
    days_of_history: int
    similarity_threshold: float
    output_dir: Path
    enable_incremental: bool
    output_mode: OutputFormat
    
    @classmethod
    def from_env(cls) -> 'CLIConfig':
        """Create configuration from environment variables."""
        return cls(
            projects=os.getenv('DATAFORM_PROJECTS', '').split(','),
            locations=os.getenv('DATAFORM_LOCATIONS', 'US').split(','),
            days_of_history=int(os.getenv('DATAFORM_HISTORY_DAYS', '30')),
            similarity_threshold=float(os.getenv('DATAFORM_SIMILARITY_THRESHOLD', '0.9')),
            output_dir=Path(os.getenv('DATAFORM_OUTPUT_DIR', 'output')),
            enable_incremental=os.getenv('DATAFORM_ENABLE_INCREMENTAL', 'true').lower() == 'true',
            output_mode=OutputFormat(os.getenv('DATAFORM_OUTPUT_MODE', 'detailed'))
        )
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        if not self.projects or not all(self.projects):
            raise ValueError("At least one project must be specified")
        if not self.locations or not all(self.locations):
            raise ValueError("At least one location must be specified")
        if self.days_of_history < 1:
            raise ValueError("Days of history must be positive")
        if not 0 <= self.similarity_threshold <= 1:
            raise ValueError("Similarity threshold must be between 0 and 1")