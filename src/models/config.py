"""
Configuration classes for BigQuery project analysis.
"""

from dataclasses import dataclass, field
from typing import List
from pathlib import Path

@dataclass
class ProjectConfig:
    """Configuration settings for BigQuery project analysis."""
    project_id: str
    locations: List[str] = field(default_factory=lambda: ['US'])
    days_of_history: int = 30
    similarity_threshold: float = 0.9
    output_dir: Path = field(default_factory=lambda: Path('output'))

    def __post_init__(self):
        """Validate and convert output_dir to Path if necessary."""
        if isinstance(self.output_dir, str):
            self.output_dir = Path(self.output_dir)

@dataclass
class OutputConfig:
    """Configuration for output directory structure."""
    base_dir: Path
    definitions_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    raw: Path = field(init=False)

    def __post_init__(self):
        """Initialise directory structure."""
        self.definitions_dir = self.base_dir / 'definitions'
        self.logs_dir = self.base_dir / 'logs'
        self.raw = self.base_dir / 'raw'

    def create_directories(self):
        """Create all required directories."""
        for directory in [self.definitions_dir, self.logs_dir, self.raw]:
            directory.mkdir(parents=True, exist_ok=True)