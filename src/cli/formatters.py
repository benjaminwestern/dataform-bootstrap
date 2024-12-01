"""
Output formatting system for migration results.
Provides extensible, type-safe output formatting for different verbosity levels.
"""

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Protocol
import json
from pathlib import Path

from ..models.metadata import MigrationStatus
from ..utils.logging import get_logger

logger = get_logger(__name__)

class OutputFormat(str, Enum):
    """Supported output format types."""
    MINIMAL = "minimal"
    DETAILED = "detailed"
    JSON = "json"
    # TODO Placeholder for future enhancement
    # YAML = "yaml"
    # HTML = "html"
    # MARKDOWN = "markdown"

@dataclass
class MigrationResult:
    """Type-safe container for migration results."""
    project_id: str
    status: MigrationStatus
    start_time: datetime
    end_time: datetime
    location_results: Dict[str, bool]
    metrics: Dict[str, Any]
    errors: List[Dict[str, Any]]

class OutputFormatter(Protocol):
    """Protocol defining output formatter interface."""
    
    @abstractmethod
    def format_results(self, results: Dict[str, MigrationResult]) -> str:
        """Format migration results according to specific output requirements."""
        pass

    @abstractmethod
    def write_report(self, results: Dict[str, MigrationResult], output_dir: Path) -> None:
        """Write formatted results to output directory."""
        pass

class MinimalFormatter:
    """Provides minimal single-character status output."""
    
    def format_results(self, results: Dict[str, MigrationResult]) -> str:
        """Return ✓ for all success, ✗ for any failure."""
        return "✓" if all(r.status == MigrationStatus.COMPLETED for r in results.values()) else "✗"
    
    def write_report(self, results: Dict[str, MigrationResult], output_dir: Path) -> None:
        """Write minimal status to status.txt."""
        status_file = output_dir / "status.txt"
        status_file.write_text(self.format_results(results))

class DetailedFormatter:
    """Provides comprehensive human-readable output."""
    
    def format_results(self, results: Dict[str, MigrationResult]) -> str:
        """Format detailed results with statistics and status."""
        lines = ["Migration Results", "=" * 30]
        
        for project_id, result in results.items():
            lines.extend([
                f"\nProject: {project_id}",
                f"Status: {result.status.name}",
                f"Duration: {(result.end_time - result.start_time).total_seconds():.2f}s",
                "\nLocation Results:",
                *[f"- {loc}: {'✓' if success else '✗'}" 
                  for loc, success in result.location_results.items()],
                "\nMetrics:",
                *[f"- {k}: {v}" for k, v in result.metrics.items()],
            ])
            
            if result.errors:
                lines.extend([
                    "\nErrors:",
                    *[f"- {error['component']}: {error['error']}"
                      for error in result.errors]
                ])
        
        return "\n".join(lines)
    
    def write_report(self, results: Dict[str, MigrationResult], output_dir: Path) -> None:
        """Write detailed report to report.txt."""
        report_file = output_dir / "report.txt"
        report_file.write_text(self.format_results(results))

class JSONFormatter:
    """Provides JSON-formatted output."""
    
    def format_results(self, results: Dict[str, MigrationResult]) -> str:
        """Format results as JSON string."""
        formatted = {
            project_id: {
                "status": result.status.name,
                "duration_seconds": (result.end_time - result.start_time).total_seconds(),
                "location_results": result.location_results,
                "metrics": result.metrics,
                "errors": result.errors
            }
            for project_id, result in results.items()
        }
        return json.dumps(formatted, indent=2)
    
    def write_report(self, results: Dict[str, MigrationResult], output_dir: Path) -> None:
        """Write JSON results to results.json."""
        json_file = output_dir / "results.json"
        json_file.write_text(self.format_results(results))

class OutputManager:
    """Manages output formatting based on specified format."""
    
    _formatters = {
        OutputFormat.MINIMAL: MinimalFormatter(),
        OutputFormat.DETAILED: DetailedFormatter(),
        OutputFormat.JSON: JSONFormatter()
    }
    
    def __init__(self, format_type: OutputFormat = OutputFormat.DETAILED):
        """Initialise with specified output format."""
        self.formatter = self._formatters.get(format_type)
        if not self.formatter:
            logger.warning(f"Unsupported format {format_type}, falling back to detailed")
            self.formatter = self._formatters[OutputFormat.DETAILED]
    
    def format_results(self, results: Dict[str, MigrationResult]) -> str:
        """Format results using configured formatter."""
        return self.formatter.format_results(results)
    
    def write_report(self, results: Dict[str, MigrationResult], output_dir: Path) -> None:
        """Write formatted report using configured formatter."""
        reports_dir = output_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        self.formatter.write_report(results, reports_dir)