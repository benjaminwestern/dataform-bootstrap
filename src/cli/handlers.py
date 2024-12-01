"""
Handles migration execution and result formatting.
"""

from typing import Dict
from .formatters import OutputManager, OutputFormat, MigrationResult
from src.models.orchestration import DataformMigrationOrchestrator
from src.utils.logging import get_logger
from .config import CLIConfig

logger = get_logger(__name__)

class MigrationHandler:
    """Handles migration execution and result formatting."""
    
    def __init__(self, config: CLIConfig):
        """Initialise handler with configuration."""
        self.config = config
        self.orchestrator = DataformMigrationOrchestrator(config.output_dir)
        self.output_manager = OutputManager(
            OutputFormat(config.output_mode)
        )
    
    def _collect_results(self, results: Dict[str, bool]) -> Dict[str, MigrationResult]:
        """Convert orchestrator results to detailed MigrationResults."""
        # TODO Placeholder for future enhancement
        # This would need to be implemented based on how I want to track detailed results... Basically im just being lazy and not implementing this
        pass
    
    def run(self) -> int:
        """Execute migration and format results."""
        try:
            results = self.orchestrator.migrate_projects(
                projects=self.config.projects,
                locations=self.config.locations,
                days_of_history=self.config.days_of_history,
                similarity_threshold=self.config.similarity_threshold,
                enable_incremental=self.config.enable_incremental
            )
            
            # TODO Placeholder for future enhancement
            # This would be where I would collect detailed results and write them to a report file and the console
            # Framework is already in place, just need to implement the logic for collecting detailed results

            # detailed_results = self._collect_results(results) # This is a placeholder for future enhancement
            # formatted_output = self.output_manager.format_results(detailed_results)
            # print(formatted_output)
            # self.output_manager.write_report(detailed_results, self.config.output_dir)
            
            return 0 if all(results.values()) else 1
            
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            return 1