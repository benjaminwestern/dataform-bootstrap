"""
Migration orchestration components for BigQuery to Dataform conversion.
Handles the core migration logic and state management.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from .metadata import MetadataCollection, MigrationStatus
from .config import ProjectConfig, OutputConfig
from ..generators.actions import DataformActionsGenerator
from ..generators.sql import SQLGenerator
from ..utils.logging import get_logger

logger = get_logger(__name__)

@dataclass
class MigrationMetrics:
    """Metrics collected during the migration process."""
    tables_processed: int = 0
    jobs_processed: int = 0
    sql_files_generated: int = 0
    duplicate_queries_found: int = 0
    errors_encountered: int = 0
    processing_time_seconds: float = 0.0

@dataclass
class MigrationContext:
    """
    Context for migration execution, containing all necessary configuration and state.
    
    Attributes:
        project_config: Configuration for the source BigQuery project
        output_config: Configuration for output file structure
        metadata: Collected BigQuery metadata
        start_time: Migration start timestamp
        end_time: Migration completion timestamp
        status: Current migration status
        metrics: Collection of migration metrics
    """
    project_config: ProjectConfig
    output_config: OutputConfig
    metadata: Optional[MetadataCollection] = None
    start_time: datetime = field(default_factory=datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    status: MigrationStatus = field(default=MigrationStatus.NOT_STARTED)
    metrics: MigrationMetrics = field(default_factory=MigrationMetrics)

    def calculate_duration(self) -> float:
        """Calculate the total duration of the migration in seconds."""
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for serialisation."""
        return {
            'project_id': self.project_config.project_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status.name,
            'metrics': {
                'tables_processed': self.metrics.tables_processed,
                'jobs_processed': self.metrics.jobs_processed,
                'sql_files_generated': self.metrics.sql_files_generated,
                'duplicate_queries_found': self.metrics.duplicate_queries_found,
                'errors_encountered': self.metrics.errors_encountered,
                'processing_time_seconds': self.metrics.processing_time_seconds
            }
        }

class DataformMigration:
    """
    Orchestrates the migration process from BigQuery to Dataform.
    
    Handles the complete lifecycle of migrating BigQuery objects to
    Dataform configurations, including schema translation, query processing,
    and file generation.
    """
    
    def __init__(self, context: MigrationContext):
        """
        Initialise migration orchestrator.
        
        Args:
            context: Migration context containing configuration and state
        """
        self.context = context
        self._actions_generator = DataformActionsGenerator(
            self.context.project_config,
            self.context.output_config
        )
        self._sql_generator = SQLGenerator(
            self.context.output_config,
            self.context.project_config.similarity_threshold
        )

    def _update_metrics(self) -> None:
        """Update migration metrics based on current state."""
        if self.context.metadata:
            self.context.metrics.tables_processed = len(self.context.metadata.tables)
            self.context.metrics.jobs_processed = len(self.context.metadata.jobs)
        self.context.metrics.processing_time_seconds = self.context.calculate_duration()

    def _generate_dataform_configs(self) -> bool:
        """
        Generate Dataform configuration files.
        
        Returns:
            bool: True if generation was successful
        """
        try:
            if not self.context.metadata:
                logger.error("No metadata available for configuration generation")
                return False

            actions_config = self._actions_generator.generate_actions_yaml(
                self.context.metadata.tables,
                self.context.metadata.jobs
            )
            self._actions_generator.write_actions_yaml(actions_config)
            return True

        except Exception as e:
            logger.error(f"Failed to generate Dataform configurations: {str(e)}")
            self.context.metrics.errors_encountered += 1
            return False

    def _generate_sql_files(self) -> bool:
        """
        Generate SQL files from collected queries.
        
        Returns:
            bool: True if generation was successful
        """
        try:
            if not self.context.metadata:
                logger.error("No metadata available for SQL generation")
                return False

            self._sql_generator.generate_sql_files(self.context.metadata.jobs)
            self.context.metrics.sql_files_generated = len(self.context.metadata.jobs)
            return True

        except Exception as e:
            logger.error(f"Failed to generate SQL files: {str(e)}")
            self.context.metrics.errors_encountered += 1
            return False

    def execute(self) -> bool:
        """
        Execute the complete migration process.
        
        Returns:
            bool: True if migration was successful
        """
        try:
            logger.info("Starting migration execution")
            self.context.status = MigrationStatus.IN_PROGRESS

            if not self._generate_dataform_configs():
                self.context.status = MigrationStatus.FAILED
                return False

            if not self._generate_sql_files():
                self.context.status = MigrationStatus.FAILED
                return False

            self.context.end_time = datetime.now(timezone.utc)
            self._update_metrics()
            self.context.status = MigrationStatus.COMPLETED

            logger.info(
                f"Migration completed successfully in "
                f"{self.context.metrics.processing_time_seconds:.2f} seconds"
            )
            return True

        except Exception as e:
            self.context.status = MigrationStatus.FAILED
            self.context.metrics.errors_encountered += 1
            logger.error(f"Migration execution failed: {str(e)}")
            return False