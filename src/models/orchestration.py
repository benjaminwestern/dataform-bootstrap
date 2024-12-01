"""
Orchestration models for multi-project, multi-location Dataform migrations.
Handles project coordination, state management, and data persistence.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any
import json
import yaml

from ..collectors.bigquery import BigQueryMetadataCollector
from ..generators.actions import DataformActionsGenerator
from ..generators.sql import SQLGenerator
from .config import ProjectConfig, OutputConfig
from .metadata import MigrationStatus, LocationConfig, ProjectMigrationConfig, ProjectMigrationState
from ..utils.logging import get_logger

logger = get_logger(__name__)

class DataPersistence:
    """Handles persistence of collected metadata and migration state."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.raw_dir = base_dir / 'raw'

    def save_tables(self, location: str, tables: List[Dict[str, Any]]):
        """Save collected table metadata as NDJSON."""
        path = self.raw_dir / f'tables_{location}.ndjson'
        self._write_ndjson(path, tables)

    def save_jobs(self, location: str, jobs: List[Dict[str, Any]]):
        """Save collected job metadata as NDJSON."""
        path = self.raw_dir / f'jobs_{location}.ndjson'
        self._write_ndjson(path, jobs)

    def _write_ndjson(self, path: Path, items: List[Dict[str, Any]]):
        """Write items to NDJSON file."""
        with open(path, 'w') as f:
            for item in items:
                json.dump(item, f, default=str)
                f.write('\n')

class ProjectMigrationManager:
    """Manages migration process for a single project."""
    
    def __init__(self, config: ProjectMigrationConfig):
        self.config = config
        self.state = ProjectMigrationState(config)
        
    def _setup_location(self, location_config: LocationConfig) -> OutputConfig:
        """Setup output configuration for a location."""
        workflow_config = {
            'dataformCoreVersion': '3.0.8', # TODO hard coded version for now - make configurable later
            'defaultProject': self.config.project_id,
            'defaultDataset': location_config.default_dataset,
            'defaultLocation': location_config.location,
            'defaultAssertionDataset': location_config.assertion_dataset
        }

        output_config = OutputConfig(location_config.output_dir)
        output_config.create_directories()
        
        with open(location_config.output_dir / 'workflow_settings.yaml', 'w') as f:
            yaml.dump(workflow_config, f)
            
        return output_config
        
    def migrate_location(self, location_config: LocationConfig) -> bool:
        """Migrate data for a specific location."""
        try:
            project_config = ProjectConfig(
                project_id=self.config.project_id,
                locations=[location_config.location],
                days_of_history=self.config.days_of_history,
                similarity_threshold=self.config.similarity_threshold,
                output_dir=location_config.output_dir
            )
            output_config = self._setup_location(location_config)
            
            collector = BigQueryMetadataCollector(project_config, location_config)
            metadata = collector.collect()
            collector.close()

            persistence = DataPersistence(location_config.output_dir)
            persistence.save_tables(location_config.location, metadata.tables)
            persistence.save_jobs(location_config.location, metadata.jobs)
            
            actions_generator = DataformActionsGenerator(project_config, output_config)
            actions_config = actions_generator.generate_actions_yaml(
                metadata.tables,
                metadata.jobs
            )
            actions_generator.write_actions_yaml(actions_config)

            sql_generator = SQLGenerator(output_config, self.config.similarity_threshold)
            sql_generator.generate_sql_files(metadata.jobs)
            
            return True
            
        except Exception as e:
            self.state.metrics.add_error(
                'migration_location',
                e,
                {'location': location_config.location}
            )
            return False

    def migrate(self) -> bool:
        """Execute migration for all locations in the project."""
        self.state.state = MigrationStatus.IN_PROGRESS
        
        success = True
        for location_config in self.config.locations:
            logger.info(f"Migrating location: {location_config.location}")
            if not self.migrate_location(location_config):
                success = False
                
        self.state.state = MigrationStatus.COMPLETED if success else MigrationStatus.FAILED
        self.state.metrics.end_time = datetime.now(timezone.utc)
        return success

class DataformMigrationOrchestrator:
    """Orchestrates migration across multiple projects and locations."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
    def create_project_config(
        self,
        project_id: str,
        locations: List[str],
        **kwargs
    ) -> ProjectMigrationConfig:
        """Create configuration for a project migration."""
        location_configs = [
            LocationConfig(
                location=loc,
                output_dir=self.base_dir / project_id / loc
            )
            for loc in locations
        ]
        
        return ProjectMigrationConfig(
            project_id=project_id,
            locations=location_configs,
            **kwargs
        )
        
    def migrate_projects(self, projects: List[str], **kwargs) -> Dict[str, bool]:
        """
        Migrate multiple projects to Dataform.
        
        Args:
            projects: List of project IDs to migrate
            **kwargs: Additional configuration options
            
        Returns:
            Dict mapping project IDs to migration success status
        """
        results = {}
        
        for project_id in projects:
            try:
                config = self.create_project_config(project_id, **kwargs)
                manager = ProjectMigrationManager(config)
                results[project_id] = manager.migrate()
                
            except Exception as e:
                logger.error(f"Failed to migrate project {project_id}: {str(e)}")
                results[project_id] = False
                
        return results