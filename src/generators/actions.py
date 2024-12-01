"""
Dataform action configuration generator module.
Handles the generation of Dataform actions.yaml configurations from BigQuery metadata.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Set
import yaml
from pathlib import Path

from ..models.metadata import TableMetadata, JobMetadata, ColumnMetadata
from ..models.config import ProjectConfig, OutputConfig
from ..utils.logging import get_logger

logger = get_logger(__name__)

@dataclass
class DependencyTarget:
    """
    Represents a Dataform dependency target with full reference information.
    
    Attributes:
        project: Google Cloud project ID
        dataset: Dataset/schema name
        name: Action name
    """
    project: str
    dataset: str
    name: str
    
    def to_dict(self) -> Dict[str, str]:
        """Convert dependency target to dictionary format."""
        return {
            'project': self.project,
            'dataset': self.dataset,
            'name': self.name
        }
    
    def __hash__(self) -> int:
        """Enable using DependencyTarget in sets for deduplication."""
        return hash((self.project, self.dataset, self.name))
    
    def __eq__(self, other) -> bool:
        """Enable comparison of DependencyTarget objects."""
        if not isinstance(other, DependencyTarget):
            return False
        return (self.project == other.project and 
                self.dataset == other.dataset and 
                self.name == other.name)

@dataclass
class ColumnConfig:
    """Represents a Dataform column configuration with path-based structure."""
    path: List[str]
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    bigquery_policy_tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert column configuration to dictionary format."""
        config = {'path': self.path}
        
        if self.description:
            config['description'] = self.description
        
        if self.tags:
            config['tags'] = sorted(self.tags)
            
        if self.bigquery_policy_tags:
            config['bigqueryPolicyTags'] = sorted(self.bigquery_policy_tags)
            
        return config

@dataclass
class ActionDefinition:
    """
    Represents a single Dataform action definition.
    
    Attributes:
        type: Type of action (table, view, incremental, declaration)
        name: Action name
        schema: Dataset/schema name
        project: Project ID (required for declarations)
        filename: Optional file path (not used for declarations)
        description: Optional description
        columns: List of column configurations
        dependency_targets: List of dependent actions with full references
        config: Additional configuration options
        disabled: Whether the action is disabled
    """
    type: str
    name: str
    schema: str
    project: str
    filename: Optional[str] = None
    description: Optional[str] = None
    columns: List[ColumnConfig] = field(default_factory=list)
    dependency_targets: List[DependencyTarget] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    disabled: Optional[bool] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert action definition to dictionary format."""
        action_dict = {
            self.type: {
                'name': self.name,
                'dataset': self.schema,
                'project': self.project
            }
        }
        
        if self.type != 'declaration':
            if not self.filename:
                raise ValueError(f"Filename is required for non-declaration action {self.name}")
            action_dict[self.type]['filename'] = self.filename
        
        if self.description:
            action_dict[self.type]['description'] = self.description
            
        if self.columns:
            action_dict[self.type]['columns'] = [
                col.to_dict() for col in sorted(self.columns, key=lambda x: x.path)
            ]
            
        if self.dependency_targets:
            action_dict[self.type]['dependencyTargets'] = [
                dep.to_dict() for dep in sorted(
                    self.dependency_targets,
                    key=lambda x: (x.project, x.dataset, x.name)
                )
            ]
            
        if self.config:
            action_dict[self.type].update(self.config)
            
        return action_dict

class DataformActionsGenerator:
    """Generates Dataform action configurations from BigQuery metadata."""
    
    def __init__(self, project_config: ProjectConfig, output_config: OutputConfig):
        self.project_config = project_config
        self.output_config = output_config
        self._ensure_output_directory()
    
    def _ensure_output_directory(self) -> None:
        """Ensure the output directory structure exists."""
        Path(self.output_config.definitions_dir).mkdir(parents=True, exist_ok=True)
    
    def _ensure_sql_file(self, dataset: str, table_id: str) -> None:
        """
        Ensure SQL file exists for the given table definition.
        
        Args:
            dataset: Dataset/schema name
            table_id: Table ID
            
        Creates an empty SQL file if it doesn't exist.
        """
        dataset_dir = Path(self.output_config.definitions_dir) / dataset
        dataset_dir.mkdir(exist_ok=True)
        
        sql_file = dataset_dir / f"{table_id}.sql"
        if not sql_file.exists():
            sql_file.touch()
            logger.info(f"Created empty SQL file: {sql_file}")

    def _parse_column_path(self, column_name: str) -> List[str]:
        """Parse a column name into path segments."""
        return column_name.split('.')

    def _convert_column_metadata(self, column: ColumnMetadata) -> ColumnConfig:
        """Convert ColumnMetadata to Dataform column configuration."""
        return ColumnConfig(
            path=self._parse_column_path(column.name),
            description=column.description,
            tags=column.tags,
            bigquery_policy_tags=column.policy_tags
        )

    def _generate_config_from_table(
        self,
        table: TableMetadata,
    ) -> Dict[str, Any]:
        """Generate configuration options from table metadata."""
        config = {}
        
        if table.partitioning:
            config['partitionBy'] = table.partitioning.get('field')
            if table.partitioning.get('expirationMs'):
                config['partitionExpirationDays'] = int(
                    int(table.partitioning['expirationMs']) / (24 * 60 * 60 * 1000)
                )

        if table.clustering:
            config['clusterBy'] = table.clustering
        
        if table.labels:
            config['labels'] = table.labels
        
        return config

    def _collect_dependencies(
        self,
        table: TableMetadata,
        jobs: List[JobMetadata]
    ) -> Set[DependencyTarget]:
        """Collect and deduplicate dependencies from jobs."""
        dependencies = set()
        table_ref = f"{table.project_id}.{table.dataset_id}.{table.table_id}"
        
        for job in jobs:
            for ref in job.referenced_tables:
                ref_str = f"{ref['projectId']}.{ref['datasetId']}.{ref['tableId']}"
                if ref_str != table_ref:
                    dependencies.add(DependencyTarget(
                        project=ref['projectId'],
                        dataset=ref['datasetId'],
                        name=ref['tableId']
                    ))
        
        return dependencies

    def _create_declaration(self, dependency: DependencyTarget) -> ActionDefinition:
        """Create a declaration action for an external dependency."""
        return ActionDefinition(
            type='declaration',
            name=dependency.name,
            schema=dependency.dataset,
            project=dependency.project
        )

    def generate_action(
        self,
        table: TableMetadata,
        jobs: List[JobMetadata]
    ) -> ActionDefinition:
        """Generate a single Dataform action definition."""
        # Ensure SQL file exists
        self._ensure_sql_file(table.dataset_id, table.table_id)
        
        columns = [
            self._convert_column_metadata(col)
            for col in table.schema.columns
        ]
        
        dependencies = self._collect_dependencies(table, jobs)
        config = self._generate_config_from_table(table)

        return ActionDefinition(
            type='view' if table.table_type == 'VIEW' else 'table',
            name=table.table_id,
            schema=table.dataset_id,
            project=table.project_id,
            filename=f"{table.dataset_id}/{table.table_id}.sql",
            description=f"Auto-generated from {table.project_id}.{table.dataset_id}.{table.table_id}",
            disabled=False,
            columns=columns,
            dependency_targets=list(dependencies),
            config=config
        )

    def generate_actions_yaml(
        self,
        tables: List[TableMetadata],
        jobs: List[JobMetadata]
    ) -> Dict[str, Any]:
        """Generate complete actions.yaml configuration."""
        actions_config = {'actions': []}
        
        # Track all known tables and dependencies - This is to ensure that required dependencies are declared if they are not present in the tables list
        known_tables = {
            (table.project_id, table.dataset_id, table.table_id)
            for table in tables
        }
        all_dependencies = set()
        
        # Generate primary actions and collect dependencies
        jobs_by_table = {}
        for job in jobs:
            if job.destination_table:
                key = (
                    job.destination_table['projectId'],
                    job.destination_table['datasetId'],
                    job.destination_table['tableId']
                )
                if key not in jobs_by_table:
                    jobs_by_table[key] = []
                jobs_by_table[key].append(job)
        
        # First pass: create main actions and collect dependencies
        actions = []
        for table in tables:
            table_key = (table.project_id, table.dataset_id, table.table_id)
            table_jobs = jobs_by_table.get(table_key, [])
            
            action = self.generate_action(table, table_jobs)
            actions.append(action)
            all_dependencies.update(action.dependency_targets)
        
        # Second pass: add declarations for external dependencies
        for dep in all_dependencies:
            dep_key = (dep.project, dep.dataset, dep.name)
            if dep_key not in known_tables:
                declaration = self._create_declaration(dep)
                actions.append(declaration)
        
        # Sort actions to ensure consistent output
        actions.sort(key=lambda x: (x.type != 'declaration', x.project, x.schema, x.name))
        actions_config['actions'] = [action.to_dict() for action in actions]
        
        return actions_config

    def write_actions_yaml(self, actions_config: Dict[str, Any]) -> None:
        """Write actions configuration to YAML file."""
        actions_file = Path(self.output_config.definitions_dir) / 'actions.yaml'
        
        try:
            yaml.add_representer(
                str,
                lambda dumper, data: dumper.represent_scalar(
                    'tag:yaml.org,2002:str',
                    data,
                    style='|' if '\n' in data else None
                )
            )
            
            with open(actions_file, 'w') as f:
                yaml.dump(
                    actions_config,
                    f,
                    sort_keys=False,
                    default_flow_style=False,
                    allow_unicode=True,
                    width=120,
                    indent=2
                )
            
            logger.info(f"Successfully wrote actions.yaml to {actions_file}")
            
        except Exception as e:
            logger.error(f"Error writing actions.yaml: {str(e)}")
            raise