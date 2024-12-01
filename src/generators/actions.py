"""
Dataform action configuration generator module.
Handles the generation of Dataform actions.yaml configurations from BigQuery metadata.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import yaml

from ..models.metadata import TableMetadata, JobMetadata, ColumnMetadata
from ..models.config import ProjectConfig, OutputConfig
from ..utils.logging import get_logger

logger = get_logger(__name__)

@dataclass
class ActionDefinition:
    """
    Represents a single Dataform action definition.
    
    Attributes:
        type: Type of action (table, view, incremental)
        name: Action name
        schema: Dataset/schema name
        description: Optional description
        columns: List of column definitions
        dependencies: List of dependent actions
        config: Additional configuration options
    """
    type: str
    name: str
    filename: str
    schema: str
    description: Optional[str] = None
    columns: List[Dict[str, Any]] = None
    dependencies: List[str] = None
    config: Dict[str, Any] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert action definition to dictionary format."""
        action_dict = {
            'type': self.type,
            'name': self.name,
            'filename': self.filename,
            'schema': self.schema
        }
        
        if self.description:
            action_dict['description'] = self.description
            
        if self.columns:
            action_dict['columns'] = self.columns
            
        if self.dependencies:
            action_dict['dependencies'] = sorted(self.dependencies)
            
        if self.config:
            action_dict.update(self.config)
            
        return action_dict

class DataformActionsGenerator:
    """
    Generates Dataform action configurations from BigQuery metadata.
    """
    
    def __init__(self, project_config: ProjectConfig, output_config: OutputConfig):
        """
        Initialise the actions generator.
        
        Args:
            project_config: Project-level configuration
            output_config: Output directory configuration
        """
        self.project_config = project_config
        self.output_config = output_config

    def _convert_column_metadata(self, column: ColumnMetadata) -> Dict[str, Any]:
        """
        Convert ColumnMetadata to Dataform column configuration.
        
        Args:
            column: Column metadata object
            
        Returns:
            Dictionary containing Dataform column configuration
        """
        column_config = {
            'name': column.name,
            # 'type': column.field_type.lower(), -- TODO, generate a 'schema' file but not in the actions.yaml
        }
        
        if column.description:
            column_config['description'] = column.description
            
        if column.policy_tags:
            column_config['bigqueryPolicyTags'] = column.policy_tags
            
        if column.fields:
            column_config['fields'] = [
                self._convert_column_metadata(field)
                for field in column.fields
            ]
            
        return column_config

    def _generate_config_from_table(
        self,
        table: TableMetadata,
    ) -> Dict[str, Any]:
        """
        Generate configuration options from table metadata.
        
        Args:
            table: Table metadata object
            
        Returns:
            Dictionary containing Dataform configuration options
        """
        config = {}
        
        # TODO Add WAY more configuration options here, I want a FULL snapshot of the table metadata
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
    ) -> List[str]:
        """
        Collect and deduplicate dependencies from jobs.
        
        Args:
            table: Table metadata object
            jobs: List of related job metadata
            
        Returns:
            List of unique dependency references
        """
        dependencies = set()
        table_ref = f"{table.project_id}.{table.dataset_id}.{table.table_id}"
        
        for job in jobs:
            for ref in job.referenced_tables:
                ref_str = f"{ref['projectId']}.{ref['datasetId']}.{ref['tableId']}"
                if ref_str != table_ref:
                    dependencies.add(ref_str)
        
        return sorted(list(dependencies))

    def generate_action(
        self,
        table: TableMetadata,
        jobs: List[JobMetadata]
    ) -> ActionDefinition:
        """
        Generate a single Dataform action definition.
        
        Args:
            table: Table metadata object
            jobs: List of related job metadata
            
        Returns:
            ActionDefinition object
        """
        columns = [
            self._convert_column_metadata(col)
            for col in table.schema.columns
        ]
        
        dependencies = self._collect_dependencies(table, jobs)
        config = self._generate_config_from_table(table)

        return ActionDefinition(
            type='table' if table.table_type == 'TABLE' else 'view',
            name=table.table_id,
            filename=f"{self.output_config.definitions_dir}/{table.dataset_id}/{table.table_id}.sql",
            schema=table.dataset_id,
            description=f"Auto-generated from {table.project_id}.{table.dataset_id}.{table.table_id}",
            columns=columns,
            dependencies=dependencies,
            config=config
        )

    def generate_actions_yaml(
        self,
        tables: List[TableMetadata],
        jobs: List[JobMetadata]
    ) -> Dict[str, Any]:
        """
        Generate complete actions.yaml configuration.
        
        Args:
            tables: List of table metadata objects
            jobs: List of job metadata objects
            
        Returns:
            Dictionary containing complete actions.yaml configuration
        """
        actions_config = {
            'version': 2,
            'actions': []
        }
        
        # TODO Add tests to this, to ensure that I am not unintentionally overwriting actions or missing operations, etc.
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
        
        for table in tables:
            table_key = (table.project_id, table.dataset_id, table.table_id)
            table_jobs = jobs_by_table.get(table_key, [])
            
            action = self.generate_action(table, table_jobs)
            actions_config['actions'].append(action.to_dict())
        
        return actions_config

    def write_actions_yaml(self, actions_config: Dict[str, Any]) -> None:
        """
        Write actions configuration to YAML file.
        
        Args:
            actions_config: Complete actions configuration dictionary
        """
        actions_file = self.output_config.definitions_dir / 'actions.yaml'
        
        try:
            # Custom YAML formatting
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