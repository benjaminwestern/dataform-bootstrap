"""
Metadata models for BigQuery to Dataform migration.
Defines the core data structures used throughout the application.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from uuid import uuid4

def _get_utc_now() -> datetime:
    """
    Returns the current UTC timestamp.
    
    Returns:
        datetime: Current UTC timestamp
    """
    return datetime.now(timezone.utc)

class MigrationStatus(Enum):
    """Enumeration of possible migration states."""
    NOT_STARTED = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()

@dataclass
class LocationConfig:
    """Configuration for a BigQuery location."""
    location: str
    output_dir: Path
    default_dataset: str = "dataform_staging"
    assertion_dataset: str = "dataform_assertions"

@dataclass
class ProjectMigrationConfig:
    """Configuration for a single project migration."""
    project_id: str
    locations: List[LocationConfig]
    days_of_history: int = 30
    similarity_threshold: float = 0.9
    enable_incremental: bool = True
    batch_size: int = 1000

@dataclass
class MigrationMetrics:
    """Metrics for tracking migration progress and results."""
    total_tables: int = 0
    total_views: int = 0
    total_jobs: int = 0
    successful_tables: int = 0
    successful_views: int = 0
    successful_jobs: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    start_time: datetime = field(default_factory=_get_utc_now)
    end_time: Optional[datetime] = None
    
    def add_error(self, component: str, error: Exception, context: Dict[str, Any]):
        """Add error with context to metrics."""
        self.errors.append({
            'component': component,
            'error': str(error),
            'context': context,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

@dataclass
class ProjectMigrationState:
    """State tracking for project migration."""
    config: ProjectMigrationConfig
    metrics: MigrationMetrics = field(default_factory=MigrationMetrics)
    state: MigrationStatus = MigrationStatus.NOT_STARTED
    run_id: str = field(default_factory=lambda: str(uuid4()))

@dataclass
class ColumnMetadata:
    """
    Represents metadata for a BigQuery column, including nested fields and policy information.
    
    Attributes:
        name: Column name
        field_type: BigQuery data type
        description: Optional column description
        mode: Column mode (NULLABLE, REQUIRED, REPEATED)
        policy_tags: List of policy tag IDs
        fields: Nested fields for RECORD types
        tags: Custom tags for documentation
    """
    name: str
    field_type: str
    description: Optional[str] = None
    mode: str = "NULLABLE"
    policy_tags: List[str] = field(default_factory=list)
    fields: List['ColumnMetadata'] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

@dataclass
class SchemaMetadata:
    """
    Represents the complete schema metadata for a BigQuery table.
    
    Attributes:
        columns: List of column metadata
        primary_keys: Optional list of primary key columns
        foreign_keys: Optional mapping of column to referenced table
    """
    columns: List[ColumnMetadata] = field(default_factory=list)
    primary_keys: Optional[List[str]] = None
    foreign_keys: Optional[Dict[str, str]] = None

@dataclass
class JobMetadata:
    """
    Represents metadata for a BigQuery job.
    
    Attributes:
        job_id: Unique job identifier
        created_time: Job creation timestamp
        job_type: Type of job (QUERY, LOAD, COPY, etc.)
        statement_type: SQL statement type
        destination_table: Target table information
        query: SQL query text
        referenced_tables: Tables referenced in the query
        labels: Job labels
    """
    job_id: str
    created_time: datetime
    job_type: str
    statement_type: Optional[str] = None
    destination_table: Optional[Dict[str, str]] = None
    query: Optional[str] = None
    referenced_tables: List[Dict[str, str]] = field(default_factory=list)
    labels: Optional[Dict[str, str]] = None

@dataclass
class TableMetadata:
    """
    Represents metadata for a BigQuery table or view.
    
    Attributes:
        project_id: BigQuery project ID
        dataset_id: Dataset name
        table_id: Table name
        table_type: Type (TABLE, VIEW, etc.)
        schema: Table schema metadata
        created_time: Creation timestamp
        last_modified_time: Last modification timestamp
        partitioning: Partitioning configuration
        clustering: Clustering configuration
        labels: Table labels
    """
    project_id: str
    dataset_id: str
    table_id: str
    table_type: str
    schema: SchemaMetadata
    created_time: Optional[datetime] = None
    last_modified_time: Optional[datetime] = None
    partitioning: Optional[Dict[str, Any]] = None
    clustering: Optional[List[str]] = None
    labels: Optional[Dict[str, str]] = None

@dataclass
class MetadataCollection:
    """
    Container for all collected metadata from a BigQuery project.
    
    Attributes:
        tables: List of table metadata
        jobs: List of job metadata
        collection_time: When the metadata was collected
    """
    tables: List[TableMetadata] = field(default_factory=list)
    jobs: List[JobMetadata] = field(default_factory=list)
    collection_time: datetime = field(default_factory=_get_utc_now)

@dataclass
class DataformAction:
    """Representation of a Dataform action configuration."""
    type: str
    name: str
    schema: str
    description: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    partition_by: Optional[str] = None
    cluster_by: Optional[List[str]] = None
    labels: Optional[Dict[str, str]] = None
    tags: Optional[List[str]] = None