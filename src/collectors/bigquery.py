"""
BigQuery metadata collection module.
Handles the collection and processing of metadata from BigQuery environments.
Includes collection of table and job metadata. To be used in conjunction with the CLI module.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from google.cloud import bigquery
from ..models.metadata import (
    TableMetadata, 
    SchemaMetadata, 
    ColumnMetadata, 
    JobMetadata,
    MetadataCollection,
    LocationConfig
)
from ..models.config import ProjectConfig
from ..utils.logging import get_logger

logger = get_logger(__name__)

class BigQueryMetadataCollector:
    """
    Collects and processes metadata from BigQuery environments.
    
    Attributes:
        config (ProjectConfig): Configuration settings for the collection process
        client (bigquery.Client): Authenticated BigQuery client
    """
    
    def __init__(self, config: ProjectConfig, location: LocationConfig):
        self.config = config
        self.location = location
        self.client = bigquery.Client(project=config.project_id, location=location)

    def collect_job_metadata(self) -> List[JobMetadata]:
        """
        Collect metadata for all relevant jobs in the project.
        
        Returns:
            List[JobMetadata]: Collection of processed job metadata
        """
        jobs_metadata = []
        start_time = datetime.now(timezone.utc) - timedelta(days=self.config.days_of_history)
        
        try:
            jobs = self.client.list_jobs(
                project=self.config.project_id,
                min_creation_time=start_time,
                all_users=True
            )
            
            for job in jobs:
                if job.job_type == 'query':
                    try:
                        job_metadata = JobMetadata(
                            job_id=job.job_id,
                            created_time=job.created,
                            job_type=job.job_type,
                            statement_type=job.statement_type if hasattr(job, 'statement_type') else None,
                            destination_table=(
                                job.destination.to_api_repr() 
                                if job.destination 
                                else None
                            ),
                            query=job.query,
                            referenced_tables=[
                                table.to_api_repr() 
                                for table in job.referenced_tables
                            ],
                            labels=job.labels
                        )
                        jobs_metadata.append(job_metadata)
                    except Exception as e:
                        logger.error(f"Error processing job {job.job_id}: {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error collecting jobs: {str(e)}")
        
        return jobs_metadata

    def collect_table_metadata(self) -> List[TableMetadata]:
        """
        Collect metadata for all tables in the project.
        
        Returns:
            List[TableMetadata]: Collection of processed table metadata
        """
        tables_metadata = []
        
        try:
            datasets = list(self.client.list_datasets())
            
            for dataset in datasets:
                logger.debug(f"Processing dataset: {dataset.dataset_id}")
                tables = list(self.client.list_tables(dataset.reference))
                
                for table in tables:
                    try:
                        table_ref = self.client.get_table(table.reference)
                        schema = self._process_table_schema(table_ref)
                        
                        metadata = TableMetadata(
                            project_id=self.config.project_id,
                            dataset_id=dataset.dataset_id,
                            table_id=table.table_id,
                            table_type=table_ref.table_type,
                            schema=schema,
                            created_time=table_ref.created,
                            last_modified_time=table_ref.modified,
                            partitioning=(
                                table_ref.time_partitioning.to_api_repr() 
                                if table_ref.time_partitioning 
                                else None
                            ),
                            clustering=table_ref.clustering_fields,
                            labels=table_ref.labels
                        )
                        # TODO Add more metadata fields here and optimise the output format
                        tables_metadata.append(metadata)
                        logger.debug(f"Successfully processed table: {table.table_id}")
                        
                    except Exception as e:
                        logger.error(f"Error processing table {table.table_id}: {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error collecting tables: {str(e)}")
        
        return tables_metadata

    def _process_table_schema(self, table_ref: bigquery.Table) -> SchemaMetadata:
        """
        Process BigQuery table schema into SchemaMetadata.
        
        Args:
            table_ref: BigQuery table reference
            
        Returns:
            SchemaMetadata: Processed schema information
        """
        try:
            columns = [
                self._process_bq_schema_field(field.to_api_repr())
                for field in table_ref.schema
            ]
            
            return SchemaMetadata(columns=columns)
            
        except Exception as e:
            logger.error(f"Error processing schema for table {table_ref.table_id}: {str(e)}")
            raise

    def _process_bq_schema_field(self, field: Dict[str, Any]) -> ColumnMetadata:
        """
        Process a single BigQuery schema field.
        
        Args:
            field: BigQuery schema field dictionary
            
        Returns:
            ColumnMetadata: Processed column information
        """
        nested_fields = []
        if 'fields' in field:
            nested_fields = [
                self._process_bq_schema_field(nested_field) 
                for nested_field in field.get('fields', [])
            ]
        
        policy_tags = []
        if 'policyTags' in field and isinstance(field['policyTags'], dict):
            policy_tags = field['policyTags'].get('names', [])
        
        return ColumnMetadata(
            name=field['name'],
            field_type=field['type'],
            description=field.get('description'),
            mode=field.get('mode', 'NULLABLE'),
            policy_tags=policy_tags,
            fields=nested_fields
        )

    def collect(self) -> MetadataCollection:
        """
        Collect all metadata from BigQuery.
        
        Returns:
            MetadataCollection: Complete collection of BigQuery metadata
        """
        logger.info(f"Starting metadata collection for project {self.config.project_id}")
        
        tables = self.collect_table_metadata()
        jobs = self.collect_job_metadata()
        
        return MetadataCollection(
            tables=tables,
            jobs=jobs
        )
    
    def close(self):
        """Close the BigQuery client connection."""
        self.client.close()
        self.client = None