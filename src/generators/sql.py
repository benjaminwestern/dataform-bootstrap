"""
SQL file generation from BigQuery job queries.
Handles deduplication and file writing based on similarity.
Supports logging of deduplication decisions.
"""

from typing import List, Dict
import json

from ..models.metadata import JobMetadata
from ..models.config import OutputConfig
from ..utils.similarity import calculate_similarity
from ..utils.logging import get_logger

logger = get_logger(__name__)

class SQLGenerator:
    """Generates SQL files from BigQuery job queries."""
    
    def __init__(self, output_config: OutputConfig, similarity_threshold: float = 0.9):
        self.output_config = output_config
        self.similarity_threshold = similarity_threshold
    
    def deduplicate_queries(self, jobs: List[JobMetadata]) -> List[Dict]:
        """Deduplicate similar queries and track decisions."""
        unique_queries = []
        
        for job in jobs:
            if not job.query:
                continue
                
            is_unique = True
            similar_queries = []
            
            for existing in unique_queries:
                similarity = calculate_similarity(job.query, existing['query'])
                if similarity >= self.similarity_threshold:
                    is_unique = False
                    similar_queries.append({
                        'job_id': existing['job_id'],
                        'similarity': similarity
                    })
            
            if is_unique:
                unique_queries.append({
                    'query': job.query,
                    'job_id': job.job_id,
                    'created_time': job.created_time
                })
            
            # Log the decision
            if similar_queries:
                self._log_deduplication_decision(job, similar_queries)
        
        return unique_queries
    
    def _log_deduplication_decision(self, job: JobMetadata, similar_queries: List[Dict]):
        """Log query deduplication decisions."""
        if not job.destination_table:
            return
            
        log_path = self.output_config.logs_dir / f"{job.destination_table['datasetId']}_{job.destination_table['tableId']}_choices.ndjson"
        
        decision = {
            'job_id': job.job_id,
            'created_time': job.created_time,
            'similar_queries': similar_queries,
            'reason': f"Query similar to existing queries with similarity >= {self.similarity_threshold}"
        }
        
        try:
            with open(log_path, 'a') as f:
                f.write(json.dumps(decision, default=str) + '\n')
        except Exception as e:
            logger.error(f"Error logging deduplication decision: {str(e)}")
    
    def generate_sql_files(self, jobs: List[JobMetadata]):
        """Generate SQL files from unique queries."""

        jobs_by_table = {}
        for job in jobs:
            if job.destination_table:
                key = (job.destination_table['datasetId'], job.destination_table['tableId'])
                if key not in jobs_by_table:
                    jobs_by_table[key] = []
                jobs_by_table[key].append(job)
        

        for (dataset_id, table_id), table_jobs in jobs_by_table.items():
            unique_queries = self.deduplicate_queries(table_jobs)    
                    
            # TODO Add tests to this, to ensure that I am not unintentionally overwriting actions or missing operations
            if unique_queries:
                latest_query = max(unique_queries, key=lambda x: x['created_time'])
                sql_path = self.output_config.definitions_dir / dataset_id / f"{table_id}.sql"

                try:
                    sql_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(sql_path, 'w') as f:
                        f.write(latest_query['query'])
                    logger.info(f"Generated SQL file: {sql_path}")
                except Exception as e:
                    logger.error(f"Error writing SQL file {sql_path}: {str(e)}")