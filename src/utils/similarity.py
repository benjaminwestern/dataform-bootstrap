"""
Utility functions for SQL query similarity analysis.
"""

from difflib import SequenceMatcher
from typing import Optional
import re
from dataclasses import dataclass

@dataclass
class QuerySimilarityConfig:
    """Configuration for query similarity analysis."""
    ignore_case: bool = True
    ignore_whitespace: bool = True
    ignore_comments: bool = True
    min_length: int = 10

def normalise_query(
    query: str,
    config: Optional[QuerySimilarityConfig] = None
) -> str:
    """
    Normalise SQL query for comparison.
    
    Args:
        query: SQL query string to normalise
        config: Optional configuration for normalisation
        
    Returns:
        Normalised query string
    """
    if config is None:
        config = QuerySimilarityConfig()
    
    # TODO Bad REGEX is bad, but it's good enough for now :) - comments are here so I can find this later
    # Remove comments
    if config.ignore_comments:
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)  # Single line comments
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)  # Multi-line comments
    
    if config.ignore_whitespace:
        query = ' '.join(query.split())
    
    if config.ignore_case:
        query = query.lower()
    
    return query

def calculate_similarity(
    query1: str,
    query2: str,
    config: Optional[QuerySimilarityConfig] = None
) -> float:
    """
    Calculate similarity ratio between two SQL queries.
    
    Args:
        query1: First SQL query
        query2: Second SQL query
        config: Optional configuration for similarity calculation
        
    Returns:
        Similarity ratio between 0 and 1
    """
    if config is None:
        config = QuerySimilarityConfig()
    
    # TODO this is a very basic implementation, I really should be using a proper SQL parser, but this is good enough for now

    # Normalise queries
    norm_query1 = normalise_query(query1, config)
    norm_query2 = normalise_query(query2, config)
    
    # Check minimum length requirement
    if len(norm_query1) < config.min_length or len(norm_query2) < config.min_length:
        return 0.0
    
    # Calculate similarity ratio
    return SequenceMatcher(None, norm_query1, norm_query2).ratio()

def find_similar_queries(
    target_query: str,
    query_list: list[str],
    threshold: float = 0.9,
    config: Optional[QuerySimilarityConfig] = None
) -> list[tuple[int, float]]:
    """
    Find similar queries in a list of queries.
    
    Args:
        target_query: Query to compare against
        query_list: List of queries to search
        threshold: Minimum similarity threshold
        config: Optional configuration for similarity calculation
        
    Returns:
        List of tuples containing (query_index, similarity_ratio)
    """
    similar_queries = []
    
    for idx, query in enumerate(query_list):
        similarity = calculate_similarity(target_query, query, config)
        if similarity >= threshold:
            similar_queries.append((idx, similarity))
    
    return sorted(similar_queries, key=lambda x: x[1], reverse=True)