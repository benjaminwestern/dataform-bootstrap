"""
Main entry point for Dataform migration CLI.
Coordinates configuration parsing and migration execution.
"""

import sys
from typing import Optional, Sequence
from .config import CLIConfig
from .parser import create_parser, parse_comma_separated
from .handlers import MigrationHandler
from src.utils.logging import get_logger

logger = get_logger(__name__)

def run_cli(args: Optional[Sequence[str]] = None) -> int:
    """
    Execute CLI with provided arguments or system arguments.
    
    Args:
        args: Optional sequence of command-line arguments
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        parser = create_parser()
        parsed_args = parser.parse_args(args)
        
        config = CLIConfig(
            projects=parse_comma_separated(parsed_args.project),
            locations=parse_comma_separated(parsed_args.location),
            days_of_history=parsed_args.days,
            similarity_threshold=parsed_args.similarity_threshold,
            output_dir=parsed_args.output_dir,
            enable_incremental=parsed_args.enable_incremental,
            output_mode=parsed_args.output_mode
        )
        
        config.validate()
        handler = MigrationHandler(config)        
        status = handler.run()

        # TODO give more detailed output on success/failure
        if status == 0:
            logger.info("Migration completed successfully")
        else:
            logger.error("Migration failed")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        return 1

def main() -> None:
    """Command-line entry point."""
    sys.exit(run_cli())

if __name__ == "__main__":
    main()