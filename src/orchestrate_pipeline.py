"""
Orchestrator script for Chicago Crime Data Pipeline

This script runs the data pipeline stages in the correct order:
1. metadata_tracker (Initial) - Check baseline metadata and status
2. bronze_api_data_pull - Fetch raw data from API
3. metadata_tracker (Verify) - Verify and update metadata after bronze pull
4. silver_data_enhance - Clean and enrich raw data
5. silver_report_data_create - Create report period aggregations
6. gold_agg - Aggregate to gold layer
7. gold_dash_agg - Final dashboard aggregations
"""

import os
import sys
import time
import logging
import importlib.util
import argparse
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_execution.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define BASE_DIR using relative path or environment variable
SCRIPT_DIR = Path(__file__).parent.absolute()
BASE_DIR = os.getenv(
    "BASE_DIR",
    str(SCRIPT_DIR.parent)  # Navigate to project root (parent of src directory)
)

# Define pipeline stages
PIPELINE_STAGES = [
    {
        "name": "Metadata Tracker (Initial)",
        "module": "00_metadata_tracker",
        "description": "Check baseline metadata and status tracking files"
    },
    {
        "name": "Bronze API Data Pull",
        "module": "0_bronze_api_data_pull",
        "description": "Fetch raw crime data from Chicago API"
    },
    {
        "name": "Metadata Tracker (Verify)",
        "module": "00_metadata_tracker",
        "description": "Verify and update metadata after bronze data pull"
    },
    {
        "name": "Silver Data Enhancement",
        "module": "1_silver_data_enhance",
        "description": "Clean and enrich raw data"
    },
    {
        "name": "Silver Report Data Creation",
        "module": "2_silver_report_data_create",
        "description": "Create report period aggregations"
    },
    {
        "name": "Gold Aggregation",
        "module": "3_gold_agg",
        "description": "Aggregate to gold layer"
    },
    {
        "name": "Gold Dashboard Aggregation",
        "module": "4_gold_dash_agg",
        "description": "Final dashboard aggregations"
    }
]


def import_module_from_path(module_name, module_path):
    """
    Dynamically import a module from a file path.
    
    Args:
        module_name: Name to assign the module
        module_path: Full file path to the Python module
    
    Returns:
        The imported module
    """
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def run_stage(stage, src_dir, stage_args=None):
    """
    Run a single pipeline stage.
    
    Args:
        stage: Dictionary containing stage information
        src_dir: Path to src directory
        stage_args: List of command-line arguments to pass to the stage
    
    Returns:
        Tuple of (success: bool, execution_time: float)
    """
    stage_name = stage["name"]
    module_name = stage["module"]
    description = stage["description"]
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Starting: {stage_name}")
    logger.info(f"Description: {description}")
    if stage_args:
        logger.info(f"Arguments: {' '.join(stage_args)}")
    logger.info(f"{'='*70}")
    
    try:
        start_time = time.time()
        
        # Construct module path
        module_path = os.path.join(src_dir, f"{module_name}.py")
        
        if not os.path.exists(module_path):
            logger.error(f"Module file not found: {module_path}")
            return False, 0
        
        # Import and run the module
        logger.info(f"Loading module: {module_path}")
        module = import_module_from_path(module_name, module_path)
        
        # Execute main function if it exists
        if hasattr(module, 'main') and callable(module.main):
            logger.info(f"Executing main() from {module_name}")
            # Pass arguments by temporarily replacing sys.argv
            if stage_args:
                original_argv = sys.argv
                try:
                    sys.argv = [sys.argv[0]] + stage_args
                    module.main()
                finally:
                    sys.argv = original_argv
            else:
                module.main()
            # Flush stdout to ensure all print statements are displayed
            sys.stdout.flush()
            sys.stderr.flush()
        else:
            logger.warning(f"No main() function found in {module_name}")
        
        execution_time = time.time() - start_time
        logger.info(f"✓ {stage_name} completed successfully in {execution_time:.2f} seconds")
        
        return True, execution_time
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"✗ {stage_name} failed after {execution_time:.2f} seconds")
        logger.error(f"Error: {str(e)}", exc_info=True)
        return False, execution_time


def create_required_directories(base_dir):
    """
    Create all required data directories for the pipeline.
    
    Args:
        base_dir: Base directory path
    """
    required_dirs = [
        "data/raw_data",
        "data/raw_data/api_crime_data",
        "data/raw_data/gold_data",
        "data/raw_data/gold_data_dash",
        "data/silver_crime_data",
        "data/silver_report_period_crime_data",
        "data/gold_data",
        "data/gold_data_dash"
    ]
    
    for dir_path in required_dirs:
        full_path = os.path.join(base_dir, dir_path)
        if not os.path.exists(full_path):
            os.makedirs(full_path, exist_ok=True)
            logger.info(f"Created directory: {full_path}")


def run_pipeline(stages_to_run=None, skip_failed=False, rerun_silver=False):
    """
    Run the complete pipeline.
    
    Args:
        stages_to_run: List of stage indices to run (0-based). If None, runs all.
        skip_failed: If True, continue pipeline even if a stage fails.
        rerun_silver: If True, reprocess all silver and gold data (pass --rerun flag to stages 3-6).
    
    Returns:
        Dictionary with execution results
    """
    src_dir = os.path.join(BASE_DIR, "src")
    
    if not os.path.exists(src_dir):
        logger.error(f"Source directory not found: {src_dir}")
        return {"success": False, "error": "Source directory not found"}
    
    # Create required directories
    create_required_directories(BASE_DIR)
    
    logger.info(f"Starting Pipeline Execution")
    logger.info(f"Base Directory: {BASE_DIR}")
    logger.info(f"Source Directory: {src_dir}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    if rerun_silver:
        logger.info(f"Silver stages will be rerun (reprocessing all data)")
    
    # Determine which stages to run
    if stages_to_run is None:
        stages_to_run = list(range(len(PIPELINE_STAGES)))
    
    results = {
        "success": True,
        "start_time": datetime.now().isoformat(),
        "stages": {},
        "total_execution_time": 0,
        "failed_stages": []
    }
    
    pipeline_start = time.time()
    
    for idx, stage_idx in enumerate(stages_to_run):
        if stage_idx < 0 or stage_idx >= len(PIPELINE_STAGES):
            logger.warning(f"Invalid stage index: {stage_idx}")
            continue
        
        stage = PIPELINE_STAGES[stage_idx]
        
        # Determine if this stage should get special arguments
        stage_args = None
        if rerun_silver and stage_idx in [3, 4, 5, 6]:  # Silver stages (3-4), gold aggregation (5), and gold dashboard (6) support --rerun
            stage_args = ["--rerun"]
        
        success, execution_time = run_stage(stage, src_dir, stage_args=stage_args)
        
        results["stages"][stage["name"]] = {
            "success": success,
            "execution_time": execution_time
        }
        
        if not success:
            results["failed_stages"].append(stage["name"])
            results["success"] = False
            
            if not skip_failed:
                logger.error("Pipeline halted due to stage failure")
                break
            else:
                logger.warning("Continuing pipeline despite stage failure")
    
    total_execution_time = time.time() - pipeline_start
    results["total_execution_time"] = total_execution_time
    results["end_time"] = datetime.now().isoformat()
    
    # Log summary
    logger.info(f"\n{'='*70}")
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info(f"{'='*70}")
    logger.info(f"Total Execution Time: {total_execution_time:.2f} seconds ({total_execution_time/60:.2f} minutes)")
    logger.info(f"Stages Executed: {len(results['stages'])}")
    logger.info(f"Successful Stages: {len([s for s in results['stages'].values() if s['success']])}")
    logger.info(f"Failed Stages: {len(results['failed_stages'])}")
    
    if results["failed_stages"]:
        logger.warning(f"Failed Stages: {', '.join(results['failed_stages'])}")
    
    logger.info(f"Overall Status: {'SUCCESS' if results['success'] else 'FAILED'}")
    logger.info(f"{'='*70}\n")
    
    return results


def main():
    """Main entry point for the orchestrator."""
    parser = argparse.ArgumentParser(
        description="Orchestrate the Chicago Crime Data Pipeline"
    )
    parser.add_argument(
        "--stages",
        type=int,
        nargs="+",
        help="Specific stages to run (0-based indices). If not specified, runs all stages."
    )
    parser.add_argument(
        "--skip-failed",
        action="store_true",
        help="Continue pipeline execution even if a stage fails"
    )
    parser.add_argument(
        "--rerun-silver",
        action="store_true",
        help="Reprocess all silver and gold data (rerun stages 3-6). By default, existing files are skipped."
    )
    parser.add_argument(
        "--list-stages",
        action="store_true",
        help="List all available stages and exit"
    )
    
    args = parser.parse_args()
    
    # List stages if requested
    if args.list_stages:
        print("\nAvailable Pipeline Stages:")
        print("=" * 70)
        for idx, stage in enumerate(PIPELINE_STAGES):
            print(f"{idx}: {stage['name']}")
            print(f"   Module: {stage['module']}")
            print(f"   Description: {stage['description']}")
        print("=" * 70)
        return
    
    # Run pipeline
    results = run_pipeline(
        stages_to_run=args.stages,
        skip_failed=args.skip_failed,
        rerun_silver=args.rerun_silver
    )
    
    # Exit with appropriate code
    sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()
