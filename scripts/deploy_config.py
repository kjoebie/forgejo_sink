"""
Configuration Deployment Script

Purpose:
    Deploys configuration JSON files from the local repository to the
    Cluster Lakehouse storage. This ensures that the local cluster environment
    has access to the same configuration files as the Fabric environment.
"""

import os
import shutil
import sys
from pathlib import Path

# Add project root to sys.path to enable module imports
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root))

from modules.path_utils import get_base_path_filesystem
from modules.logging_utils import configure_logging
import logging

# Setup logging
configure_logging(run_name="deploy_config", enable_console_logging=True)
logger = logging.getLogger(__name__)

def deploy_definitions() -> None:
    """
    Deploy configuration definitions to the cluster filesystem.

    Purpose:
        Copies all JSON configuration files from the local `config/definitions` directory
        to the physical `Files/config/definitions` directory on the cluster. This relies
        on `path_utils` to resolve the correct physical root path for the environment.

    Returns:
        None

    Raises:
        FileNotFoundError: If the source configuration directory does not exist.
        OSError: If there are permission issues or errors during file copying.
    """
    # 1. Determine the source directory (Local repo config directory)
    source_dir = project_root / "config" / "definitions"
    
    if not source_dir.exists():
        logger.error(f"Source directory not found: {source_dir}")
        return

    # 2. Determine the destination (Physical 'Files' directory on the cluster)
    # We use the filesystem variant because we are performing OS operations (shutil), not Spark operations.
    files_root = get_base_path_filesystem()
    
    # The path expected by Notebook 03 is: Files/config/definitions
    # On the cluster this resolves to: /data/lakehouse/.../Files/config/definitions
    dest_dir = Path(files_root) / "config" / "definitions"

    logger.info(f"Starting deployment...")
    logger.info(f"Source (Repo):       {source_dir}")
    logger.info(f"Destination (Lake):  {dest_dir}")

    # 3. Create destination directory if it doesn't exist
    dest_dir.mkdir(parents=True, exist_ok=True)

    # 4. Copy files
    count = 0
    for json_file in source_dir.glob("*.json"):
        shutil.copy2(json_file, dest_dir)
        logger.info(f"   -> Copied: {json_file.name}")
        count += 1

    logger.info(f"Finished! {count} files deployed.")

if __name__ == "__main__":
    deploy_definitions()