"""
Fabric Module Bootstrap Utility

Provides robust, future-proof module path initialization for Microsoft Fabric notebooks.
Handles automatic detection of module locations across different environments.

Key Features:
- Automatic Fabric environment detection
- Multiple fallback strategies for module discovery
- Support for both lakehouse and workspace file storage
- Idempotent and safe to call multiple times
- Works in Fabric, Cluster, and Local environments

Usage in Fabric Notebooks:
    # First cell of any Fabric notebook
    from modules.fabric_bootstrap import ensure_module_path
    ensure_module_path()

    # Now you can import your modules
    from modules.logging_utils import configure_logging

Author: Albert @ QBIDS
Date: 2025-12-03
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, List

# Module-level logger
logger = logging.getLogger(__name__)

# Track if bootstrap has already run
_bootstrap_completed = False

# Default module folder name (can be overridden)
DEFAULT_MODULE_FOLDER = "code"


def _is_fabric_environment() -> bool:
    """
    Detect if running in Microsoft Fabric.

    Returns:
        bool: True if in Fabric environment
    """
    return os.path.exists('/lakehouse/default')


def _get_workspace_info() -> Optional[dict]:
    """
    Get workspace information using notebookutils (if available).

    Returns:
        dict: Workspace context information, or None if not available
    """
    try:
        from notebookutils import mssparkutils
        context = mssparkutils.runtime.context
        return {
            'workspace_name': getattr(context, 'workspaceName', None),
            'workspace_id': getattr(context, 'workspaceId', None),
            'lakehouse_name': getattr(context, 'lakehouseName', None),
            'notebook_name': getattr(context, 'notebookName', None),
        }
    except (ImportError, AttributeError) as e:
        logger.debug(f"Could not get workspace info: {e}")
        return None


def _find_module_paths(module_folder: str = DEFAULT_MODULE_FOLDER) -> List[str]:
    """
    Find all possible module paths using multiple detection strategies.

    Args:
        module_folder: Name of the folder containing modules (e.g., 'code')

    Returns:
        List[str]: List of candidate paths, ordered by priority
    """
    candidates = []

    # Strategy 1: Fabric default lakehouse mount point
    if _is_fabric_environment():
        fabric_path = f"/lakehouse/default/Files/{module_folder}"
        if os.path.exists(fabric_path):
            candidates.append(fabric_path)
            logger.debug(f"Found Fabric lakehouse path: {fabric_path}")

    # Strategy 2: Environment variable override
    env_code_path = os.getenv('FABRIC_CODE_PATH')
    if env_code_path and os.path.exists(env_code_path):
        candidates.append(env_code_path)
        logger.debug(f"Found environment variable path: {env_code_path}")

    # Strategy 3: Search common Fabric locations
    common_fabric_locations = [
        f"/lakehouse/default/Files/{module_folder}",
        f"/lakehouse/default/{module_folder}",
        f"/workspace/Files/{module_folder}",
    ]

    for location in common_fabric_locations:
        if os.path.exists(location) and location not in candidates:
            candidates.append(location)
            logger.debug(f"Found common Fabric location: {location}")

    # Strategy 4: Cluster environment (OneLake mount)
    cluster_root = "/data/lakehouse"
    if os.path.exists(cluster_root):
        # Search for Files/{module_folder} under any lakehouse
        for root, dirs, files in os.walk(cluster_root):
            if root.endswith(f"/Files/{module_folder}") or root.endswith(f"/Files"):
                code_path = os.path.join(root, module_folder) if not root.endswith(module_folder) else root
                if os.path.exists(code_path) and code_path not in candidates:
                    candidates.append(code_path)
                    logger.debug(f"Found cluster path: {code_path}")
                    break  # Stop after first match for performance

    # Strategy 5: Relative path (local development)
    relative_path = os.path.abspath(module_folder)
    if os.path.exists(relative_path) and relative_path not in candidates:
        candidates.append(relative_path)
        logger.debug(f"Found relative path: {relative_path}")

    # Strategy 6: Parent directory search (notebooks in subdirectory)
    current_dir = Path.cwd()
    for i in range(3):  # Search up to 3 levels up
        parent = current_dir.parents[i] if i < len(current_dir.parents) else None
        if parent:
            parent_code = parent / module_folder
            if parent_code.exists() and str(parent_code) not in candidates:
                candidates.append(str(parent_code))
                logger.debug(f"Found parent directory path: {parent_code}")

    return candidates


def ensure_module_path(
    module_folder: str = DEFAULT_MODULE_FOLDER,
    verbose: bool = False,
    force: bool = False
) -> str:
    """
    Ensure module path is in sys.path for imports to work.

    This function is idempotent and safe to call multiple times.
    It searches for the module folder using multiple strategies and
    adds the first found location to sys.path.

    Args:
        module_folder: Name of folder containing modules (default: 'code')
        verbose: If True, print detailed information about path detection
        force: If True, re-run bootstrap even if already completed

    Returns:
        str: The path that was added to sys.path

    Raises:
        FileNotFoundError: If no valid module path could be found

    Examples:
        >>> # In a Fabric notebook (first cell)
        >>> from modules.fabric_bootstrap import ensure_module_path
        >>> ensure_module_path()
        '/lakehouse/default/Files/code'

        >>> # With custom module folder
        >>> ensure_module_path(module_folder='my_modules')
        '/lakehouse/default/Files/my_modules'

        >>> # With verbose output
        >>> ensure_module_path(verbose=True)
        Searching for module path...
        ✓ Found module path: /lakehouse/default/Files/code
        Added to sys.path: /lakehouse/default/Files/code
        '/lakehouse/default/Files/code'
    """
    global _bootstrap_completed

    # Skip if already completed (unless forced)
    if _bootstrap_completed and not force:
        if verbose:
            logger.info("Bootstrap already completed, skipping...")
        # Find existing path in sys.path
        for path in sys.path:
            if module_folder in path and os.path.exists(path):
                return path
        # If we get here, bootstrap was marked complete but path not found
        # Fall through to re-run bootstrap

    if verbose:
        logger.info(f"Searching for module path (folder: '{module_folder}')...")
        env = "Fabric" if _is_fabric_environment() else "Local/Cluster"
        logger.info(f"Detected environment: {env}")

        workspace_info = _get_workspace_info()
        if workspace_info:
            logger.info(f"Workspace info: {workspace_info}")

    # Find all candidate paths
    candidates = _find_module_paths(module_folder)

    if not candidates:
        error_msg = (
            f"Could not find module folder '{module_folder}' in any expected location.\n"
            f"Searched locations:\n"
            f"  - /lakehouse/default/Files/{module_folder}\n"
            f"  - /workspace/Files/{module_folder}\n"
            f"  - $FABRIC_CODE_PATH (environment variable)\n"
            f"  - Cluster paths under /data/lakehouse\n"
            f"  - Relative path: {os.path.abspath(module_folder)}\n\n"
            f"Please ensure your modules are uploaded to the Lakehouse Files section.\n"
            f"See FABRIC_MODULE_SETUP.md for setup instructions."
        )
        raise FileNotFoundError(error_msg)

    # Use first candidate (highest priority)
    module_path = candidates[0]

    # Add to sys.path if not already present
    if module_path not in sys.path:
        sys.path.insert(0, module_path)
        if verbose:
            logger.info(f"✓ Added to sys.path: {module_path}")
    else:
        if verbose:
            logger.info(f"✓ Path already in sys.path: {module_path}")

    # Mark bootstrap as completed
    _bootstrap_completed = True

    if verbose and len(candidates) > 1:
        logger.info(f"Note: Found {len(candidates)} possible paths, using: {module_path}")
        logger.info(f"Other candidates: {candidates[1:]}")

    return module_path


def get_module_path(module_folder: str = DEFAULT_MODULE_FOLDER) -> Optional[str]:
    """
    Get the current module path without modifying sys.path.

    Args:
        module_folder: Name of folder containing modules

    Returns:
        str: The module path if found, None otherwise

    Examples:
        >>> path = get_module_path()
        >>> print(f"Modules are located at: {path}")
        Modules are located at: /lakehouse/default/Files/code
    """
    candidates = _find_module_paths(module_folder)
    return candidates[0] if candidates else None


def verify_module_path(module_folder: str = DEFAULT_MODULE_FOLDER) -> bool:
    """
    Verify that the module path exists and is accessible.

    Args:
        module_folder: Name of folder containing modules

    Returns:
        bool: True if module path exists and is in sys.path

    Examples:
        >>> if verify_module_path():
        ...     print("Modules are ready to import")
        ... else:
        ...     print("Module path not configured")
        Modules are ready to import
    """
    module_path = get_module_path(module_folder)
    if not module_path:
        return False

    # Check if path exists
    if not os.path.exists(module_path):
        return False

    # Check if path is in sys.path
    return module_path in sys.path


def reset_bootstrap():
    """
    Reset bootstrap state (mainly for testing).

    This does NOT remove paths from sys.path, it only resets
    the internal flag that tracks if bootstrap has run.
    """
    global _bootstrap_completed
    _bootstrap_completed = False


# Auto-bootstrap if imported in Fabric environment
# (can be disabled by setting environment variable DISABLE_AUTO_BOOTSTRAP=1)
if _is_fabric_environment() and not os.getenv('DISABLE_AUTO_BOOTSTRAP'):
    try:
        ensure_module_path(verbose=False)
        logger.debug("Auto-bootstrap completed successfully")
    except Exception as e:
        logger.warning(f"Auto-bootstrap failed: {e}")
        # Don't raise - let user call ensure_module_path() explicitly
