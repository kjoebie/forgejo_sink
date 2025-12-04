"""
Notebook Utilities Module - Mock van mssparkutils voor vanilla Spark

Nabootst Microsoft Fabric's mssparkutils functionaliteit voor vanilla Spark clusters:
- notebook.run() via Papermill
- fs.* file system operations via pathlib
"""
import json
import logging
import shutil
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union, List

import papermill as pm

from modules.logging_utils import configure_logging

logger = logging.getLogger(__name__)


class NotebookRunner:
    """
    Mock van mssparkutils.notebook voor vanilla Spark
    Compatibel met Fabric notebook.run() API
    """
    
    @staticmethod
    def run(
        notebook_path: str,
        timeout_seconds: int = 3600,
        arguments: Optional[Dict[str, Any]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> str:
        """
        Voer een notebook uit met parameters (zoals Fabric mssparkutils.notebook.run)

        Args:
            notebook_path: Pad naar notebook (relatief of absoluut, met of zonder .ipynb)
            timeout_seconds: Timeout in seconden
            arguments: Dictionary met parameters voor notebook
            output_dir: Optioneel pad om notebook outputs in te schrijven (voor tests/CI)
            
        Returns:
            JSON string met resultaat (compatible met Fabric format)
            
        Example:
            result = mssparkutils.notebook.run(
                "1. nb_load_bronze",
                timeout_seconds=3600,
                arguments={
                    "source": "sales_data",
                    "run_ts": "2024-11-24T23:00:00"
                }
            )
        """
        log_file = configure_logging(run_name="notebook_runner")

        output_base_dir = Path(output_dir) if output_dir else Path('notebook_outputs')
        output_base_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Notebook outputs directory: %s", output_base_dir.resolve())

        logger.info("Logbestanden worden weggeschreven naar: %s", log_file.resolve())

        # Converteer naar .ipynb pad als extensie ontbreekt
        if not notebook_path.endswith('.ipynb'):
            notebook_path = f"{notebook_path}.ipynb"
        
        # Converteer naar Path object
        notebook_path_obj = Path(notebook_path)
        
        # Als relatief pad, zoek in notebooks/ directory
        if not notebook_path_obj.is_absolute():
            notebook_path_obj = Path('notebooks') / notebook_path_obj
        
        # Valideer dat notebook bestaat
        if not notebook_path_obj.exists():
            raise FileNotFoundError(
                f"Notebook niet gevonden: {notebook_path_obj}\n"
                f"Zorg dat het notebook in de 'notebooks/' directory staat."
            )
        
        # Output notebook met timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_path_absolute = (
            output_base_dir / f"{notebook_path_obj.stem}_{timestamp}.ipynb"
        ).resolve()

        logger.info("📓 Executing notebook: %s", notebook_path_obj)
        logger.info("⚙️  Arguments: %s", arguments)
        logger.info("💾 Output: %s", output_path_absolute)
        logger.info("-" * 70)
        
        try:
            # Voer notebook uit met Papermill
            pm.execute_notebook(
                str(notebook_path_obj),
                str(output_path_absolute),
                parameters=arguments or {},
                kernel_name='python3',
                timeout=timeout_seconds,
                progress_bar=True
            )
            
            logger.info("-" * 70)
            logger.info("✅ Notebook succesvol uitgevoerd!")
            
            # Fabric-compatible resultaat
            result = {
                "status": "success",
                "output_notebook": str(output_path_absolute),
                "exit_value": None
            }
            
            return json.dumps(result)
            
        except pm.PapermillExecutionError as e:
            logger.error("-" * 70)
            logger.exception("❌ Notebook executie mislukt!")

            error_result = {
                "status": "failed",
                "error": traceback.format_exc(),
                "output_notebook": str(output_path_absolute)
            }
            return json.dumps(error_result)

        except Exception as e:
            logger.error("-" * 70)
            logger.exception("❌ Onverwachte error!")

            error_result = {
                "status": "failed",
                "error": traceback.format_exc(),
                "output_notebook": None
            }
            return json.dumps(error_result)


@dataclass
class FileInfo:
    """
    File information object compatible with Fabric's mssparkutils.fs.ls() output.
    """
    name: str
    path: str
    size: int
    modificationTime: int  # milliseconds since epoch
    isDir: bool
    isFile: bool


class MockFileSystem:
    """
    Mock van mssparkutils.fs voor vanilla Spark
    Gebruikt pathlib en path_utils voor environment-aware file operations
    """

    def __init__(self, spark=None):
        """
        Args:
            spark: Optional SparkSession for path resolution
        """
        self.spark = spark

    def _resolve_path(self, path: str) -> Path:
        """
        Resolve Fabric-style path to absolute local path.

        Args:
            path: Fabric-style path (e.g., "Files/config/foo.json")

        Returns:
            Path: Absolute local path
        """
        # Import here to avoid circular dependency
        from modules.path_utils import resolve_files_path

        resolved = resolve_files_path(path, self.spark)
        return Path(resolved)

    def put(self, path: str, content: str, overwrite: bool = False) -> None:
        """
        Write content to a file.

        Args:
            path: File path (Fabric-style, e.g., "Files/config/foo.json")
            content: String content to write
            overwrite: If True, overwrite existing file; if False, raise error if exists

        Raises:
            FileExistsError: If file exists and overwrite=False

        Example:
            mssparkutils.fs.put("Files/config/metadata.json", json_string, True)
        """
        file_path = self._resolve_path(path)

        # Check if file exists and overwrite is False
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {file_path}")

        # Create parent directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content
        file_path.write_text(content, encoding='utf-8')
        logger.debug(f"fs.put: Wrote {len(content)} bytes to {file_path}")

    def read(self, path: str) -> str:
        """
        Read content from a file.

        Args:
            path: File path (Fabric-style)

        Returns:
            str: File content

        Raises:
            FileNotFoundError: If file does not exist

        Example:
            content = mssparkutils.fs.read("Files/config/metadata.json")
        """
        file_path = self._resolve_path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if not file_path.is_file():
            raise IsADirectoryError(f"Path is a directory, not a file: {file_path}")

        content = file_path.read_text(encoding='utf-8')
        logger.debug(f"fs.read: Read {len(content)} bytes from {file_path}")
        return content

    def mkdirs(self, path: str) -> None:
        """
        Create directory and all parent directories.

        Args:
            path: Directory path (Fabric-style)

        Example:
            mssparkutils.fs.mkdirs("Files/config/new_folder")
        """
        dir_path = self._resolve_path(path)
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"fs.mkdirs: Created directory {dir_path}")

    def rm(self, path: str, recurse: bool = False) -> None:
        """
        Remove file or directory.

        Args:
            path: Path to remove (Fabric-style)
            recurse: If True, remove directory recursively; if False, only remove empty dirs

        Raises:
            FileNotFoundError: If path does not exist
            OSError: If trying to remove non-empty directory without recurse=True

        Example:
            mssparkutils.fs.rm("Files/temp/old_file.json")
            mssparkutils.fs.rm("Files/temp", recurse=True)
        """
        file_path = self._resolve_path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"Path not found: {file_path}")

        if file_path.is_file():
            file_path.unlink()
            logger.debug(f"fs.rm: Removed file {file_path}")
        elif file_path.is_dir():
            if recurse:
                shutil.rmtree(file_path)
                logger.debug(f"fs.rm: Removed directory recursively {file_path}")
            else:
                file_path.rmdir()  # Raises OSError if not empty
                logger.debug(f"fs.rm: Removed empty directory {file_path}")

    def ls(self, path: str) -> List[FileInfo]:
        """
        List files and directories in a path.

        Args:
            path: Directory path (Fabric-style)

        Returns:
            List[FileInfo]: List of file information objects

        Raises:
            FileNotFoundError: If path does not exist
            NotADirectoryError: If path is not a directory

        Example:
            files = mssparkutils.fs.ls("Files/config")
            for f in files:
                print(f"{f.name}: {f.size} bytes")
        """
        dir_path = self._resolve_path(path)

        if not dir_path.exists():
            raise FileNotFoundError(f"Path not found: {dir_path}")

        if not dir_path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {dir_path}")

        result = []
        for item in dir_path.iterdir():
            stat = item.stat()

            # Convert modification time to milliseconds since epoch (Fabric format)
            mod_time_ms = int(stat.st_mtime * 1000)

            file_info = FileInfo(
                name=item.name,
                path=str(item),
                size=stat.st_size if item.is_file() else 0,
                modificationTime=mod_time_ms,
                isDir=item.is_dir(),
                isFile=item.is_file()
            )
            result.append(file_info)

        logger.debug(f"fs.ls: Listed {len(result)} items in {dir_path}")
        return result


@dataclass
class RuntimeContext:
    """
    Mock of mssparkutils.runtime.context for Fabric compatibility.

    Provides essential runtime context information similar to Fabric notebooks.
    Only includes fields that are available in cluster/local environments.
    """
    productType: str = "Spark"  # "Fabric" in real Fabric, "Spark" in cluster
    currentWorkspaceName: str = "local_workspace"
    defaultLakehouseName: str = "local_lakehouse"
    defaultLakehouseWorkspaceName: str = "local_workspace"
    currentNotebookName: Optional[str] = None
    currentWorkspaceId: Optional[str] = None
    defaultLakehouseId: Optional[str] = None
    defaultLakehouseWorkspaceId: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format similar to Fabric output."""
        return {
            'productType': self.productType,
            'currentWorkspaceName': self.currentWorkspaceName,
            'defaultLakehouseName': self.defaultLakehouseName,
            'defaultLakehouseWorkspaceName': self.defaultLakehouseWorkspaceName,
            'currentNotebookName': self.currentNotebookName,
            'currentWorkspaceId': self.currentWorkspaceId,
            'defaultLakehouseId': self.defaultLakehouseId,
            'defaultLakehouseWorkspaceId': self.defaultLakehouseWorkspaceId,
        }


class MockRuntime:
    """
    Mock of mssparkutils.runtime for Fabric compatibility.
    """
    def __init__(self, spark=None):
        """
        Args:
            spark: Optional SparkSession for environment detection
        """
        self.spark = spark
        self._context = None

    @property
    def context(self) -> RuntimeContext:
        """
        Get runtime context information.

        Returns:
            RuntimeContext: Context information object

        Example:
            >>> from modules.notebook_utils import get_mssparkutils
            >>> mssparkutils = get_mssparkutils(spark)
            >>> context = mssparkutils.runtime.context
            >>> print(context.productType)
            Spark
            >>> print(context.to_dict())
            {'productType': 'Spark', 'currentWorkspaceName': 'local_workspace', ...}
        """
        if self._context is None:
            self._context = self._detect_context()
        return self._context

    def _detect_context(self) -> RuntimeContext:
        """
        Detect runtime context based on environment.

        Returns:
            RuntimeContext: Detected context information
        """
        # Import here to avoid circular dependency
        from modules.path_utils import detect_environment
        from modules.constants import CLUSTER_FILES_ROOT

        env = detect_environment(self.spark)

        if env == 'fabric':
            # In real Fabric, this would be populated by Fabric itself
            # For mock, we return minimal Fabric-like context
            return RuntimeContext(
                productType="Fabric",
                currentWorkspaceName="fabric_workspace",
                defaultLakehouseName="fabric_lakehouse",
                defaultLakehouseWorkspaceName="fabric_workspace"
            )
        else:
            # Cluster/local environment
            # Try to extract lakehouse name from CLUSTER_FILES_ROOT
            workspace_name = "local_workspace"
            lakehouse_name = "local_lakehouse"

            if CLUSTER_FILES_ROOT and '/lakehouse/' in CLUSTER_FILES_ROOT:
                # Parse path like: /data/lakehouse/gh_b_avd/lh_gh_bronze/Files
                parts = CLUSTER_FILES_ROOT.split('/')
                try:
                    lakehouse_idx = parts.index('lakehouse')
                    if lakehouse_idx + 2 < len(parts):
                        workspace_name = parts[lakehouse_idx + 1]  # gh_b_avd
                        lakehouse_name = parts[lakehouse_idx + 2]  # lh_gh_bronze
                except (ValueError, IndexError):
                    pass

            return RuntimeContext(
                productType="Spark",
                currentWorkspaceName=workspace_name,
                defaultLakehouseName=lakehouse_name,
                defaultLakehouseWorkspaceName=workspace_name
            )


class MockMSSparkUtils:
    """
    Mock van mssparkutils voor Microsoft Fabric compatibility

    Gebruik:
        from modules.notebook_utils import mssparkutils

        # Notebook operations
        result = mssparkutils.notebook.run(
            "my_notebook",
            timeout_seconds=3600,
            arguments={"param1": "value1"}
        )

        # File system operations
        mssparkutils.fs.put("Files/config/metadata.json", json_string, True)
        content = mssparkutils.fs.read("Files/config/metadata.json")
        files = mssparkutils.fs.ls("Files/config")

        # Runtime context
        context = mssparkutils.runtime.context
        print(f"Running in: {context.productType}")
        print(f"Workspace: {context.currentWorkspaceName}")
        print(f"Lakehouse: {context.defaultLakehouseName}")
    """
    def __init__(self, spark=None):
        """
        Args:
            spark: Optional SparkSession for path resolution in fs operations
        """
        self.notebook = NotebookRunner()
        self.fs = MockFileSystem(spark)
        self.runtime = MockRuntime(spark)


def get_mssparkutils(spark=None):
    """
    Factory function to get mssparkutils instance with Spark session.

    Args:
        spark: SparkSession for path resolution

    Returns:
        MockMSSparkUtils: Configured instance

    Example:
        from modules.notebook_utils import get_mssparkutils
        mssparkutils = get_mssparkutils(spark)
    """
    return MockMSSparkUtils(spark)


# Global instance - gebruik zoals in Fabric (zonder Spark context)
# Voor gebruik met Spark, gebruik get_mssparkutils(spark)
mssparkutils = MockMSSparkUtils()