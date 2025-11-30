"""
Notebook Utilities Module - Mock van mssparkutils voor vanilla Spark

Nabootst Microsoft Fabric's mssparkutils.notebook.run() functionaliteit
met Papermill voor vanilla Spark clusters.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import papermill as pm

logger = logging.getLogger(__name__)

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


class NotebookRunner:
    """
    Mock van mssparkutils.notebook voor vanilla Spark
    Compatibel met Fabric notebook.run() API
    """
    
    @staticmethod
    def run(
        notebook_path: str, 
        timeout_seconds: int = 3600,
        arguments: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Voer een notebook uit met parameters (zoals Fabric mssparkutils.notebook.run)
        
        Args:
            notebook_path: Pad naar notebook (relatief of absoluut, met of zonder .ipynb)
            timeout_seconds: Timeout in seconden
            arguments: Dictionary met parameters voor notebook
            
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
        output_dir = Path('notebook_outputs')
        output_dir.mkdir(exist_ok=True)
        
        output_path = output_dir / f"{notebook_path_obj.stem}_{timestamp}.ipynb"
        
        logger.info("📓 Executing notebook: %s", notebook_path_obj)
        logger.info("⚙️  Arguments: %s", arguments)
        logger.info("💾 Output: %s", output_path)
        logger.info("-" * 70)
        
        try:
            # Voer notebook uit met Papermill
            pm.execute_notebook(
                str(notebook_path_obj),
                str(output_path),
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
                "output_notebook": str(output_path),
                "exit_value": None
            }
            
            return json.dumps(result)
            
        except pm.PapermillExecutionError as e:
            logger.error("-" * 70)
            logger.exception("❌ Notebook executie mislukt!")
            
            error_result = {
                "status": "failed",
                "error": str(e),
                "output_notebook": str(output_path)
            }
            return json.dumps(error_result)
        
        except Exception as e:
            logger.error("-" * 70)
            logger.exception("❌ Onverwachte error!")

            error_result = {
                "status": "failed",
                "error": str(e),
                "output_notebook": None
            }
            return json.dumps(error_result)


class MockMSSparkUtils:
    """
    Mock van mssparkutils voor Microsoft Fabric compatibility
    
    Gebruik:
        from modules.notebook_utils import mssparkutils
        
        result = mssparkutils.notebook.run(
            "my_notebook",
            timeout_seconds=3600,
            arguments={"param1": "value1"}
        )
    """
    def __init__(self):
        self.notebook = NotebookRunner()


# Global instance - gebruik zoals in Fabric
mssparkutils = MockMSSparkUtils()
