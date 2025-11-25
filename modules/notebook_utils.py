"""
Notebook Utilities Module - Mock van mssparkutils voor vanilla Spark

Nabootst Microsoft Fabric's mssparkutils.notebook.run() functionaliteit
met Papermill voor vanilla Spark clusters.
"""
import papermill as pm
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


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
        
        print(f"📓 Executing notebook: {notebook_path_obj}")
        print(f"⚙️  Arguments: {arguments}")
        print(f"💾 Output: {output_path}")
        print("-" * 70)
        
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
            
            print("-" * 70)
            print(f"✅ Notebook succesvol uitgevoerd!")
            
            # Fabric-compatible resultaat
            result = {
                "status": "success",
                "output_notebook": str(output_path),
                "exit_value": None
            }
            
            return json.dumps(result)
            
        except pm.PapermillExecutionError as e:
            print("-" * 70)
            print(f"❌ Notebook executie mislukt!")
            print(f"Error: {str(e)}")
            
            error_result = {
                "status": "failed",
                "error": str(e),
                "output_notebook": str(output_path)
            }
            return json.dumps(error_result)
        
        except Exception as e:
            print("-" * 70)
            print(f"❌ Onverwachte error!")
            print(f"Error: {str(e)}")
            
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