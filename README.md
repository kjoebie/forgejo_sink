# DWH Spark Processing

Data Warehouse processing notebooks voor Apache Spark, compatibel met Microsoft Fabric Runtime 1.3.

## 🎯 Doel

Dit project maakt het mogelijk om Microsoft Fabric notebooks te testen en uit te voeren op een vanilla Apache Spark cluster. Het biedt een mock van `mssparkutils.notebook.run()` met Papermill, waardoor Fabric-achtige workflow orchestration mogelijk is.

## 🏗️ Architectuur

- **Spark Cluster**: 1 master + 3 workers (Spark 3.5.7)
- **Table Formats**: Delta Lake, Apache Iceberg support
- **Development**: VS Code Remote SSH
- **Orchestration**: Papermill voor notebook execution

## 📦 Tech Stack

- **Python**: 3.11 (compatible met Fabric Runtime 1.3)
- **Delta-Spark**: 3.2.1 (Delta Lake 3.2)
- **PyIceberg**: 0.10.0+
- **Papermill**: 2.6.0+ (notebook orchestration)
- **PyArrow**: 15.0.0 (compatible met Spark 3.5)

## 🚀 Setup

### Op Spark Cluster
```bash
# Clone repository
git clone https://git.qbids.net/albert/dwh_spark_processing.git
cd dwh_spark_processing

# Installeer dependencies met UV
uv sync

# Activeer environment
source .venv/bin/activate
```

### VS Code Remote SSH

1. Configureer SSH in `~/.ssh/config`:
```
Host spark-cluster
    HostName spark-master.qbids.net
    User sparkadmin
    IdentityFile ~/.ssh/id_ed25519_spark-master
```

2. In VS Code: `Ctrl+Shift+P` → "Remote-SSH: Connect to Host" → `spark-cluster`
3. Open folder: `/home/sparkadmin/source/repos/dwh_spark_processing`
4. Select Python interpreter: `.venv/bin/python`

## 📓 Gebruik

### Orchestrator Pattern (zoals Fabric)
```python
from modules.notebook_utils import mssparkutils
from datetime import datetime

# Roep worker notebook aan
result = mssparkutils.notebook.run(
    "01_process_data",
    timeout_seconds=3600,
    arguments={
        "source": "sales_data",
        "run_ts": datetime.now().isoformat(),
        "process_type": "bronze"
    }
)
```

### Spark Sessie
```python
from config.spark_config import create_spark_session

spark = create_spark_session(
    app_name="MyDataPipeline",
    master="spark://spark-master.qbids.net:7077"
)
```

## 📁 Project Structuur
```
dwh_spark_processing/
├── notebooks/           # Jupyter notebooks
│   ├── 00_orchestrator.ipynb
│   └── 01_process_data.ipynb
├── modules/            # Python modules
│   └── notebook_utils.py    # mssparkutils mock
├── config/             # Configuratie
│   └── spark_config.py      # Spark session setup
├── notebook_outputs/   # Uitgevoerde notebooks (gitignored)
└── pyproject.toml     # Dependencies
```

## ✅ Tests draaien

- Snelle checks: `python -m pytest tests -m "unit or integration"`
- Notebook-tests apart draaien: `python -m pytest tests -m notebook`

## 🔄 Workflow

1. **Feature branch**: `git checkout -b feature/my-feature`
2. **Develop**: Maak/wijzig notebooks en modules
3. **Test**: Voer uit op cluster via VS Code Remote SSH
4. **Commit**: `git commit -m "Description"`
5. **Push**: `git push origin feature/my-feature`
6. **Pull Request**: In Forgejo
7. **Merge**: Na review/test

⚠️ **Direct pushen naar `main` is disabled** - gebruik altijd Pull Requests.

## 🎓 Fabric Compatibility

Dit project bootst Microsoft Fabric Runtime 1.3 na:
- Python 3.11
- Spark 3.5.7
- Delta Lake 3.2
- PyArrow 15.x
- mssparkutils.notebook.run() API

## 📝 Licentie

Internal QBIDS project