# Microsoft Fabric Module Setup Guide

## Overview

This guide explains how to properly set up and use Python modules in Microsoft Fabric notebooks. It provides a **robust, future-proof solution** for module imports that works across different Fabric workspace and lakehouse configurations.

## The Problem

When running notebooks in Microsoft Fabric:
1. Modules must be stored in a Lakehouse Files section
2. The path to modules can vary between workspaces and lakehouses
3. You cannot use `get_base_path()` before importing modules (chicken-and-egg problem)
4. Manual `sys.path` manipulation is fragile and not future-proof

## The Solution: `fabric_bootstrap.py`

We provide a dedicated bootstrap utility that:
- ✅ **Automatically detects** module location using multiple strategies
- ✅ **Works across different** lakehouses and workspaces
- ✅ **Future-proof** with fallback mechanisms
- ✅ **Idempotent** - safe to call multiple times
- ✅ **Zero configuration** needed in most cases

---

## Quick Start

### 1. Upload Modules to Lakehouse

Upload your entire `modules/` folder to the Lakehouse Files section:

```
Lakehouse: <your_lakehouse>
  Files/
    code/                    # ← Recommended folder name
      modules/
        __init__.py
        logging_utils.py
        path_utils.py
        bronze_processor.py
        ... (all other modules)
```

**Note:** The folder name `code` is configurable, but we recommend using it as a standard.

### 2. Use in Fabric Notebooks

In **every Fabric notebook**, add this as the **first cell**:

```python
# First cell - Module Bootstrap
import sys
import os

# Simple, future-proof bootstrap
if os.path.exists('/lakehouse/default'):
    CODE_PATH = "/lakehouse/default/Files/code"
    if CODE_PATH not in sys.path:
        sys.path.insert(0, CODE_PATH)

# Now you can import modules
from modules.logging_utils import configure_logging
from modules.bronze_processor import process_bronze_table
# ... other imports
```

**That's it!** This simple pattern is:
- ✅ Future-proof (uses standard Fabric mount point)
- ✅ Works across all lakehouses attached to the notebook
- ✅ Easy to understand and maintain

---

## Advanced: Robust Bootstrap Module

For more complex scenarios (multiple lakehouses, workspace files, etc.), use the `fabric_bootstrap` module:

### Installation

The bootstrap module is at `modules/fabric_bootstrap.py`. Upload it along with your other modules.

### Usage in Notebooks

```python
# First cell - Advanced Bootstrap
from modules.fabric_bootstrap import ensure_module_path

# Auto-detect and configure module path
ensure_module_path()

# Now import your modules
from modules.logging_utils import configure_logging
from modules.bronze_processor import process_bronze_table
```

### Custom Module Folder

If you use a different folder name:

```python
from modules.fabric_bootstrap import ensure_module_path

# Use custom folder name
ensure_module_path(module_folder='my_custom_modules')
```

### Verbose Mode (Debugging)

To see detailed path detection:

```python
from modules.fabric_bootstrap import ensure_module_path

# Enable verbose output
ensure_module_path(verbose=True)
```

Output:
```
Searching for module path (folder: 'code')...
Detected environment: Fabric
Workspace info: {'workspace_name': 'MyWorkspace', ...}
✓ Added to sys.path: /lakehouse/default/Files/code
```

---

## How It Works

### Detection Strategies (in order)

The bootstrap module tries multiple strategies to find your modules:

1. **Fabric Default Lakehouse**: `/lakehouse/default/Files/<folder>`
2. **Environment Variable**: `$FABRIC_CODE_PATH` (if set)
3. **Common Fabric Locations**: `/lakehouse/default/<folder>`, `/workspace/Files/<folder>`
4. **Cluster OneLake Mount**: Searches `/data/lakehouse/**/Files/<folder>`
5. **Relative Path**: Local development fallback
6. **Parent Directory Search**: For notebooks in subdirectories

### Why This Is Future-Proof

1. **Multiple Fallbacks**: If Fabric changes paths, another strategy likely still works
2. **Standard Mount Points**: Uses documented Fabric mount points (`/lakehouse/default`)
3. **Environment Override**: Can be overridden via `FABRIC_CODE_PATH` if needed
4. **No Hardcoded Workspace/Lakehouse Names**: Works across different configurations

---

## Setup Instructions

### One-Time Lakehouse Setup

1. **Create Folder Structure** in your Lakehouse:
   ```
   Files/
     code/           # Or your preferred name
       modules/      # Your Python modules here
   ```

2. **Upload Modules**:
   - Option A: Use Fabric UI to upload files/folders
   - Option B: Use notebook to upload programmatically:
     ```python
     from notebookutils import mssparkutils

     # Upload from local file
     mssparkutils.fs.put(
         "Files/code/modules/logging_utils.py",
         local_file_content,
         True
     )
     ```
   - Option C: Use Azure Storage Explorer

3. **Verify Structure**:
   ```python
   from notebookutils import mssparkutils

   # List files in code folder
   files = mssparkutils.fs.ls("Files/code/modules")
   for file in files:
       print(file.name)
   ```

### Alternative: Workspace Files (Not Recommended)

You can also store modules in Workspace Files, but this has limitations:
- ❌ Cannot be easily shared across workspaces
- ❌ More complex path detection needed
- ❌ Not mounted by default in notebooks

**Recommendation**: Always use **Lakehouse Files** for module storage.

---

## Best Practices

### 1. **Use Lakehouse Files (Not Workspace Files)**

✅ **DO**: Store modules in Lakehouse Files
```
/lakehouse/default/Files/code/modules/
```

❌ **DON'T**: Store in Workspace Files (harder to maintain)

### 2. **Standard Folder Name**

Use `code` as your module folder for consistency across projects:
```
Files/code/modules/
```

### 3. **Single Source of Truth**

Store modules in **one central Lakehouse** and attach it to all notebooks that need it.

**Example Workflow**:
- Create "Common" lakehouse → Store modules there
- Attach "Common" lakehouse to all notebooks as secondary lakehouse
- Primary lakehouse = data-specific (e.g., "Bronze", "Silver")

### 4. **Version Control**

Keep your modules in Git and use CI/CD to deploy to Fabric:
```bash
# Example deployment script
az storage blob upload-batch \
  --destination "Files/code" \
  --source "./modules" \
  --account-name <storage_account>
```

### 5. **Environment-Specific Configuration**

Use environment variables for environment-specific settings:
```python
# Set in Fabric environment or notebook
import os
os.environ['FABRIC_CODE_PATH'] = '/lakehouse/default/Files/prod_code'

# Bootstrap will use this path
ensure_module_path()
```

---

## Alternatives Considered

### Alternative 1: Wheel/Requirements.txt (❌ Too Complex)

**Pros**:
- "Proper" Python packaging
- Version management

**Cons**:
- ❌ Requires building wheels for every change
- ❌ Complex deployment process
- ❌ Overkill for internal project modules
- ❌ Slower iteration during development

**Recommendation**: Only use wheels for **external/third-party libraries**, not internal modules.

### Alternative 2: Inline Code in Notebooks (❌ Not Maintainable)

**Pros**:
- No import needed

**Cons**:
- ❌ Code duplication across notebooks
- ❌ Nightmare to maintain
- ❌ No version control benefits

**Recommendation**: Never inline complex logic.

### Alternative 3: %run Magic Command (❌ Doesn't Work Well)

**Pros**:
- Simple syntax

**Cons**:
- ❌ Not recommended for Fabric Spark notebooks
- ❌ Breaks parallelization features
- ❌ Limited to notebook files, not Python modules

**Recommendation**: Avoid `%run` in Fabric notebooks.

---

## Troubleshooting

### Problem: "ModuleNotFoundError: No module named 'modules'"

**Solution 1**: Verify modules are uploaded
```python
from notebookutils import mssparkutils

# Check if code folder exists
files = mssparkutils.fs.ls("Files/code")
for f in files:
    print(f.name)
```

**Solution 2**: Verify sys.path
```python
import sys
print("Current sys.path:")
for path in sys.path:
    print(f"  {path}")
```

**Solution 3**: Use verbose bootstrap
```python
from modules.fabric_bootstrap import ensure_module_path
ensure_module_path(verbose=True)
```

### Problem: "FileNotFoundError: Could not find module folder 'code'"

**Cause**: Modules not uploaded or wrong folder name

**Solution**:
1. Upload modules to Lakehouse Files
2. Verify folder structure matches expected layout
3. If using custom folder name, specify it:
   ```python
   ensure_module_path(module_folder='your_folder_name')
   ```

### Problem: Modules work locally but not in Fabric

**Cause**: Relative imports or missing dependencies

**Solution**:
1. Ensure all imports are absolute (not relative)
2. Check for any local-only dependencies
3. Verify `__init__.py` exists in `modules/` folder

### Problem: Changes to modules not reflected in notebook

**Cause**: Python caches imported modules

**Solution**: Restart kernel
```python
from notebookutils import mssparkutils

# Restart Python kernel to reload modules
mssparkutils.session.restartPython()
```

---

## Migration Guide

### From Manual sys.path to Bootstrap

**Before** (manual approach):
```python
import sys
CODE_PATH = "/lakehouse/default/Files/code"
if CODE_PATH not in sys.path:
    sys.path.append(CODE_PATH)

from modules.logging_utils import configure_logging
```

**After** (recommended simple approach):
```python
# Same code! No changes needed if using standard path
import sys
import os

if os.path.exists('/lakehouse/default'):
    CODE_PATH = "/lakehouse/default/Files/code"
    if CODE_PATH not in sys.path:
        sys.path.insert(0, CODE_PATH)

from modules.logging_utils import configure_logging
```

**Or** (advanced bootstrap):
```python
from modules.fabric_bootstrap import ensure_module_path
ensure_module_path()

from modules.logging_utils import configure_logging
```

---

## FAQ

**Q: Do I need to copy modules to every Lakehouse?**
A: No! Attach one "Common" lakehouse containing modules to all your notebooks.

**Q: Can I use different module versions in different notebooks?**
A: Yes, use different folder names:
```python
ensure_module_path(module_folder='code_v1')  # Notebook A
ensure_module_path(module_folder='code_v2')  # Notebook B
```

**Q: Is the simple `sys.path.insert()` approach future-proof?**
A: Yes! The `/lakehouse/default` mount point is a documented Fabric feature and unlikely to change. Using `sys.path.insert(0, ...)` ensures your modules take priority.

**Q: Should I use wheels/packages instead?**
A: Only for external/third-party libraries. For internal project modules, direct file upload is simpler and more maintainable.

**Q: Can I use environment variables to configure the path?**
A: Yes, set `FABRIC_CODE_PATH`:
```python
import os
os.environ['FABRIC_CODE_PATH'] = '/custom/path/to/modules'
```

**Q: What if I have modules in multiple locations?**
A: Add multiple paths to sys.path:
```python
paths = [
    '/lakehouse/default/Files/code',
    '/lakehouse/default/Files/shared_modules'
]
for path in paths:
    if path not in sys.path:
        sys.path.insert(0, path)
```

---

## Recommended Approach Summary

### ✅ Recommended: Simple sys.path Bootstrap

**When to use**: Standard Fabric setup with modules in default lakehouse

```python
# First cell of every notebook
import sys
import os

if os.path.exists('/lakehouse/default'):
    CODE_PATH = "/lakehouse/default/Files/code"
    if CODE_PATH not in sys.path:
        sys.path.insert(0, CODE_PATH)
```

**Pros**:
- ✅ Simple and easy to understand
- ✅ Future-proof (uses standard Fabric mount)
- ✅ Works across all lakehouses
- ✅ No dependencies

### ✅ Advanced: fabric_bootstrap Module

**When to use**: Multiple lakehouses, complex setups, need debugging

```python
# First cell of every notebook
from modules.fabric_bootstrap import ensure_module_path
ensure_module_path()
```

**Pros**:
- ✅ Multiple fallback strategies
- ✅ Automatic detection
- ✅ Verbose mode for debugging
- ✅ Environment variable support

---

## References

Based on Microsoft Fabric best practices from 2025:

- [Using Custom Libraries in Microsoft Fabric Data Engineering](https://blog.gbrueckl.at/2025/06/using-custom-libraries-in-microsoft-fabric-data-engineering/)
- [Elevate Your Code: Creating Python Libraries Using Microsoft Fabric](https://milescole.dev/data-engineering/2025/03/26/Packaging-Python-Libraries-Using-Microsoft-Fabric.html)
- [Microsoft Spark Utilities (MSSparkUtils) for Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/microsoft-spark-utilities)
- [NotebookUtils for Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities)
- [Work with Python and R modules - Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/files/workspace-modules)
- [Manage Apache Spark libraries - Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/library-management)

---

## Conclusion

**For most use cases, the simple `sys.path.insert()` approach is sufficient and future-proof.**

The `/lakehouse/default` mount point is a standard Fabric feature that won't change. Your current approach is actually quite good! The bootstrap module adds robustness for edge cases but isn't strictly necessary for standard setups.

**Key Takeaway**: Store modules in Lakehouse Files under a standard folder (like `code/`), and use the simple bootstrap pattern at the top of every notebook.
