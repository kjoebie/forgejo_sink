"""
Test runtime context mock functionality.

Verifies that the mssparkutils.runtime.context mock works correctly
and provides essential context information for Fabric compatibility.
"""

import pytest
from modules.notebook_utils import get_mssparkutils, RuntimeContext


def test_runtime_context_has_required_fields():
    """Test that RuntimeContext has all required fields."""
    context = RuntimeContext()

    # Required fields
    assert hasattr(context, 'productType')
    assert hasattr(context, 'currentWorkspaceName')
    assert hasattr(context, 'defaultLakehouseName')
    assert hasattr(context, 'defaultLakehouseWorkspaceName')

    # Optional fields
    assert hasattr(context, 'currentNotebookName')
    assert hasattr(context, 'currentWorkspaceId')
    assert hasattr(context, 'defaultLakehouseId')
    assert hasattr(context, 'defaultLakehouseWorkspaceId')


def test_runtime_context_default_values():
    """Test that RuntimeContext has sensible default values."""
    context = RuntimeContext()

    assert context.productType == "Spark"
    assert context.currentWorkspaceName == "local_workspace"
    assert context.defaultLakehouseName == "local_lakehouse"
    assert context.defaultLakehouseWorkspaceName == "local_workspace"


def test_runtime_context_to_dict():
    """Test that RuntimeContext.to_dict() produces correct output."""
    context = RuntimeContext(
        productType="Fabric",
        currentWorkspaceName="gh_b_avd",
        defaultLakehouseName="lh_gh_bronze",
        defaultLakehouseWorkspaceName="gh_b_avd"
    )

    result = context.to_dict()

    assert isinstance(result, dict)
    assert result['productType'] == "Fabric"
    assert result['currentWorkspaceName'] == "gh_b_avd"
    assert result['defaultLakehouseName'] == "lh_gh_bronze"
    assert result['defaultLakehouseWorkspaceName'] == "gh_b_avd"


def test_mssparkutils_has_runtime():
    """Test that mssparkutils mock has runtime attribute."""
    mssparkutils = get_mssparkutils()

    assert hasattr(mssparkutils, 'runtime')
    assert hasattr(mssparkutils.runtime, 'context')


def test_mssparkutils_runtime_context_works():
    """Test that mssparkutils.runtime.context returns RuntimeContext."""
    mssparkutils = get_mssparkutils()
    context = mssparkutils.runtime.context

    assert isinstance(context, RuntimeContext)
    assert context.productType in ["Spark", "Fabric"]
    assert context.currentWorkspaceName is not None
    assert context.defaultLakehouseName is not None


def test_runtime_context_caching():
    """Test that runtime context is cached (property only evaluated once)."""
    mssparkutils = get_mssparkutils()

    # Get context twice
    context1 = mssparkutils.runtime.context
    context2 = mssparkutils.runtime.context

    # Should be the same object (cached)
    assert context1 is context2


def test_runtime_context_with_spark_session():
    """Test runtime context with SparkSession."""
    from modules.spark_session import get_or_create_spark_session

    spark = get_or_create_spark_session(app_name="Test_Runtime_Context")
    mssparkutils = get_mssparkutils(spark)

    context = mssparkutils.runtime.context

    assert isinstance(context, RuntimeContext)
    assert context.productType in ["Spark", "Fabric"]


def test_runtime_context_parses_cluster_path():
    """Test that runtime context can parse workspace/lakehouse from CLUSTER_FILES_ROOT."""
    from modules.constants import CLUSTER_FILES_ROOT
    from modules.notebook_utils import get_mssparkutils

    # Skip if not in cluster environment
    if not CLUSTER_FILES_ROOT or '/lakehouse/' not in CLUSTER_FILES_ROOT:
        pytest.skip("Not in cluster environment")

    mssparkutils = get_mssparkutils()
    context = mssparkutils.runtime.context

    # Should extract names from path like: /data/lakehouse/gh_b_avd/lh_gh_bronze/Files
    assert context.currentWorkspaceName != "local_workspace"
    assert context.defaultLakehouseName != "local_lakehouse"

    # Verify it extracted the correct values
    if 'gh_b_avd' in CLUSTER_FILES_ROOT:
        assert context.currentWorkspaceName == "gh_b_avd"
    if 'lh_gh_bronze' in CLUSTER_FILES_ROOT:
        assert context.defaultLakehouseName == "lh_gh_bronze"


def test_runtime_context_mimics_fabric_output():
    """Test that context output structure mimics real Fabric output."""
    mssparkutils = get_mssparkutils()
    context = mssparkutils.runtime.context
    context_dict = context.to_dict()

    # Check that essential fields from Fabric are present
    required_fabric_fields = [
        'productType',
        'currentWorkspaceName',
        'defaultLakehouseName',
        'defaultLakehouseWorkspaceName'
    ]

    for field in required_fabric_fields:
        assert field in context_dict, f"Missing required field: {field}"


def test_runtime_context_fabric_detection():
    """Test that context correctly identifies Fabric environment."""
    import os
    from modules.notebook_utils import MockRuntime

    # Mock Fabric environment check
    original_exists = os.path.exists

    def mock_exists(path):
        if path == '/lakehouse/default':
            return True
        return original_exists(path)

    # Temporarily patch os.path.exists
    os.path.exists = mock_exists
    try:
        runtime = MockRuntime()
        context = runtime._detect_context()

        # In Fabric, productType should be "Fabric"
        assert context.productType == "Fabric"
        assert context.currentWorkspaceName == "fabric_workspace"
        assert context.defaultLakehouseName == "fabric_lakehouse"
    finally:
        # Restore original
        os.path.exists = original_exists


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
