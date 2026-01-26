"""
DBR Migration Demo Utilities Package
"""

from .dbr_test_helpers import (
    generate_test_data,
    create_breaking_change_sample,
    validate_dataframe_schema,
    compare_outputs,
    get_dbr_version,
    is_breaking_change_applicable,
    BreakingChangeTestResult,
    get_legacy_config_settings,
    apply_legacy_configs,
    generate_test_report
)

__all__ = [
    'generate_test_data',
    'create_breaking_change_sample',
    'validate_dataframe_schema',
    'compare_outputs',
    'get_dbr_version',
    'is_breaking_change_applicable',
    'BreakingChangeTestResult',
    'get_legacy_config_settings',
    'apply_legacy_configs',
    'generate_test_report'
]

__version__ = '1.0.0'
