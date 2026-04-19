"""Operational-DB adapters (Gold bulk-push targets).

The warehouse router fans out to every entry in adapters.operational_dbs
whose key is present in features.warehouse_router.targets.
"""
