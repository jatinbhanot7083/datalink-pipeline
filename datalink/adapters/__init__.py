"""Adapter layer — every external dependency lives behind a Protocol here.

Concrete implementations are selected by config at startup via
`datalink.adapters.factory.build_adapters(settings)`. Nothing in the pipeline
code imports a concrete adapter directly — only Protocols.
"""
