# In clients/__init__.py
class CriticalConnectorError(Exception):
    """Indicates a critical, non-recoverable error within a connector for a target."""
    pass