import os
from dataclasses import dataclass


@dataclass
class AppConfig:
    """Application configuration loaded from environment variables.

    All field names are generic so that this demo can be reused in different
    environments without leaking any business context.
    """

    # Basic metadata
    service_name: str = os.getenv("APP_SERVICE_NAME", "autotest-open-demo")
    env: str = os.getenv("APP_ENV", "dev")
    region: str = os.getenv("APP_REGION", "local")

    # Gateway TCP server settings
    gateway_host: str = os.getenv("APP_GATEWAY_HOST", "127.0.0.1")
    gateway_port: int = int(os.getenv("APP_GATEWAY_PORT", "9000"))

    # Optional HTTP control plane
    control_host: str = os.getenv("APP_CONTROL_HOST", "127.0.0.1")
    control_port: int = int(os.getenv("APP_CONTROL_PORT", "8000"))

    # Heartbeat and timeouts (seconds)
    heartbeat_interval: float = float(os.getenv("APP_HEARTBEAT_INTERVAL", "30.0"))
    client_read_timeout: float = float(os.getenv("APP_CLIENT_READ_TIMEOUT", "60.0"))

    # Simple demo access token used during the initial handshake.
    # In a real system this should be replaced with proper authentication.
    access_token: str = os.getenv("APP_ACCESS_TOKEN", "demo-token")


def load_config() -> AppConfig:
    """Return a fresh AppConfig instance.

    A function is used instead of a module-level singleton to make it easier
    to override configuration in tests or custom entrypoints.
    """

    return AppConfig()
