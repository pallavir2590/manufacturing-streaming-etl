"""Storage paths for every layer of the pipeline.

The same code runs in two places:
  * Azure Databricks  -> abfss://<container>@<account>.dfs.core.windows.net/manufacturing/...
  * A laptop / CI     -> <local folder>/<container>/manufacturing/...

Only the root changes, so notebooks, the local runner and the tests share one layout.
"""

from __future__ import annotations

from dataclasses import dataclass

CONTAINERS = ("landing", "bronze", "silver", "gold")


@dataclass(frozen=True)
class Paths:
    """Builds every path from one root template that contains '{container}'."""

    root_template: str

    @classmethod
    def for_adls(cls, storage_account: str) -> Paths:
        return cls(f"abfss://{{container}}@{storage_account}.dfs.core.windows.net/manufacturing")

    @classmethod
    def for_volume(cls, volume_path: str) -> Paths:
        """Unity Catalog volume, e.g. /Volumes/workspace/default/manufacturing (Databricks Free Edition)."""
        return cls(f"{volume_path.rstrip('/')}/{{container}}/manufacturing")

    @classmethod
    def for_local(cls, base_dir: str) -> Paths:
        return cls(f"{base_dir.rstrip('/')}/{{container}}/manufacturing")

    def path(self, container: str, suffix: str) -> str:
        if container not in CONTAINERS:
            raise ValueError(f"Unknown container '{container}'. Expected one of {CONTAINERS}")
        return f"{self.root_template.format(container=container)}/{suffix}"

    # ---- landing (raw JSON files dropped by the source systems) ----
    @property
    def landing_telemetry(self) -> str:
        return self.path("landing", "telemetry")

    @property
    def landing_production(self) -> str:
        return self.path("landing", "production")

    # ---- bronze ----
    @property
    def bronze_telemetry(self) -> str:
        return self.path("bronze", "telemetry")

    @property
    def bronze_production(self) -> str:
        return self.path("bronze", "production")

    # ---- silver ----
    @property
    def silver_telemetry(self) -> str:
        return self.path("silver", "telemetry")

    @property
    def silver_production(self) -> str:
        return self.path("silver", "production")

    @property
    def quarantine_telemetry(self) -> str:
        return self.path("silver", "quarantine/telemetry")

    @property
    def quarantine_production(self) -> str:
        return self.path("silver", "quarantine/production")

    def master(self, name: str) -> str:
        return self.path("silver", f"master/{name}")

    # ---- gold ----
    def gold(self, name: str) -> str:
        return self.path("gold", name)

    # ---- checkpoints: one per streaming query, never shared ----
    def checkpoint(self, name: str) -> str:
        return self.path("bronze", f"checkpoints/{name}")


def configure_adls_oauth(spark, dbutils, storage_account: str, tenant_id: str, client_id: str,
                         secret_scope: str = "manufacturing", secret_key: str = "adls-sp-secret") -> None:
    """Classic ABFS access with a service principal.

    The client secret is read from a Databricks secret scope at runtime; it is never
    written in code or committed to Git. Prefer a Unity Catalog external location
    (storage credential + managed identity) where your workspace supports it; then
    this function is not needed at all.
    """
    host = f"{storage_account}.dfs.core.windows.net"
    client_secret = dbutils.secrets.get(secret_scope, secret_key)
    spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{host}",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{host}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{host}", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{host}",
                   f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
