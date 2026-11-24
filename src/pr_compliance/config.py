"""Runtime configuration, read once from environment variables (``.env`` in Docker)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """All tunables for the pipeline. Every field maps to an upper-case env var."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore", case_sensitive=False)

    # --- GitHub -------------------------------------------------------------------------
    github_token: SecretStr | None = Field(default=None, description="PAT or Actions token")
    github_api_url: str = "https://api.github.com"
    github_org: str = "home-assistant"
    repo_allowlist: Annotated[list[str], NoDecode] = Field(
        default_factory=list, description="Comma-separated repo names; empty means all repos"
    )
    max_prs_per_repo: int = Field(default=5000, ge=1, description="Safety valve per repo per run")
    lookback_days: int | None = Field(
        default=None, ge=1, description="Override the data interval (demo/backfill)"
    )

    # --- HTTP behaviour -----------------------------------------------------------------
    extract_max_workers: int = Field(default=8, ge=1, le=32)
    http_connect_timeout_s: float = 10.0
    http_read_timeout_s: float = 30.0
    retry_max_attempts: int = Field(default=5, ge=1)
    retry_max_wait_s: float = 60.0
    rate_limit_floor: int = Field(default=50, ge=0, description="Sleep until reset below this")
    rate_limit_max_sleep_s: float = Field(
        default=3900.0, description="Give up (raise) if a reset is further away than this"
    )

    # --- Storage ------------------------------------------------------------------------
    data_dir: Path = Path("/opt/airflow/data")

    # --- Compliance rules ---------------------------------------------------------------
    min_approvals: int = Field(default=1, ge=1)
    no_checks_passes: bool = False
    exclude_bots: bool = False

    # --- Data quality thresholds --------------------------------------------------------
    dq_freshness_hours: float = 48.0
    dq_min_prs: int = Field(default=1, ge=0, description="Fail when fewer PRs are loaded")
    dq_volume_drop_ratio: float = 0.5
    dq_max_quarantine_rate: float = 0.05
    dq_max_compliance_drift_pts: float = 20.0

    # --- Alerting -----------------------------------------------------------------------
    alert_email_to: str = "data-alerts@example.com"
    alert_email_from: str = "airflow@pr-compliance.local"
    airflow_base_url: str = "http://localhost:8080"

    @field_validator("repo_allowlist", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("lookback_days", mode="before")
    @classmethod
    def _empty_is_none(cls, value: object) -> object:
        return None if value == "" else value

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings (cached; tests build ``Settings`` directly)."""
    return Settings()
