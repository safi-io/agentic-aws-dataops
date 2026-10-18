"""Validated runtime settings and centralized AWS client factories."""

from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
from typing import Any, Self

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Application settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    DATABASE_URL: str = Field(..., min_length=1)
    AWS_REGION: str = Field(default="us-east-1", min_length=1)
    AWS_PROFILE: str | None = Field(default=None, min_length=1)
    AWS_ACCESS_KEY_ID: str | None = Field(default=None, min_length=1)
    AWS_SECRET_ACCESS_KEY: SecretStr | None = None
    AWS_SESSION_TOKEN: SecretStr | None = None
    AWS_ENDPOINT_URL: str | None = Field(default=None, min_length=1)
    S3_BUCKET_NAME: str = Field(..., min_length=3)
    S3_BRONZE_PREFIX: str = Field(default="bronze/", min_length=1)
    S3_SILVER_PREFIX: str = Field(default="silver/", min_length=1)
    S3_QUARANTINE_PREFIX: str = Field(default="quarantine/", min_length=1)
    DECIDER_MODE: str = Field(default="http", min_length=1)
    STRANDS_DECIDER_API_KEY: SecretStr | None = None
    STRANDS_DECIDER_ENDPOINT: str | None = Field(default=None, min_length=1)
    STRANDS_DECIDER_MODEL_ID: str = Field(
        default="StrandsAgents/strands-decider-2B-hobson-v19",
        min_length=1,
    )
    DECIDER_TIMEOUT_SECONDS: float = Field(default=240.0, gt=0.0)
    BEDROCK_MODEL_ID: str = Field(
        default="anthropic.claude-3-haiku-20240307-v1:0",
        min_length=1,
    )
    ALERT_SNS_TOPIC_ARN: str | None = Field(default=None, min_length=1)
    MAX_NULL_PERCENTAGE: float = Field(default=0.15, ge=0.0, le=1.0)
    MAX_RETRY_COUNT: int = Field(default=3, ge=0)

    @field_validator(
        "AWS_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_ENDPOINT_URL",
        "STRANDS_DECIDER_ENDPOINT",
        "ALERT_SNS_TOPIC_ARN",
        mode="before",
    )
    @classmethod
    def _empty_string_to_none(cls, value: Any) -> Any:
        if isinstance(value, str) and value.strip() == "":
            return None
        return value

    @field_validator(
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "STRANDS_DECIDER_API_KEY",
        mode="before",
    )
    @classmethod
    def _empty_secret_to_none(cls, value: Any) -> Any:
        if isinstance(value, str) and value.strip() == "":
            return None
        if isinstance(value, SecretStr) and value.get_secret_value().strip() == "":
            return None
        return value

    @field_validator("DATABASE_URL")
    @classmethod
    def _validate_database_url(cls, value: str) -> str:
        normalized = value.strip()
        allowed_prefixes = ("postgresql://", "postgresql+", "postgres://")
        if not normalized.startswith(allowed_prefixes):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection URL")
        if normalized.startswith("postgres://"):
            return f"postgresql://{normalized.removeprefix('postgres://')}"
        return normalized

    @field_validator("DECIDER_MODE")
    @classmethod
    def _normalize_decider_mode(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"http", "cli"}:
            raise ValueError("DECIDER_MODE must be either http or cli")
        return normalized

    @field_validator("AWS_ENDPOINT_URL", "STRANDS_DECIDER_ENDPOINT")
    @classmethod
    def _validate_http_endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().rstrip("/")
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("endpoint URLs must start with http:// or https://")
        return normalized

    @field_validator(
        "S3_BRONZE_PREFIX",
        "S3_SILVER_PREFIX",
        "S3_QUARANTINE_PREFIX",
    )
    @classmethod
    def _normalize_s3_prefix(cls, value: str) -> str:
        normalized = value.strip().replace("\\", "/")
        if normalized == "":
            raise ValueError("S3 prefixes cannot be empty")
        if normalized.startswith("/"):
            raise ValueError("S3 prefixes must be relative keys, not absolute paths")
        while "//" in normalized:
            normalized = normalized.replace("//", "/")
        if "../" in normalized or normalized == ".." or normalized.startswith("../"):
            raise ValueError("S3 prefixes cannot contain parent directory segments")
        if not normalized.endswith("/"):
            normalized = f"{normalized}/"
        return normalized

    @model_validator(mode="after")
    def _validate_cross_field_constraints(self) -> Self:
        has_access_key = self.AWS_ACCESS_KEY_ID is not None
        has_secret_key = self.AWS_SECRET_ACCESS_KEY is not None
        if has_access_key != has_secret_key:
            raise ValueError(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be provided together"
            )
        if self.AWS_SESSION_TOKEN is not None and not (has_access_key and has_secret_key):
            raise ValueError(
                "AWS_SESSION_TOKEN requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            )

        prefixes = {
            "S3_BRONZE_PREFIX": self.S3_BRONZE_PREFIX,
            "S3_SILVER_PREFIX": self.S3_SILVER_PREFIX,
            "S3_QUARANTINE_PREFIX": self.S3_QUARANTINE_PREFIX,
        }
        seen: dict[str, str] = {}
        for name, prefix in prefixes.items():
            if prefix in seen:
                raise ValueError(f"{name} duplicates {seen[prefix]}: {prefix}")
            seen[prefix] = name

        bronze_prefix = self.S3_BRONZE_PREFIX
        for name, prefix in prefixes.items():
            if name == "S3_BRONZE_PREFIX":
                continue
            if prefix.startswith(bronze_prefix):
                raise ValueError(
                    f"{name} must not be nested under the immutable Bronze prefix"
                )

        if self.DECIDER_MODE == "http":
            if self.STRANDS_DECIDER_ENDPOINT is None:
                raise ValueError(
                    "STRANDS_DECIDER_ENDPOINT is required when DECIDER_MODE=http"
                )
            if self.STRANDS_DECIDER_API_KEY is None:
                raise ValueError(
                    "STRANDS_DECIDER_API_KEY is required when DECIDER_MODE=http"
                )
        if self.DECIDER_MODE == "cli" and self.STRANDS_DECIDER_MODEL_ID.strip() == "":
            raise ValueError("STRANDS_DECIDER_MODEL_ID is required when DECIDER_MODE=cli")

        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings instance."""

    env_file = os.environ.get("DATAOPS_ENV_FILE")
    if env_file is None:
        return Settings()
    path = Path(env_file).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.is_file():
        raise FileNotFoundError(f"Application environment file not found: {path}")
    return Settings(_env_file=path)


def clear_settings_cache() -> None:
    """Clear cached settings so tests can load a fresh environment."""

    get_settings.cache_clear()


def create_boto3_session(settings: Settings | None = None) -> boto3.Session:
    """Create a boto3 session from validated settings."""

    resolved = settings or get_settings()
    session_kwargs: dict[str, Any] = {"region_name": resolved.AWS_REGION}

    if resolved.AWS_PROFILE is not None:
        session_kwargs["profile_name"] = resolved.AWS_PROFILE

    if resolved.AWS_ACCESS_KEY_ID is not None:
        session_kwargs["aws_access_key_id"] = resolved.AWS_ACCESS_KEY_ID
        if resolved.AWS_SECRET_ACCESS_KEY is None:
            raise ValueError("AWS_SECRET_ACCESS_KEY is required when access key is set")
        session_kwargs["aws_secret_access_key"] = (
            resolved.AWS_SECRET_ACCESS_KEY.get_secret_value()
        )
        if resolved.AWS_SESSION_TOKEN is not None:
            session_kwargs["aws_session_token"] = resolved.AWS_SESSION_TOKEN.get_secret_value()

    return boto3.Session(**session_kwargs)


def _client_config(service_name: str) -> Config:
    base_config = Config(
        retries={"total_max_attempts": 3, "mode": "adaptive"},
        connect_timeout=5,
        read_timeout=10,
        max_pool_connections=50,
        user_agent_extra="agentic-aws-dataops/1.0",
    )
    if service_name == "s3":
        return base_config.merge(
            Config(
                signature_version="s3v4",
                s3={
                    "addressing_style": "path",
                    "us_east_1_regional_endpoint": "regional",
                },
            )
        )
    return base_config


def get_boto3_client(
    service_name: str,
    settings: Settings | None = None,
) -> BaseClient:
    """Create a boto3 client through the centralized application factory."""

    if not service_name or service_name.strip() == "":
        raise ValueError("service_name must be a non-empty boto3 service name")

    resolved = settings or get_settings()
    client_kwargs: dict[str, Any] = {
        "region_name": resolved.AWS_REGION,
        "config": _client_config(service_name),
    }
    if resolved.AWS_ENDPOINT_URL is not None:
        client_kwargs["endpoint_url"] = resolved.AWS_ENDPOINT_URL

    return create_boto3_session(resolved).client(service_name, **client_kwargs)


def get_s3_client(settings: Settings | None = None) -> BaseClient:
    """Return a configured S3 client."""

    return get_boto3_client("s3", settings=settings)


def get_bedrock_runtime_client(settings: Settings | None = None) -> BaseClient:
    """Return a configured Bedrock Runtime client."""

    return get_boto3_client("bedrock-runtime", settings=settings)
