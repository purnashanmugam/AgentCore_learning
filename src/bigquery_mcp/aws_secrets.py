"""Utilities for retrieving Google Cloud credentials from AWS Secrets Manager."""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .exceptions import CredentialRetrievalError


class SecretsManagerProvider:
    """Thin wrapper around boto3 for fetching secrets with retries."""

    def __init__(self, *, region: str, endpoint_url: str | None = None) -> None:
        self._client = boto3.client("secretsmanager", region_name=region, endpoint_url=endpoint_url)

    def fetch_service_account(self, secret_name: str) -> dict[str, Any]:
        """Return the service-account JSON payload stored in the given secret."""

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self._client.get_secret_value(SecretId=secret_name)
                payload = response.get("SecretString")
                if payload is None:
                    secret_binary = response.get("SecretBinary")
                    if secret_binary is None:
                        raise CredentialRetrievalError("Secret did not contain a value")
                    payload = base64.b64decode(secret_binary).decode("utf-8")

                data = json.loads(payload)
                if not isinstance(data, dict):
                    raise CredentialRetrievalError(
                        "Expected JSON object for service-account secret"
                    )
                # Never log or expose the secret contents
                return data
            except ClientError as exc:
                last_error = exc
                error_code = exc.response.get("Error", {}).get("Code", "")
                if error_code in {"ThrottlingException", "TooManyRequestsException"} and attempt < 2:
                    backoff = 2**attempt * 0.5
                    time.sleep(backoff)
                    continue
                raise CredentialRetrievalError(
                    f"Unable to retrieve secret '{secret_name}': {error_code or 'unknown error'}"
                ) from exc
            except json.JSONDecodeError as exc:
                raise CredentialRetrievalError(
                    "Secret payload is not valid JSON"
                ) from exc
        if last_error:
            raise CredentialRetrievalError(
                f"Failed to retrieve secret '{secret_name}' after retries"
            ) from last_error
        raise CredentialRetrievalError(
            f"Failed to retrieve secret '{secret_name}' after retries"
        )
