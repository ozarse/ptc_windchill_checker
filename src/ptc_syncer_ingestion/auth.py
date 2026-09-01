"""Keyring-based credential management for Windchill API."""

from __future__ import annotations

import click
import keyring

SERVICE_NAME = "oneplm_ingestion"


def store_credentials(username: str, password: str) -> None:
    """Store credentials in the system keyring."""
    keyring.set_password(SERVICE_NAME, "username", username)
    keyring.set_password(SERVICE_NAME, username, password)


def get_credentials() -> tuple[str, str]:
    """Retrieve stored credentials. Raises click.ClickException if not found."""
    username = keyring.get_password(SERVICE_NAME, "username")
    if not username:
        raise click.ClickException("No credentials stored. Run: oneplm auth login")
    password = keyring.get_password(SERVICE_NAME, username)
    if not password:
        raise click.ClickException("Password not found in keyring. Run: oneplm auth login")
    return username, password


def delete_credentials() -> None:
    """Remove stored credentials from keyring."""
    username = keyring.get_password(SERVICE_NAME, "username")
    if username:
        keyring.delete_password(SERVICE_NAME, username)
        keyring.delete_password(SERVICE_NAME, "username")


def get_basic_auth():
    """Return a requests HTTPBasicAuth from stored credentials."""
    from requests.auth import HTTPBasicAuth

    username, password = get_credentials()
    return HTTPBasicAuth(username, password)
