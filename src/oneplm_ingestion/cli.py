"""Click CLI entry point and all subcommands."""

from __future__ import annotations

import logging
from pathlib import Path

import click

DEFAULT_DATA_DIR = Path("./data")
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "oneplm.db"
DEFAULT_TYPES_CONFIG = Path("./config/types.json")
DEFAULT_CHECKS_CONFIG = Path("./config/checks.json")


@click.group()
@click.option("--db", default=str(DEFAULT_DB_PATH), envvar="ONEPLM_DB_PATH",
              help="Path to SQLite database file.")
@click.option("--data-dir", default=str(DEFAULT_DATA_DIR), envvar="ONEPLM_DATA_DIR",
              help="Directory for downloaded files.")
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx, db, data_dir, verbose):
    """oneplm - Windchill PLM data ingestion and validation tool."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = Path(db)
    ctx.obj["data_dir"] = Path(data_dir)


# ---------------------------------------------------------------------------
# init / status
# ---------------------------------------------------------------------------


@cli.command("init")
@click.pass_context
def init_database(ctx):
    """Initialize the local database (creates tables if they don't exist)."""
    from oneplm_ingestion.db import get_connection, init_db

    db_path = ctx.obj["db_path"]
    conn = get_connection(db_path)
    init_db(conn)
    click.echo(f"Database initialized at {db_path}")
    conn.close()


@cli.command()
@click.pass_context
def status(ctx):
    """Show database status: object counts, last sync times."""
    from oneplm_ingestion.db import get_connection, init_db

    conn = get_connection(ctx.obj["db_path"])
    init_db(conn)

    rows = conn.execute(
        "SELECT type_name, COUNT(*) as cnt FROM objects GROUP BY type_name"
    ).fetchall()
    if rows:
        click.echo("Objects in database:")
        for row in rows:
            click.echo(f"  {row['type_name']}: {row['cnt']}")
    else:
        click.echo("No objects in database.")

    sync_rows = conn.execute(
        "SELECT * FROM sync_log ORDER BY last_sync_at DESC"
    ).fetchall()
    if sync_rows:
        click.echo("\nLast sync:")
        for row in sync_rows:
            click.echo(f"  {row['type_name']}: {row['last_sync_at']} ({row['objects_fetched']} fetched)")

    check_rows = conn.execute(
        """SELECT check_name, COUNT(*) as total,
                  SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END) as passed,
                  SUM(CASE WHEN passed = 0 THEN 1 ELSE 0 END) as failed
           FROM check_results GROUP BY check_name"""
    ).fetchall()
    if check_rows:
        click.echo("\nCheck results:")
        for row in check_rows:
            click.echo(f"  {row['check_name']}: {row['passed']} passed, {row['failed']} failed")

    conn.close()


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------


@cli.group()
def auth():
    """Manage Windchill API credentials."""


@auth.command("login")
def auth_login():
    """Store Windchill credentials in the system keyring."""
    from oneplm_ingestion.auth import store_credentials

    username = click.prompt("Username")
    password = click.prompt("Password", hide_input=True)
    store_credentials(username, password)
    click.echo(f"Credentials stored for {username}.")


@auth.command("logout")
def auth_logout():
    """Remove stored credentials."""
    from oneplm_ingestion.auth import delete_credentials

    delete_credentials()
    click.echo("Credentials removed.")


@auth.command("status")
def auth_status():
    """Check if credentials are stored."""
    from oneplm_ingestion.auth import get_credentials

    try:
        username, _ = get_credentials()
        click.echo(f"Credentials stored for: {username}")
    except click.ClickException:
        click.echo("No credentials stored.")


# ---------------------------------------------------------------------------
# lookup
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("number")
@click.pass_context
def lookup(ctx, number):
    """Look up a document or part by number and show related objects.

    Searches both Documents and Parts, then follows relationships to find
    associated documents, parts, and containers/libraries.
    """
    from oneplm_ingestion.api import WindchillClient
    from oneplm_ingestion.lookup import format_lookup_result, lookup_by_number

    client = WindchillClient()
    result = lookup_by_number(client, number)
    click.echo(format_lookup_result(result))


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--types-config", default=str(DEFAULT_TYPES_CONFIG), help="Path to types.json")
@click.option("--type", "type_names", multiple=True,
              help="Sync only these types (by human name). Repeatable.")
@click.option("--full", is_flag=True, help="Full sync (ignore last_modified, fetch everything).")
@click.pass_context
def sync(ctx, types_config, type_names, full):
    """Sync objects from Windchill into local database."""
    from oneplm_ingestion.api import WindchillClient
    from oneplm_ingestion.db import get_connection, init_db
    from oneplm_ingestion.sync import sync_all

    conn = get_connection(ctx.obj["db_path"])
    init_db(conn)
    client = WindchillClient()
    results = sync_all(
        client, conn, Path(types_config),
        types=list(type_names) if type_names else None,
        full=full,
    )
    for type_name, count in results.items():
        click.echo(f"  {type_name}: {count} objects synced")
    conn.close()


# ---------------------------------------------------------------------------
# pdf
# ---------------------------------------------------------------------------


@cli.group()
def pdf():
    """Download and extract text from PDFs."""


@pdf.command("download")
@click.option("--type", "type_name", help="Download PDFs for all objects of this type.")
@click.option("--object-id", help="Download PDFs for a specific object ID.")
@click.option("--types-config", default=str(DEFAULT_TYPES_CONFIG), help="Path to types.json")
@click.pass_context
def pdf_download(ctx, type_name, object_id, types_config):
    """Download PDFs from Windchill for local objects."""
    from oneplm_ingestion.api import WindchillClient
    from oneplm_ingestion.db import get_connection, get_objects_by_type
    from oneplm_ingestion.pdf import download_pdfs_for_object
    from oneplm_ingestion.sync import load_type_configs

    conn = get_connection(ctx.obj["db_path"])
    client = WindchillClient()

    if object_id:
        pdfs = download_pdfs_for_object(client, conn, object_id, ctx.obj["data_dir"])
        click.echo(f"Downloaded {len(pdfs)} PDF(s) for {object_id}")
    elif type_name:
        # Look up domain/collection from types config
        type_configs = load_type_configs(Path(types_config))
        tc = next((t for t in type_configs if t.human_name == type_name), None)
        domain = tc.domain if tc else "v6/DocMgmt"
        collection = tc.collection if tc else "Documents"

        objects = get_objects_by_type(conn, type_name)
        total = 0
        for obj in objects:
            click.echo(f"  Downloading PDFs for {obj.number or obj.id}...")
            pdfs = download_pdfs_for_object(
                client, conn, obj.id, ctx.obj["data_dir"],
                domain=domain, collection=collection,
            )
            total += len(pdfs)
        click.echo(f"Downloaded {total} PDF(s) for {len(objects)} objects")
    else:
        raise click.UsageError("Provide --type or --object-id")
    conn.close()


@pdf.command("extract")
@click.option("--object-id", help="Extract text from PDFs of a specific object.")
@click.option("--all", "extract_all", is_flag=True, help="Extract text from all downloaded PDFs.")
@click.pass_context
def pdf_extract(ctx, object_id, extract_all):
    """Extract text from downloaded PDFs using docling."""
    from oneplm_ingestion.db import get_connection, get_pdfs_for_object, get_pdfs_pending_extraction
    from oneplm_ingestion.pdf import extract_and_save

    conn = get_connection(ctx.obj["db_path"])

    if object_id:
        pdfs = get_pdfs_for_object(conn, object_id)
        pdfs = [p for p in pdfs if p.local_path and not p.extracted_text]
    elif extract_all:
        pdfs = get_pdfs_pending_extraction(conn)
    else:
        raise click.UsageError("Provide --object-id or --all")

    if not pdfs:
        click.echo("No PDFs pending extraction.")
        conn.close()
        return

    click.echo(f"Extracting text from {len(pdfs)} PDF(s)...")
    for pdf in pdfs:
        click.echo(f"  {pdf.filename}...")
        extract_and_save(conn, pdf)
    click.echo("Done.")
    conn.close()


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--checks-config", default=str(DEFAULT_CHECKS_CONFIG), help="Path to checks.json")
@click.option("--check", "check_names", multiple=True, help="Run only these checks. Repeatable.")
@click.pass_context
def check(ctx, checks_config, check_names):
    """Run attribute validation checks against local data."""
    from oneplm_ingestion.checks import run_all_checks
    from oneplm_ingestion.db import get_connection

    conn = get_connection(ctx.obj["db_path"])
    results = run_all_checks(
        conn, Path(checks_config),
        check_names=list(check_names) if check_names else None,
    )
    for name, checks in results.items():
        passed = sum(1 for r in checks if r.passed)
        failed = sum(1 for r in checks if not r.passed)
        icon = "PASS" if failed == 0 else "FAIL"
        click.echo(f"  [{icon}] {name}: {passed} passed, {failed} failed")
    conn.close()


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


@cli.group()
def export():
    """Export data to CSV files."""


@export.command("objects")
@click.option("--type", "type_name", help="Export only this type.")
@click.option("-o", "--output", default="objects.csv", help="Output CSV file path.")
@click.pass_context
def export_objects_cmd(ctx, type_name, output):
    """Export synced objects to CSV."""
    from oneplm_ingestion.db import get_connection
    from oneplm_ingestion.export import export_objects

    conn = get_connection(ctx.obj["db_path"])
    count = export_objects(conn, type_name, Path(output))
    click.echo(f"Exported {count} objects to {output}")
    conn.close()


@export.command("checks")
@click.option("--check", "check_name", help="Export only this check's results.")
@click.option("--failed-only", is_flag=True, help="Export only failed checks.")
@click.option("-o", "--output", default="check_results.csv", help="Output CSV file path.")
@click.pass_context
def export_checks_cmd(ctx, check_name, failed_only, output):
    """Export check results to CSV."""
    from oneplm_ingestion.db import get_connection
    from oneplm_ingestion.export import export_check_results

    conn = get_connection(ctx.obj["db_path"])
    count = export_check_results(conn, Path(output), check_name=check_name, failed_only=failed_only)
    click.echo(f"Exported {count} check results to {output}")
    conn.close()
