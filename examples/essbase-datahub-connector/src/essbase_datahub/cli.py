"""
Command-line interface for Essbase to DataHub connector.
"""

import argparse
import logging
import sys
import json
from pathlib import Path

from .connector import run_sync, EssbaseDataHubConnector


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract metadata from Oracle Essbase and emit to DataHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync a single cube
  essbase-datahub sync --essbase-url https://essbase.example.com \\
      --essbase-user admin --essbase-password secret \\
      --datahub-server http://datahub-gms:8080 \\
      --app FinanceApp --cube Plan

  # Sync all cubes in an application
  essbase-datahub sync --essbase-url https://essbase.example.com \\
      --essbase-user admin --essbase-password secret \\
      --datahub-server http://datahub-gms:8080 \\
      --app FinanceApp

  # Extract to YAML only (dry run)
  essbase-datahub sync --essbase-url https://essbase.example.com \\
      --essbase-user admin --essbase-password secret \\
      --datahub-server http://datahub-gms:8080 \\
      --app FinanceApp --cube Plan \\
      --output-dir ./extractions --dry-run

  # Test connections
  essbase-datahub test --essbase-url https://essbase.example.com \\
      --essbase-user admin --essbase-password secret \\
      --datahub-server http://datahub-gms:8080
        """,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync metadata from Essbase to DataHub")
    _add_connection_args(sync_parser)
    sync_parser.add_argument("--app", help="Application name (optional, syncs all if not specified)")
    sync_parser.add_argument("--cube", help="Cube name (requires --app)")
    sync_parser.add_argument("--output-dir", "-o", help="Directory to save YAML extractions")
    sync_parser.add_argument("--dry-run", action="store_true", help="Extract only, don't emit to DataHub")
    sync_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Test connections to Essbase and DataHub")
    _add_connection_args(test_parser)
    test_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List applications or cubes")
    _add_essbase_args(list_parser)
    list_parser.add_argument("--app", help="List cubes in this application")
    list_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    # Extract command (YAML only)
    extract_parser = subparsers.add_parser("extract", help="Extract to YAML without emitting")
    _add_essbase_args(extract_parser)
    extract_parser.add_argument("--app", required=True, help="Application name")
    extract_parser.add_argument("--cube", required=True, help="Cube name")
    extract_parser.add_argument("--output", "-o", required=True, help="Output YAML file path")
    extract_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    setup_logging(getattr(args, "verbose", False))
    
    try:
        if args.command == "sync":
            return _cmd_sync(args)
        elif args.command == "test":
            return _cmd_test(args)
        elif args.command == "list":
            return _cmd_list(args)
        elif args.command == "extract":
            return _cmd_extract(args)
    except Exception as e:
        logging.error(f"Error: {e}")
        if getattr(args, "verbose", False):
            raise
        return 1
    
    return 0


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add common connection arguments."""
    _add_essbase_args(parser)
    parser.add_argument("--datahub-server", required=True, help="DataHub GMS server URL")
    parser.add_argument("--datahub-token", help="DataHub authentication token")


def _add_essbase_args(parser: argparse.ArgumentParser) -> None:
    """Add Essbase connection arguments."""
    parser.add_argument("--essbase-url", required=True, help="Essbase REST API URL")
    parser.add_argument("--essbase-user", required=True, help="Essbase username")
    parser.add_argument("--essbase-password", required=True, help="Essbase password")


def _cmd_sync(args: argparse.Namespace) -> int:
    """Execute sync command."""
    if args.cube and not args.app:
        logging.error("--cube requires --app")
        return 1
    
    result = run_sync(
        essbase_url=args.essbase_url,
        essbase_user=args.essbase_user,
        essbase_password=args.essbase_password,
        datahub_server=args.datahub_server,
        datahub_token=args.datahub_token,
        app_name=args.app,
        cube_name=args.cube,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
    )
    
    print(json.dumps(result, indent=2, default=str))
    return 0


def _cmd_test(args: argparse.Namespace) -> int:
    """Execute test command."""
    with EssbaseDataHubConnector(
        essbase_url=args.essbase_url,
        essbase_user=args.essbase_user,
        essbase_password=args.essbase_password,
        datahub_server=args.datahub_server,
        datahub_token=args.datahub_token,
        dry_run=True,
    ) as connector:
        results = connector.test_connections()
    
    print("Connection test results:")
    for name, status in results.items():
        status_str = "✓ OK" if status is True else ("⚠ DRY RUN" if status == "dry_run" else "✗ FAILED")
        print(f"  {name}: {status_str}")
    
    return 0 if all(v is True or v == "dry_run" for v in results.values()) else 1


def _cmd_list(args: argparse.Namespace) -> int:
    """Execute list command."""
    from .extractors import EssbaseClient, EssbaseConnectionConfig
    
    config = EssbaseConnectionConfig(
        base_url=args.essbase_url,
        username=args.essbase_user,
        password=args.essbase_password,
    )
    
    with EssbaseClient(config) as client:
        if args.app:
            items = client.list_cubes(args.app)
            print(f"Cubes in {args.app}:")
        else:
            items = client.list_applications()
            print("Applications:")
        
        for item in items:
            print(f"  - {item.get('name', 'unknown')}")
    
    return 0


def _cmd_extract(args: argparse.Namespace) -> int:
    """Execute extract command."""
    with EssbaseDataHubConnector(
        essbase_url=args.essbase_url,
        essbase_user=args.essbase_user,
        essbase_password=args.essbase_password,
        datahub_server="http://localhost:8080",  # Not used
        dry_run=True,
    ) as connector:
        extraction = connector.extract_cube(args.app, args.cube)
        extraction.to_yaml(args.output)
    
    print(f"Extracted to: {args.output}")
    print(f"  Datasets: {len(extraction.datasets)}")
    print(f"  Jobs: {len(extraction.jobs)}")
    print(f"  Lineage edges: {len(extraction.lineage_edges)}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
