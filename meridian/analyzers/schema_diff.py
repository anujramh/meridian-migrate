from datetime import datetime
from rich.console import Console

console = Console()

MOCK_DATA = {
    "source": "aws-rds-postgresql",
    "target": "oracle-managed-postgresql",
    "analyzed_at": None,
    "postgresql_versions": {
        "source": "14.9",
        "target": "15.12",
        "compatible": True,
        "warnings": [
            "Minor version upgrade from 14 to 15 — test application compatibility",
            "PostgreSQL 15 changed default permissions on public schema"
        ]
    },
    "extensions": {
        "source_extensions": [
            "uuid-ossp",
            "pgcrypto",
            "pg_stat_statements",
            "aws_s3",
            "pg_partman",
            "timescaledb",
            "pg_trgm",
            "btree_gin"
        ],
        "available_on_target": [
            "uuid-ossp",
            "pgcrypto",
            "pg_stat_statements",
            "pg_trgm",
            "btree_gin"
        ],
        "missing_on_target": [
            {
                "name": "aws_s3",
                "severity": "warning",
                "reason": "AWS-specific extension — not available on Oracle Managed PostgreSQL",
                "action": "Remove usage of aws_s3 functions and replace with Oracle Object Storage SDK"
            },
            {
                "name": "pg_partman",
                "severity": "warning",
                "reason": "Not available on Oracle Managed PostgreSQL",
                "action": "Implement partitioning using native PostgreSQL 15 declarative partitioning"
            },
            {
                "name": "timescaledb",
                "severity": "critical",
                "reason": "TimescaleDB is not available on Oracle Managed PostgreSQL",
                "action": "Migrate time-series data to Oracle-native solution or use plain PostgreSQL partitioning"
            }
        ]
    },
    "parameters": {
        "differences": [
            {
                "parameter": "max_connections",
                "source_value": "200",
                "target_value": "100",
                "severity": "warning",
                "action": "Increase max_connections on target or reduce connection pool size on application"
            },
            {
                "parameter": "work_mem",
                "source_value": "64MB",
                "target_value": "4MB",
                "severity": "warning",
                "action": "Increase work_mem on target to match source for query performance"
            },
            {
                "parameter": "wal_level",
                "source_value": "logical",
                "target_value": "replica",
                "severity": "critical",
                "action": "Set wal_level=logical on target — required for CDC replication to work"
            }
        ]
    },
    "rds_specific": {
        "found": [
            {
                "feature": "rds_superuser",
                "severity": "warning",
                "action": "Replace rds_superuser role with appropriate Oracle Managed PostgreSQL roles"
            },
            {
                "feature": "iam_authentication",
                "severity": "warning",
                "action": "Replace IAM authentication with Oracle Cloud IAM or username/password auth"
            }
        ]
    },
    "ssl": {
        "source_requires_ssl": False,
        "target_requires_ssl": True,
        "severity": "warning",
        "action": "Enable SSL in application connection strings before cutover"
    },
    "summary": {
        "total_checks": 12,
        "critical": 2,
        "warnings": 6,
        "passed": 4,
        "ready_to_migrate": False,
        "blocker_reason": "2 critical issues must be resolved before migration can proceed"
    }
}


def analyze_mock():
    console.print("[bold yellow]Running in mock mode — no real DB credentials needed[/bold yellow]\n")

    console.print("[bold blue]Checking PostgreSQL versions...[/bold blue]")
    console.print(f"[green]Source: PostgreSQL {MOCK_DATA['postgresql_versions']['source']}[/green]")
    console.print(f"[green]Target: PostgreSQL {MOCK_DATA['postgresql_versions']['target']}[/green]")

    console.print("\n[bold blue]Checking extensions...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['extensions']['source_extensions'])} extensions on source[/green]")
    console.print(f"[red]Missing {len(MOCK_DATA['extensions']['missing_on_target'])} extensions on target[/red]")

    console.print("\n[bold blue]Checking connection parameters...[/bold blue]")
    console.print(f"[yellow]Found {len(MOCK_DATA['parameters']['differences'])} parameter differences[/yellow]")

    console.print("\n[bold blue]Checking RDS-specific features...[/bold blue]")
    console.print(f"[yellow]Found {len(MOCK_DATA['rds_specific']['found'])} RDS-specific features[/yellow]")

    console.print("\n[bold blue]Checking SSL configuration...[/bold blue]")
    console.print("[yellow]SSL not enforced on source — required on Oracle Managed PostgreSQL[/yellow]")

    result = MOCK_DATA.copy()
    result['analyzed_at'] = datetime.utcnow().isoformat()
    return result


def print_summary(result):
    summary = result['summary']
    console.print("\n" + "─" * 50)
    console.print("[bold magenta]  Meridian — Schema Analysis Summary[/bold magenta]")
    console.print("─" * 50)
    console.print(f"  Source:  AWS RDS PostgreSQL {result['postgresql_versions']['source']}")
    console.print(f"  Target:  Oracle Managed PostgreSQL {result['postgresql_versions']['target']}")
    console.print(f"  Analyzed: {result['analyzed_at']}")
    console.print()
    console.print(f"  [red]❌ Critical issues:  {summary['critical']}[/red]")
    console.print(f"  [yellow]⚠️  Warnings:         {summary['warnings']}[/yellow]")
    console.print(f"  [green]✅ Passed checks:    {summary['passed']}[/green]")
    console.print()

    if summary['critical'] > 0:
        console.print("  [bold red]CRITICAL — fix before migrating:[/bold red]")
        for ext in result['extensions']['missing_on_target']:
            if ext['severity'] == 'critical':
                console.print(f"  [red]❌ Extension {ext['name']}: {ext['reason']}[/red]")
        for param in result['parameters']['differences']:
            if param['severity'] == 'critical':
                console.print(f"  [red]❌ {param['parameter']}: {param['action']}[/red]")

    console.print()
    if summary['warnings'] > 0:
        console.print("  [bold yellow]WARNINGS — review before migrating:[/bold yellow]")
        for ext in result['extensions']['missing_on_target']:
            if ext['severity'] == 'warning':
                console.print(f"  [yellow]⚠️  Extension {ext['name']}: {ext['action']}[/yellow]")
        for param in result['parameters']['differences']:
            if param['severity'] == 'warning':
                console.print(f"  [yellow]⚠️  {param['parameter']}: source={param['source_value']} target={param['target_value']}[/yellow]")
        for feature in result['rds_specific']['found']:
            console.print(f"  [yellow]⚠️  {feature['feature']}: {feature['action']}[/yellow]")
        if not result['ssl']['source_requires_ssl'] and result['ssl']['target_requires_ssl']:
            console.print(f"  [yellow]⚠️  SSL: {result['ssl']['action']}[/yellow]")

    console.print()
    if summary['ready_to_migrate']:
        console.print("  [bold green]✅ READY TO MIGRATE[/bold green]")
    else:
        console.print("  [bold red]🚫 NOT READY TO MIGRATE[/bold red]")
        console.print(f"  [red]{summary['blocker_reason']}[/red]")

    console.print("─" * 50)


def analyze(mock=False):
    console.print(f"\n[bold magenta]Meridian — Schema Diff Analyzer[/bold magenta]")
    console.print(f"Path: [yellow]AWS RDS PostgreSQL → Oracle Managed PostgreSQL[/yellow]\n")

    if mock:
        return analyze_mock()

    # Real analysis comes later
    console.print("[red]Real mode not yet implemented — use --mock for now[/red]")
    return None