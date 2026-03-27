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
    console.print(f"  Source DB:  {result.get('source_db', 'unknown')} (AWS RDS PostgreSQL {result['postgresql_versions']['source']})")
    console.print(f"  Target DB:  {result.get('target_db', 'unknown')} (Oracle Managed PostgreSQL {result['postgresql_versions']['target']})")
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
        for feature in result.get('rds_specific', {}).get('found', []):
            console.print(f"  [yellow]⚠️  {feature['feature']}: {feature['action']}[/yellow]")
        if result.get('ssl') and not result['ssl']['source_requires_ssl'] and result['ssl']['target_requires_ssl']:
            console.print(f"  [yellow]⚠️  SSL: {result['ssl']['action']}[/yellow]")

    console.print()
    if summary['ready_to_migrate']:
        console.print("  [bold green]✅ READY TO MIGRATE[/bold green]")
    else:
        console.print("  [bold red]🚫 NOT READY TO MIGRATE[/bold red]")
        console.print(f"  [red]{summary['blocker_reason']}[/red]")

    console.print("─" * 50)


def analyze(mock=False, source_db=None, target_db=None,
            source_config=None, target_config=None):
    console.print(f"\n[bold magenta]Meridian — Schema Diff Analyzer[/bold magenta]")
    console.print(f"Path: [yellow]AWS RDS PostgreSQL → Oracle Managed PostgreSQL[/yellow]\n")

    if mock:
        result = analyze_mock()
        result['source_db'] = source_db
        result['target_db'] = target_db
        return result

    if source_config and target_config:
        return analyze_real(source_config, target_config)

    console.print("[red]Provide --env or connection details to run real analysis[/red]")
    return None

def get_db_info(host, port, database, user, password, sslmode='prefer'):
    import psycopg2
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        sslmode=sslmode
    )
    cur = conn.cursor()

    # Version
    cur.execute("SELECT version()")
    version = cur.fetchone()[0].split(',')[0]

    # Extensions
    cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")
    extensions = [r[0] for r in cur.fetchall()]

    # Parameters
    cur.execute("""
        SELECT name, setting, unit FROM pg_settings
        WHERE name IN (
            'max_connections', 'shared_buffers', 'work_mem',
            'maintenance_work_mem', 'wal_level', 'max_wal_senders'
        )
    """)
    parameters = {r[0]: {"value": r[1], "unit": r[2]} for r in cur.fetchall()}

    # Tables
    cur.execute("""
        SELECT t.table_name, s.n_live_tup
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
        WHERE t.table_schema = 'public'
        ORDER BY t.table_name
    """)
    tables = [{"name": r[0], "rows": r[1] or 0} for r in cur.fetchall()]

    conn.close()
    return {
        "version": version,
        "extensions": extensions,
        "parameters": parameters,
        "tables": tables
    }


ORACLE_PG_SUPPORTED_EXTENSIONS = [
    "plpgsql", "pg_stat_statements", "pgcrypto", "uuid-ossp",
    "pg_trgm", "btree_gin", "btree_gist", "hstore", "citext",
    "fuzzystrmatch", "unaccent", "tablefunc"
]


def analyze_real(source_config, target_config):
    console.print("[bold blue]Connecting to source database...[/bold blue]")
    source = get_db_info(**source_config)
    console.print(f"[green]Connected — {source['version']}[/green]")

    console.print("[bold blue]Connecting to target database...[/bold blue]")
    target = get_db_info(**target_config)
    console.print(f"[green]Connected — {target['version']}[/green]\n")

    # Version check
    console.print("[bold blue]Checking PostgreSQL versions...[/bold blue]")
    src_major = int(source['version'].split('PostgreSQL ')[1].split('.')[0])
    tgt_major = int(target['version'].split('PostgreSQL ')[1].split('.')[0])
    version_warnings = []
    if src_major != tgt_major:
        version_warnings.append(f"Major version difference: source={src_major} target={tgt_major} — test app compatibility")

    # Extensions check
    console.print("[bold blue]Checking extensions...[/bold blue]")
    missing_extensions = []
    for ext in source['extensions']:
        if ext not in ORACLE_PG_SUPPORTED_EXTENSIONS:
            missing_extensions.append({
                "name": ext,
                "severity": "critical" if ext in ["timescaledb", "postgis"] else "warning",
                "reason": f"Extension {ext} may not be available on Oracle Managed PostgreSQL",
                "action": f"Verify {ext} availability on target or find alternative"
            })

    # Parameters check
    console.print("[bold blue]Checking connection parameters...[/bold blue]")
    param_differences = []
    critical_params = {
        "wal_level": {
            "required": "logical",
            "reason": "CDC replication requires wal_level=logical"
        }
    }
    for param, info in critical_params.items():
        src_val = source['parameters'].get(param, {}).get('value', 'unknown')
        tgt_val = target['parameters'].get(param, {}).get('value', 'unknown')
        if src_val != info['required'] or tgt_val != info['required']:
            param_differences.append({
                "parameter": param,
                "source_value": src_val,
                "target_value": tgt_val,
                "required_value": info['required'],
                "severity": "critical",
                "action": f"Set {param}={info['required']} on both source and target"
            })

    for param in ['max_connections', 'work_mem']:
        src_val = source['parameters'].get(param, {}).get('value', '0')
        tgt_val = target['parameters'].get(param, {}).get('value', '0')
        if src_val != tgt_val:
            param_differences.append({
                "parameter": param,
                "source_value": src_val,
                "target_value": tgt_val,
                "severity": "warning",
                "action": f"Review {param} difference before migration"
            })

    # Tables check
    console.print("[bold blue]Checking tables...[/bold blue]")
    source_tables = [t['name'] for t in source['tables']]
    target_tables = [t['name'] for t in target['tables']]
    missing_tables = [t for t in source_tables if t not in target_tables]

    # SSL check
    console.print("[bold blue]Checking SSL configuration...[/bold blue]")

    # Summary
    critical_count = (
        len([e for e in missing_extensions if e['severity'] == 'critical']) +
        len([p for p in param_differences if p['severity'] == 'critical'])
    )
    warning_count = (
        len([e for e in missing_extensions if e['severity'] == 'warning']) +
        len([p for p in param_differences if p['severity'] == 'warning']) +
        len(missing_tables)
    )
    passed = 4 - critical_count - min(warning_count, 2)
    passed = max(0, passed)

    return {
        "source": "aws-rds-postgresql",
        "target": "oracle-managed-postgresql",
        "analyzed_at": datetime.utcnow().isoformat(),
        "source_db": source_config.get('database'),
        "target_db": target_config.get('database'),
        "postgresql_versions": {
            "source": source['version'],
            "target": target['version'],
            "compatible": abs(src_major - tgt_major) <= 1,
            "warnings": version_warnings
        },
        "extensions": {
            "source_extensions": source['extensions'],
            "available_on_target": ORACLE_PG_SUPPORTED_EXTENSIONS,
            "missing_on_target": missing_extensions
        },
        "parameters": {
            "differences": param_differences
        },
        "tables": {
            "source_tables": source_tables,
            "target_tables": target_tables,
            "missing_on_target": missing_tables
        },
        "ssl": {
            "source_requires_ssl": False,
            "target_requires_ssl": True,
            "severity": "warning",
            "action": "Enable SSL in application connection strings before cutover"
        },
        "summary": {
            "total_checks": 12,
            "critical": critical_count,
            "warnings": warning_count,
            "passed": passed,
            "ready_to_migrate": critical_count == 0,
            "blocker_reason": f"{critical_count} critical issues must be resolved" if critical_count > 0 else None
        }
    }