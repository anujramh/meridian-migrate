import socket
import subprocess
from datetime import datetime
from rich.console import Console

console = Console()

ORACLE_PG_SUPPORTED_EXTENSIONS = [
    "plpgsql", "pg_stat_statements", "pgcrypto", "uuid-ossp",
    "pg_trgm", "btree_gin", "btree_gist", "hstore", "citext",
    "fuzzystrmatch", "unaccent", "tablefunc", "pglogical"
]

MOCK_DATA = {
    "source": "aws-rds-postgresql",
    "target": "oracle-managed-postgresql",
    "analyzed_at": None,
    "source_db": None,
    "target_db": None,
    "postgresql_versions": {
        "source": "14.9",
        "target": "15.12",
        "compatible": True,
        "warnings": ["Minor version upgrade from 14 to 15 — test application compatibility"]
    },
    "network": {
        "source_to_target": {"reachable": True, "latency_ms": 12},
        "target_to_source": {"reachable": True, "note": "Verify from target side"}
    },
    "pglogical": {
        "source_available": True,
        "source_installed": False,
        "target_available": True,
        "target_installed": False,
        "issues": [
            {
                "location": "source",
                "severity": "critical",
                "issue": "pglogical available but not installed on source",
                "action": "Run: CREATE EXTENSION pglogical on source database"
            },
            {
                "location": "target",
                "severity": "critical",
                "issue": "pglogical available but not installed on target",
                "action": "Run: CREATE EXTENSION pglogical on target database"
            }
        ]
    },
    "wal_level": {
        "source": "logical",
        "target": "replica",
        "issues": [
            {
                "location": "target",
                "severity": "critical",
                "current": "replica",
                "required": "logical",
                "action": "Set wal_level=logical in Oracle Cloud PostgreSQL configuration"
            }
        ]
    },
    "replication": {
        "max_wal_senders": 10,
        "max_replication_slots": 10,
        "track_commit_timestamp": "off",
        "has_replication_privilege": True,
        "issues": [
            {
                "severity": "warning",
                "issue": "track_commit_timestamp=off on target",
                "action": "Set track_commit_timestamp=1 in Oracle Cloud PostgreSQL configuration"
            }
        ]
    },
    "primary_keys": {
        "tables_without_pk": ["audit_logs"],
        "issues": [
            {
                "table": "audit_logs",
                "severity": "critical",
                "issue": "Table audit_logs has no primary key",
                "action": "Add primary key — pglogical cannot replicate tables without PKs"
            }
        ]
    },
    "unlogged_tables": {
        "tables": [],
        "issues": []
    },
    "large_objects": {
        "count": 0,
        "issues": []
    },
    "sequences": {
        "source_sequences": ["users_id_seq", "orders_id_seq", "products_id_seq"],
        "note": "pglogical does not replicate sequences automatically — sync manually after CDC catches up",
        "action": "After cutover run: SELECT setval(seq_name, (SELECT MAX(id) FROM table)) for each sequence"
    },
    "extensions": {
        "source_extensions": ["plpgsql", "pg_stat_statements", "aws_s3", "timescaledb"],
        "available_on_target": ORACLE_PG_SUPPORTED_EXTENSIONS,
        "missing_on_target": [
            {
                "name": "aws_s3",
                "severity": "warning",
                "reason": "AWS-specific extension",
                "action": "Replace with Oracle Object Storage SDK"
            },
            {
                "name": "timescaledb",
                "severity": "critical",
                "reason": "Not available on Oracle Managed PostgreSQL",
                "action": "Migrate time-series data to native PostgreSQL partitioning"
            }
        ]
    },
    "initial_load": {
        "pg_dump_available": True,
        "pg_restore_available": True,
        "issues": []
    },
    "target_readiness": {
        "existing_subscriptions": [],
        "existing_nodes": [],
        "tables_missing_on_target": ["users", "orders", "products"],
        "issues": [
            {
                "severity": "warning",
                "issue": "3 tables missing on target",
                "action": "Run pg_dump --schema-only on source then pg_restore on target before starting CDC"
            }
        ]
    },
    "ddl_warning": {
        "message": "pglogical does NOT replicate DDL changes automatically",
        "action": "Any schema changes during replication must be applied manually on both source and target"
    },
    "post_restore_steps": [
        "Run VACUUM ANALYZE on target after pg_restore",
        "Sync sequences manually after CDC catches up",
        "Set synchronize_data=false when creating pglogical subscription — data already loaded by pg_dump",
        "Verify row counts match before cutover"
    ],
    "parameters": {
        "differences": [
            {
                "parameter": "max_connections",
                "source_value": "200",
                "target_value": "100",
                "severity": "warning",
                "action": "Review max_connections difference"
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
        "total_checks": 15,
        "critical": 3,
        "warnings": 4,
        "passed": 8,
        "ready_to_migrate": False,
        "blocker_reason": "3 critical issues must be resolved before migration can proceed"
    }
}


def check_tcp_connectivity(host, port, timeout=5):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        start = datetime.utcnow()
        result = sock.connect_ex((host, int(port)))
        elapsed = (datetime.utcnow() - start).total_seconds() * 1000
        sock.close()
        return result == 0, round(elapsed, 2)
    except Exception as e:
        return False, 0


def check_tool_available(tool):
    try:
        result = subprocess.run(
            [tool, '--version'],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


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

    # Extensions installed
    cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")
    extensions = [r[0] for r in cur.fetchall()]

    # pglogical available
    cur.execute("SELECT name FROM pg_available_extensions WHERE name = 'pglogical'")
    pglogical_available = cur.fetchone() is not None

    # Parameters
    cur.execute("""
        SELECT name, setting, unit FROM pg_settings
        WHERE name IN (
            'max_connections', 'shared_buffers', 'work_mem',
            'maintenance_work_mem', 'wal_level', 'max_wal_senders',
            'max_replication_slots', 'track_commit_timestamp'
        )
    """)
    parameters = {r[0]: {"value": r[1], "unit": r[2]} for r in cur.fetchall()}

    # Tables
    cur.execute("""
        SELECT t.table_name, COALESCE(s.n_live_tup, 0)
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
        WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """)
    tables = [{"name": r[0], "rows": r[1]} for r in cur.fetchall()]

    # Tables without primary keys
    cur.execute("""
        SELECT t.table_name
        FROM information_schema.tables t
        LEFT JOIN information_schema.table_constraints tc
            ON tc.table_name = t.table_name
            AND tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
        WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
        AND tc.constraint_name IS NULL
    """)
    tables_without_pk = [r[0] for r in cur.fetchall()]

    # Unlogged tables
    cur.execute("""
        SELECT relname FROM pg_class
        WHERE relpersistence = 'u'
        AND relnamespace = (
            SELECT oid FROM pg_namespace WHERE nspname = 'public'
        )
    """)
    unlogged_tables = [r[0] for r in cur.fetchall()]

    # Large objects
    cur.execute("SELECT count(*) FROM pg_largeobject_metadata")
    large_objects_count = cur.fetchone()[0]

    # Sequences
    cur.execute("""
        SELECT sequence_name FROM information_schema.sequences
        WHERE sequence_schema = 'public'
    """)
    sequences = [r[0] for r in cur.fetchall()]

    # Replication slots
    cur.execute("SELECT count(*) FROM pg_replication_slots")
    replication_slots = cur.fetchone()[0]

    # Replication privilege
    cur.execute("SELECT rolreplication FROM pg_roles WHERE rolname = current_user")
    row = cur.fetchone()
    has_replication_privilege = row[0] if row else False

    # Also check for rds_replication role membership (AWS RDS specific)
    if not has_replication_privilege:
        cur.execute("""
        SELECT COUNT(*) FROM pg_auth_members am
        JOIN pg_roles r ON r.oid = am.roleid
        JOIN pg_roles m ON m.oid = am.member
        WHERE r.rolname = 'rds_replication'
        AND m.rolname = current_user
        """)
        rds_replication = cur.fetchone()[0]
        if rds_replication > 0:
         has_replication_privilege = True

    # Existing pglogical nodes
    pglogical_nodes = []
    if 'pglogical' in extensions:
        try:
            cur.execute("SELECT node_name FROM pglogical.node")
            pglogical_nodes = [r[0] for r in cur.fetchall()]
        except Exception:
            pass

    # Existing pglogical subscriptions
    pglogical_subscriptions = []
    if 'pglogical' in extensions:
        try:
            cur.execute("SELECT sub_name FROM pglogical.subscription")
            pglogical_subscriptions = [r[0] for r in cur.fetchall()]
        except Exception:
            pass

    conn.close()
    return {
        "version": version,
        "extensions": extensions,
        "pglogical_available": pglogical_available,
        "pglogical_installed": 'pglogical' in extensions,
        "parameters": parameters,
        "tables": tables,
        "tables_without_pk": tables_without_pk,
        "unlogged_tables": unlogged_tables,
        "large_objects_count": large_objects_count,
        "sequences": sequences,
        "replication_slots": replication_slots,
        "has_replication_privilege": has_replication_privilege,
        "pglogical_nodes": pglogical_nodes,
        "pglogical_subscriptions": pglogical_subscriptions
    }


def analyze_mock():
    console.print("[bold yellow]Running in mock mode — no real DB credentials needed[/bold yellow]\n")
    console.print("[bold blue]Checking network connectivity...[/bold blue]")
    console.print("[green]Source → Target: reachable (12ms)[/green]")
    console.print("[bold blue]Checking PostgreSQL versions...[/bold blue]")
    console.print("[green]Source: PostgreSQL 14.9 · Target: PostgreSQL 15.12[/green]")
    console.print("[bold blue]Checking pglogical...[/bold blue]")
    console.print("[red]pglogical not installed on source or target[/red]")
    console.print("[bold blue]Checking wal_level...[/bold blue]")
    console.print("[green]Source: logical ✅[/green]")
    console.print("[red]Target: replica ❌ — needs logical[/red]")
    console.print("[bold blue]Checking replication parameters...[/bold blue]")
    console.print("[bold blue]Checking primary keys...[/bold blue]")
    console.print("[red]1 table without primary key: audit_logs[/red]")
    console.print("[bold blue]Checking unlogged tables...[/bold blue]")
    console.print("[green]No unlogged tables found[/green]")
    console.print("[bold blue]Checking large objects...[/bold blue]")
    console.print("[green]No large objects found[/green]")
    console.print("[bold blue]Checking sequences...[/bold blue]")
    console.print("[yellow]3 sequences found — sync manually after CDC catches up[/yellow]")
    console.print("[bold blue]Checking extensions...[/bold blue]")
    console.print("[bold blue]Checking pg_dump/pg_restore availability...[/bold blue]")
    console.print("[bold blue]Checking target readiness...[/bold blue]")
    console.print("[bold blue]Checking SSL configuration...[/bold blue]")

    result = MOCK_DATA.copy()
    result['analyzed_at'] = datetime.utcnow().isoformat()
    return result


def analyze_real(source_config, target_config):
    issues = []
    warnings = []

    # Network check
    console.print("[bold blue]Checking network connectivity...[/bold blue]")
    src_host = source_config['host']
    tgt_host = target_config['host']
    src_port = source_config['port']
    tgt_port = target_config['port']

    src_reachable, src_latency = check_tcp_connectivity(src_host, src_port)
    tgt_reachable, tgt_latency = check_tcp_connectivity(tgt_host, tgt_port)

    network_issues = []
    if not src_reachable:
        network_issues.append({
            "severity": "critical",
            "issue": f"Cannot reach source at {src_host}:{src_port}",
            "action": "Check security group rules and network connectivity"
        })
        issues.append("network_source")
    else:
        console.print(f"[green]Source reachable — {src_latency}ms[/green]")

    if not tgt_reachable:
        network_issues.append({
            "severity": "critical",
            "issue": f"Cannot reach target at {tgt_host}:{tgt_port}",
            "action": "Check Oracle Cloud security lists and network connectivity"
        })
        issues.append("network_target")
    else:
        console.print(f"[green]Target reachable — {tgt_latency}ms[/green]")

    # DNS resolution check
    console.print("[bold blue]Checking DNS resolution...[/bold blue]")
    import socket
    dns_issues = []

    # Can we resolve source hostname?
    try:
        src_ip = socket.gethostbyname(src_host)
        console.print(f"[green]Source hostname resolves to {src_ip} ✅[/green]")
    except socket.gaierror:
        dns_issues.append({
            "severity": "critical",
            "issue": f"Cannot resolve source hostname: {src_host}",
            "action": "Add DNS A record for source hostname in target cloud private DNS"
        })
        issues.append("dns_source")

    # Can we resolve target hostname?
    try:
        tgt_ip = socket.gethostbyname(tgt_host)
        console.print(f"[green]Target hostname resolves to {tgt_ip} ✅[/green]")
    except socket.gaierror:
        dns_issues.append({
            "severity": "critical",
            "issue": f"Cannot resolve target hostname: {tgt_host}",
            "action": "Add DNS A record for target hostname in source cloud private DNS"
        })
        issues.append("dns_target")

    # Check ORACLE_PG_FQDN is set
    oracle_fqdn = target_config.get('fqdn')
    if not oracle_fqdn:
        dns_issues.append({
            "severity": "warning",
            "issue": "ORACLE_PG_FQDN not set — pglogical subscriber node may use IP instead of FQDN",
            "action": "Add ORACLE_PG_FQDN=<your-oracle-fqdn> to .env file"
        })
        warnings.append("oracle_fqdn")
    else:
        console.print(f"[green]ORACLE_PG_FQDN set to {oracle_fqdn} ✅[/green]")

    # Connect to databases
    console.print("[bold blue]Connecting to source database...[/bold blue]")
    source = get_db_info(
        host=source_config['host'],
        port=source_config['port'],
        database=source_config['database'],
        user=source_config['user'],
        password=source_config['password'],
        sslmode=source_config.get('sslmode', 'prefer')
    )
    console.print(f"[green]Connected — {source['version']}[/green]")

    console.print("[bold blue]Connecting to target database...[/bold blue]")
    target = get_db_info(
        host=target_config['host'],
        port=target_config['port'],
        database=target_config['database'],
        user=target_config['user'],
        password=target_config['password'],
        sslmode=target_config.get('sslmode', 'require')
    )

    console.print(f"[green]Connected — {target['version']}[/green]\n")

    # Version check
    console.print("[bold blue]Checking PostgreSQL versions...[/bold blue]")
    src_major = int(source['version'].split('PostgreSQL ')[1].split('.')[0])
    tgt_major = int(target['version'].split('PostgreSQL ')[1].split('.')[0])
    version_warnings = []
    if src_major != tgt_major:
        version_warnings.append(
            f"Major version difference: source={src_major} target={tgt_major} — test app compatibility"
        )
        warnings.append("version_mismatch")

    # pglogical check
    console.print("[bold blue]Checking pglogical...[/bold blue]")
    pglogical_issues = []
    if not source['pglogical_available']:
        pglogical_issues.append({
            "location": "source",
            "severity": "critical",
            "issue": "pglogical not available on source",
            "action": "Install pglogical extension on source PostgreSQL"
        })
        issues.append("pglogical_not_available_source")
    elif not source['pglogical_installed']:
        pglogical_issues.append({
            "location": "source",
            "severity": "critical",
            "issue": "pglogical available but not installed on source",
            "action": "Run: CREATE EXTENSION pglogical on source database"
        })
        issues.append("pglogical_not_installed_source")
    else:
        console.print("[green]pglogical installed on source ✅[/green]")

    if not target['pglogical_available']:
        pglogical_issues.append({
            "location": "target",
            "severity": "critical",
            "issue": "pglogical not available on target",
            "action": "Enable pglogical in Oracle Cloud PostgreSQL configuration"
        })
        issues.append("pglogical_not_available_target")
    elif not target['pglogical_installed']:
        pglogical_issues.append({
            "location": "target",
            "severity": "critical",
            "issue": "pglogical available but not installed on target",
            "action": "Run: CREATE EXTENSION pglogical on target database"
        })
        issues.append("pglogical_not_installed_target")
    else:
        console.print("[green]pglogical installed on target ✅[/green]")

    # wal_level check
    console.print("[bold blue]Checking wal_level...[/bold blue]")
    wal_issues = []
    src_wal = source['parameters'].get('wal_level', {}).get('value', 'unknown')
    tgt_wal = target['parameters'].get('wal_level', {}).get('value', 'unknown')
    if src_wal != 'logical':
        wal_issues.append({
            "location": "source",
            "severity": "critical",
            "current": src_wal,
            "required": "logical",
            "action": "Set rds.logical_replication=1 in AWS RDS parameter group"
        })
        issues.append("wal_level_source")
    else:
        console.print(f"[green]wal_level=logical on source ✅[/green]")

    if tgt_wal != 'logical':
        wal_issues.append({
            "location": "target",
            "severity": "critical",
            "current": tgt_wal,
            "required": "logical",
            "action": "Set wal_level=logical in Oracle Cloud PostgreSQL configuration"
        })
        issues.append("wal_level_target")
    else:
        console.print(f"[green]wal_level=logical on target ✅[/green]")

    # Replication parameters
    console.print("[bold blue]Checking replication parameters...[/bold blue]")
    replication_issues = []
    max_wal_senders = int(source['parameters'].get('max_wal_senders', {}).get('value', '0'))
    max_repl_slots = int(source['parameters'].get('max_replication_slots', {}).get('value', '0'))
    tct = target['parameters'].get('track_commit_timestamp', {}).get('value', 'off')

    if max_wal_senders < 1:
        replication_issues.append({
            "severity": "critical",
            "issue": f"max_wal_senders={max_wal_senders} on source — must be > 0",
            "action": "Increase max_wal_senders in source parameter group"
        })
        issues.append("max_wal_senders")
    else:
        console.print(f"[green]max_wal_senders={max_wal_senders} on source ✅[/green]")

    if max_repl_slots < 1:
        replication_issues.append({
            "severity": "critical",
            "issue": f"max_replication_slots={max_repl_slots} on source — must be > 0",
            "action": "Increase max_replication_slots in source parameter group"
        })
        issues.append("max_replication_slots")
    else:
        console.print(f"[green]max_replication_slots={max_repl_slots} on source ✅[/green]")

    if tct not in ('on', '1'):
        replication_issues.append({
            "severity": "warning",
            "issue": f"track_commit_timestamp={tct} on target — recommended for pglogical",
            "action": "Set track_commit_timestamp=1 in Oracle Cloud PostgreSQL configuration"
        })
        warnings.append("track_commit_timestamp")

    if not source['has_replication_privilege']:
        replication_issues.append({
            "severity": "critical",
            "issue": "Current user lacks REPLICATION privilege on source",
            "action": "Run: ALTER ROLE <user> WITH REPLICATION on source"
        })
        issues.append("replication_privilege")

    # Primary key check
    console.print("[bold blue]Checking primary keys...[/bold blue]")
    pk_issues = []
    if source['tables_without_pk']:
        for table in source['tables_without_pk']:
            pk_issues.append({
                "table": table,
                "severity": "critical",
                "issue": f"Table '{table}' has no primary key",
                "action": f"Add primary key to '{table}' — pglogical cannot replicate tables without PKs"
            })
            issues.append(f"no_pk_{table}")
    else:
        console.print("[green]All tables have primary keys ✅[/green]")

    # Unlogged tables
    console.print("[bold blue]Checking unlogged tables...[/bold blue]")
    unlogged_issues = []
    if source['unlogged_tables']:
        for table in source['unlogged_tables']:
            unlogged_issues.append({
                "table": table,
                "severity": "warning",
                "issue": f"Table '{table}' is UNLOGGED — cannot be replicated by pglogical",
                "action": f"ALTER TABLE {table} SET LOGGED before replication"
            })
            warnings.append(f"unlogged_{table}")
    else:
        console.print("[green]No unlogged tables ✅[/green]")

    # Large objects
    console.print("[bold blue]Checking large objects...[/bold blue]")
    lo_issues = []
    if source['large_objects_count'] > 0:
        lo_issues.append({
            "severity": "warning",
            "issue": f"{source['large_objects_count']} large objects found — pglogical cannot replicate these",
            "action": "Migrate large objects separately using pg_dump or convert to bytea"
        })
        warnings.append("large_objects")
    else:
        console.print("[green]No large objects ✅[/green]")

    # Sequences
    console.print("[bold blue]Checking sequences...[/bold blue]")
    if source['sequences']:
        console.print(f"[yellow]Found {len(source['sequences'])} sequence(s) — sync manually after CDC catches up[/yellow]")
        warnings.append("sequences")

    # Extensions check
    console.print("[bold blue]Checking extensions...[/bold blue]")
    missing_extensions = []
    for ext in source['extensions']:
        if ext in ['pglogical', 'plpgsql']:
            continue
        if ext not in ORACLE_PG_SUPPORTED_EXTENSIONS:
            sev = "critical" if ext in ["timescaledb", "postgis"] else "warning"
            missing_extensions.append({
                "name": ext,
                "severity": sev,
                "reason": f"{ext} may not be available on Oracle Managed PostgreSQL",
                "action": f"Verify {ext} on target or find alternative"
            })
            if sev == "critical":
                issues.append(f"extension_{ext}")
            else:
                warnings.append(f"extension_{ext}")

    # pg_dump / pg_restore availability
    console.print("[bold blue]Checking pg_dump/pg_restore availability...[/bold blue]")
    pg_dump_ok = check_tool_available('pg_dump')
    pg_restore_ok = check_tool_available('pg_restore')
    initial_load_issues = []
    if not pg_dump_ok:
        initial_load_issues.append({
            "severity": "warning",
            "issue": "pg_dump not found on this machine",
            "action": "Install PostgreSQL client tools: brew install libpq"
        })
        warnings.append("pg_dump_missing")
    else:
        console.print("[green]pg_dump available ✅[/green]")
    if not pg_restore_ok:
        initial_load_issues.append({
            "severity": "warning",
            "issue": "pg_restore not found on this machine",
            "action": "Install PostgreSQL client tools: brew install libpq"
        })
        warnings.append("pg_restore_missing")
    else:
        console.print("[green]pg_restore available ✅[/green]")

    # Target readiness
    console.print("[bold blue]Checking target readiness...[/bold blue]")
    source_tables = [t['name'] for t in source['tables']]
    target_tables = [t['name'] for t in target['tables']]
    missing_tables = [t for t in source_tables if t not in target_tables]
    target_issues = []
    if missing_tables:
        target_issues.append({
            "severity": "critical",
            "issue": f"{len(missing_tables)} tables missing on target: {', '.join(missing_tables)}",
            "action": "Run pg_dump --schema-only on source then pg_restore on target before starting pglogical"
        })
        issues.append("missing_tables")


    if target['pglogical_nodes']:
        target_issues.append({
            "severity": "warning",
            "issue": f"Existing pglogical nodes on target: {', '.join(target['pglogical_nodes'])}",
            "action": "Drop existing nodes before creating new subscription"
        })
        warnings.append("existing_nodes")

    if target['pglogical_subscriptions']:
        target_issues.append({
            "severity": "warning",
            "issue": f"Existing subscriptions on target: {', '.join(target['pglogical_subscriptions'])}",
            "action": "Drop existing subscriptions before creating new one"
        })
        warnings.append("existing_subscriptions")

   # SSL mode check
    console.print("[bold blue]Checking SSL configuration for VPN...[/bold blue]")
    ssl_issues = []
    src_sslmode = source_config.get('sslmode', 'prefer')
    if src_sslmode not in ('disable', 'require'):
        ssl_issues.append({
            "severity": "warning",
            "issue": f"AWS_RDS_SSLMODE={src_sslmode} — for VPN connections use disable or require",
            "action": "Set AWS_RDS_SSLMODE=disable in .env if connecting over VPN"
        })
        warnings.append("sslmode")
    else:
        console.print(f"[green]AWS_RDS_SSLMODE={src_sslmode} ✅[/green]")

    # Parameter differences
    param_differences = []
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
            warnings.append(param)

    critical_count = len(issues)
    warning_count = len(warnings)

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
        "network": {
            "source_to_target": {"reachable": src_reachable, "latency_ms": src_latency},
            "target_to_source": {"reachable": None, "note": "Verify connectivity from target side manually"},
            "issues": network_issues
        },
        "dns": {
            "source_resolves": "dns_source" not in issues,
            "target_resolves": "dns_target" not in issues,
            "oracle_fqdn": oracle_fqdn,
            "issues": dns_issues
        },
        "ssl_config": {
            "source_sslmode": src_sslmode,
            "issues": ssl_issues
        },
        
        "pglogical": {
            "source_available": source['pglogical_available'],
            "source_installed": source['pglogical_installed'],
            "target_available": target['pglogical_available'],
            "target_installed": target['pglogical_installed'],
            "issues": pglogical_issues
        },
        "wal_level": {
            "source": src_wal,
            "target": tgt_wal,
            "issues": wal_issues
        },
        "replication": {
            "max_wal_senders": max_wal_senders,
            "max_replication_slots": max_repl_slots,
            "track_commit_timestamp": tct,
            "has_replication_privilege": source['has_replication_privilege'],
            "existing_slots": source['replication_slots'],
            "issues": replication_issues
        },
        "primary_keys": {
            "tables_without_pk": source['tables_without_pk'],
            "issues": pk_issues
        },
        "unlogged_tables": {
            "tables": source['unlogged_tables'],
            "issues": unlogged_issues
        },
        "large_objects": {
            "count": source['large_objects_count'],
            "issues": lo_issues
        },
        "sequences": {
            "source_sequences": source['sequences'],
            "count": len(source['sequences']),
            "note": "pglogical does not replicate sequences automatically",
            "action": "Sync sequences manually after CDC catches up to source"
        },
        "extensions": {
            "source_extensions": source['extensions'],
            "available_on_target": ORACLE_PG_SUPPORTED_EXTENSIONS,
            "missing_on_target": missing_extensions
        },
        "initial_load": {
            "pg_dump_available": pg_dump_ok,
            "pg_restore_available": pg_restore_ok,
            "issues": initial_load_issues
        },
        "target_readiness": {
            "source_tables": source_tables,
            "target_tables": target_tables,
            "tables_missing_on_target": missing_tables,
            "existing_nodes": target['pglogical_nodes'],
            "existing_subscriptions": target['pglogical_subscriptions'],
            "issues": target_issues
        },
        "ddl_warning": {
            "message": "pglogical does NOT replicate DDL changes automatically",
            "action": "Any schema changes during replication must be applied manually on both source and target"
        },
        "post_restore_steps": [
            "Run VACUUM ANALYZE on target after pg_restore completes",
            "Sync sequences manually after CDC catches up",
            "Set synchronize_data=false in pglogical subscription — data already loaded by pg_dump",
            "Verify row counts match on all tables before cutover",
            "Plan a controlled cutover — pause writes, let CDC catch up, then redirect apps"
        ],
        "parameters": {
            "differences": param_differences
        },
        "ssl": {
            "source_requires_ssl": False,
            "target_requires_ssl": True,
            "severity": "warning",
            "action": "Enable SSL in application connection strings before cutover"
        },
        "summary": {
            "total_checks": 15,
            "critical": critical_count,
            "warnings": warning_count,
            "passed": max(0, 15 - critical_count - warning_count),
            "ready_to_migrate": critical_count == 0,
            "blocker_reason": f"{critical_count} critical issues must be resolved" if critical_count > 0 else None
        }
    }


def print_summary(result):
    summary = result['summary']
    console.print("\n" + "─" * 55)
    console.print("[bold magenta]  Meridian — Pre-flight Migration Checklist[/bold magenta]")
    console.print("─" * 55)
    console.print(f"  Source DB:  {result.get('source_db', 'unknown')}")
    console.print(f"             {result['postgresql_versions']['source']}")
    console.print(f"  Target DB:  {result.get('target_db', 'unknown')}")
    console.print(f"             {result['postgresql_versions']['target']}")
    console.print(f"  Analyzed:   {result['analyzed_at']}")
    console.print()
    console.print(f"  [red]❌ Critical issues:  {summary['critical']}[/red]")
    console.print(f"  [yellow]⚠️  Warnings:         {summary['warnings']}[/yellow]")
    console.print(f"  [green]✅ Passed checks:    {summary['passed']}[/green]")
    console.print()

    # Network
    console.print("  [bold]Network:[/bold]")
    net = result.get('network', {})
    if net.get('source_to_target', {}).get('reachable'):
        ms = net['source_to_target'].get('latency_ms', 0)
        console.print(f"  [green]✅ Source → Target reachable ({ms}ms)[/green]")
    else:
        console.print(f"  [red]❌ Source → Target unreachable[/red]")
    for issue in net.get('issues', []):
        console.print(f"  [red]❌ {issue['issue']}[/red]")
        console.print(f"     Action: {issue['action']}")


    # DNS
    dns = result.get('dns', {})
    if dns:
        console.print("\n  [bold]DNS resolution:[/bold]")
        if dns.get('source_resolves'):
            console.print("  [green]✅ Source hostname resolves[/green]")
        else:
            console.print("  [red]❌ Source hostname cannot be resolved[/red]")
        if dns.get('target_resolves'):
            console.print("  [green]✅ Target hostname resolves[/green]")
        else:
            console.print("  [red]❌ Target hostname cannot be resolved[/red]")
        if dns.get('oracle_fqdn'):
            console.print(f"  [green]✅ ORACLE_PG_FQDN={dns['oracle_fqdn']}[/green]")
        for issue in dns.get('issues', []):
            if issue['severity'] == 'critical':
                console.print(f"  [red]❌ {issue['issue']}[/red]")
            else:
                console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")
            console.print(f"     Action: {issue['action']}")


    # pglogical
    console.print("\n  [bold]pglogical:[/bold]")
    pg = result.get('pglogical', {})
    if not pg.get('issues'):
        console.print("  [green]✅ pglogical installed on source and target[/green]")
    else:
        for issue in pg.get('issues', []):
            if issue['severity'] == 'critical':
                console.print(f"  [red]❌ {issue['issue']}[/red]")
            else:
                console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")
            console.print(f"     Action: {issue['action']}")

    # wal_level
    console.print("\n  [bold]WAL level:[/bold]")
    wal = result.get('wal_level', {})
    if not wal.get('issues'):
        console.print(f"  [green]✅ wal_level=logical on source and target[/green]")
    else:
        for issue in wal.get('issues', []):
            console.print(f"  [red]❌ wal_level={issue['current']} on {issue['location']} — needs {issue['required']}[/red]")
            console.print(f"     Action: {issue['action']}")

    # Replication parameters
    console.print("\n  [bold]Replication parameters:[/bold]")
    repl = result.get('replication', {})
    if not repl.get('issues'):
        console.print(f"  [green]✅ max_wal_senders={repl.get('max_wal_senders')} · max_replication_slots={repl.get('max_replication_slots')}[/green]")
    else:
        for issue in repl.get('issues', []):
            if issue['severity'] == 'critical':
                console.print(f"  [red]❌ {issue['issue']}[/red]")
            else:
                console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")
            console.print(f"     Action: {issue['action']}")

    # Primary keys
    console.print("\n  [bold]Primary keys:[/bold]")
    pk = result.get('primary_keys', {})
    if not pk.get('issues'):
        console.print("  [green]✅ All tables have primary keys[/green]")
    else:
        for issue in pk.get('issues', []):
            console.print(f"  [red]❌ {issue['issue']}[/red]")
            console.print(f"     Action: {issue['action']}")

    # Unlogged tables
    ul = result.get('unlogged_tables', {})
    if ul.get('issues'):
        console.print("\n  [bold]Unlogged tables:[/bold]")
        for issue in ul.get('issues', []):
            console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")
            console.print(f"     Action: {issue['action']}")

    # Large objects
    lo = result.get('large_objects', {})
    if lo.get('issues'):
        console.print("\n  [bold]Large objects:[/bold]")
        for issue in lo.get('issues', []):
            console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")
            console.print(f"     Action: {issue['action']}")

    # Sequences
    seq = result.get('sequences', {})
    console.print("\n  [bold]Sequences:[/bold]")
    if seq.get('count', 0) > 0:
        console.print(f"  [yellow]⚠️  {seq['count']} sequence(s) found — {seq['note']}[/yellow]")
        console.print(f"     Action: {seq['action']}")
    else:
        console.print("  [green]✅ No sequences to sync[/green]")

    # Extensions
    ext = result.get('extensions', {})
    if ext.get('missing_on_target'):
        console.print("\n  [bold]Extensions:[/bold]")
        for e in ext['missing_on_target']:
            if e['severity'] == 'critical':
                console.print(f"  [red]❌ {e['name']}: {e['reason']}[/red]")
            else:
                console.print(f"  [yellow]⚠️  {e['name']}: {e['action']}[/yellow]")

    # Initial load tools
    il = result.get('initial_load', {})
    if il.get('issues'):
        console.print("\n  [bold]Initial load tools:[/bold]")
        for issue in il.get('issues', []):
            console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")
            console.print(f"     Action: {issue['action']}")
    else:
        console.print("\n  [bold]Initial load tools:[/bold]")
        console.print("  [green]✅ pg_dump and pg_restore available[/green]")

    # Target readiness
    tr = result.get('target_readiness', {})
    console.print("\n  [bold]Target readiness:[/bold]")
    if tr.get('tables_missing_on_target'):
        console.print(f"  [red]❌ Tables missing on target: {', '.join(tr['tables_missing_on_target'])}[/red]")
        console.print("     Run to fix:")
        console.print(f"     [dim]meridian fix-schema --env[/dim]")
        console.print("     Or manually:")
        console.print(f"     [dim]pg_dump -h <source-host> -U <user> -d <db> --schema-only --no-owner --no-privileges -f schema.sql[/dim]")
        console.print(f"     [dim]psql \"host=<target-host> dbname=<db> sslmode=require\" -f schema.sql[/dim]")
    else:
        console.print("  [green]✅ All source tables exist on target[/green]")
    for issue in tr.get('issues', []):
        if 'missing' not in issue['issue']:
            console.print(f"  [yellow]⚠️  {issue['issue']}[/yellow]")

    # SSL
    ssl = result.get('ssl', {})
    if ssl and not ssl.get('source_requires_ssl') and ssl.get('target_requires_ssl'):
        console.print(f"\n  [yellow]⚠️  SSL: {ssl['action']}[/yellow]")

    # DDL warning
    ddl = result.get('ddl_warning', {})
    if ddl:
        console.print(f"\n  [yellow]⚠️  DDL: {ddl['message']}[/yellow]")
        console.print(f"     Action: {ddl['action']}")

    # Parameter differences
    params = result.get('parameters', {})
    if params.get('differences'):
        console.print("\n  [bold]Parameter differences:[/bold]")
        for p in params['differences']:
            console.print(f"  [yellow]⚠️  {p['parameter']}: source={p['source_value']} target={p['target_value']}[/yellow]")

    # Post restore steps
    steps = result.get('post_restore_steps', [])
    if steps:
        console.print("\n  [bold]Post-restore checklist:[/bold]")
        for step in steps:
            console.print(f"  📋 {step}")

    # Final verdict
    console.print()
    if summary['ready_to_migrate']:
        console.print("  [bold green]✅ READY TO REPLICATE WITH PGLOGICAL[/bold green]")
        console.print("  [green]Next: Run pg_dump/pg_restore for initial load, then start pglogical CDC[/green]")
    else:
        console.print("  [bold red]🚫 NOT READY TO MIGRATE[/bold red]")
        if summary.get('blocker_reason'):
            console.print(f"  [red]{summary['blocker_reason']}[/red]")

    console.print("─" * 55)


def analyze(mock=False, source_db=None, target_db=None,
            source_config=None, target_config=None):
    console.print(f"\n[bold magenta]Meridian — Pre-flight Migration Checklist[/bold magenta]")
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