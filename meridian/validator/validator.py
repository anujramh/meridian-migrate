import time
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

console = Console()

MOCK_DATA = {
    "validation_id": "val-mock-20260401",
    "source_db": None,
    "target_db": None,
    "started_at": None,
    "status": "passed",
    "tables": [
        {
            "name": "users",
            "source_rows": 1000,
            "target_rows": 1000,
            "source_checksum": "a1b2c3d4e5f6",
            "target_checksum": "a1b2c3d4e5f6",
            "row_count_match": True,
            "checksum_match": True,
            "status": "passed"
        },
        {
            "name": "orders",
            "source_rows": 5000,
            "target_rows": 5000,
            "source_checksum": "b2c3d4e5f6a1",
            "target_checksum": "b2c3d4e5f6a1",
            "row_count_match": True,
            "checksum_match": True,
            "status": "passed"
        },
        {
            "name": "products",
            "source_rows": 500,
            "target_rows": 498,
            "source_checksum": "c3d4e5f6a1b2",
            "target_checksum": "c3d4e5f6a1b3",
            "row_count_match": False,
            "checksum_match": False,
            "status": "failed",
            "drift": {
                "missing_rows": 2,
                "action": "Re-sync 2 missing rows from source before cutover"
            }
        }
    ],
    "summary": {
        "total_tables": 3,
        "passed": 2,
        "failed": 1,
        "total_source_rows": 6500,
        "total_target_rows": 6498,
        "drift_rows": 2,
        "ready_for_cutover": False,
        "blocker_reason": "1 table failed parity check — 2 rows missing on target"
    }
}


def get_table_count(conn, table_name):
    """Get exact row count for a table."""
    import psycopg2
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def get_table_checksum(conn, table_name):
    """Get MD5 checksum of all rows in a table."""
    import psycopg2
    cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT md5(string_agg(row_hash, ',' ORDER BY row_hash))
            FROM (
                SELECT md5(CAST(t.* AS text)) as row_hash
                FROM {table_name} t
            ) sub
        """)
        result = cur.fetchone()[0]
        return result or 'empty'
    except Exception as e:
        return f"error: {e}"


def get_public_tables(conn):
    """Get all tables in public schema."""
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    return [r[0] for r in cur.fetchall()]


def validate_real(source_config, target_config):
    """Run real parity validation between source and target."""
    import psycopg2

    console.print("[bold blue]Connecting to source database...[/bold blue]")
    src_conn = psycopg2.connect(
        host=source_config['host'],
        port=source_config['port'],
        database=source_config['database'],
        user=source_config['user'],
        password=source_config['password'],
        sslmode=source_config.get('sslmode', 'prefer')
    )
    console.print(f"[green]Connected to source ✅[/green]")

    console.print("[bold blue]Connecting to target database...[/bold blue]")
    tgt_conn = psycopg2.connect(
        host=target_config['host'],
        port=target_config['port'],
        database=target_config['database'],
        user=target_config['user'],
        password=target_config['password'],
        sslmode=target_config.get('sslmode', 'require')
    )
    console.print(f"[green]Connected to target ✅[/green]\n")

    # Get tables
    source_tables = get_public_tables(src_conn)
    target_tables = get_public_tables(tgt_conn)
    common_tables = [t for t in source_tables if t in target_tables]

    console.print(f"[bold blue]Validating {len(common_tables)} tables...[/bold blue]\n")

    tables_result = []
    total_source_rows = 0
    total_target_rows = 0
    passed = 0
    failed = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console
    ) as progress:

        for table in common_tables:
            task = progress.add_task(f"Validating {table}", total=100)

            # Row count
            progress.update(task, completed=30)
            src_count = get_table_count(src_conn, table)
            tgt_count = get_table_count(tgt_conn, table)

            # Checksum
            progress.update(task, completed=70)
            src_checksum = get_table_checksum(src_conn, table)
            tgt_checksum = get_table_checksum(tgt_conn, table)

            progress.update(task, completed=100)

            row_match = src_count == tgt_count
            checksum_match = src_checksum == tgt_checksum
            status = "passed" if row_match and checksum_match else "failed"

            if status == "passed":
                console.print(f"[green]✅ {table} — {src_count:,} rows · checksum match[/green]")
                passed += 1
            else:
                console.print(f"[red]❌ {table} — source={src_count:,} target={tgt_count:,} · {'checksum mismatch' if not checksum_match else 'row count mismatch'}[/red]")
                failed += 1

            total_source_rows += src_count
            total_target_rows += tgt_count

            table_result = {
                "name": table,
                "source_rows": src_count,
                "target_rows": tgt_count,
                "source_checksum": src_checksum,
                "target_checksum": tgt_checksum,
                "row_count_match": row_match,
                "checksum_match": checksum_match,
                "status": status
            }

            if not row_match:
                table_result["drift"] = {
                    "missing_rows": abs(src_count - tgt_count),
                    "action": f"Re-sync {abs(src_count - tgt_count)} missing rows before cutover"
                }

            tables_result.append(table_result)

    src_conn.close()
    tgt_conn.close()

    drift_rows = abs(total_source_rows - total_target_rows)
    ready = failed == 0

    return {
        "validation_id": f"val-{source_config['database']}-to-{target_config['database']}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "source_db": source_config['database'],
        "target_db": target_config['database'],
        "started_at": datetime.utcnow().isoformat(),
        "status": "passed" if ready else "failed",
        "tables": tables_result,
        "summary": {
            "total_tables": len(common_tables),
            "passed": passed,
            "failed": failed,
            "total_source_rows": total_source_rows,
            "total_target_rows": total_target_rows,
            "drift_rows": drift_rows,
            "ready_for_cutover": ready,
            "blocker_reason": f"{failed} table(s) failed parity check" if not ready else None
        }
    }


def simulate_validation(source_db, target_db):
    console.print("[bold yellow]Running in mock mode — simulating parity validation[/bold yellow]\n")
    console.print("[bold blue]Starting parity validation...[/bold blue]")
    console.print("[green]Connecting to source and target databases[/green]\n")

    result = MOCK_DATA.copy()
    result['source_db'] = source_db
    result['target_db'] = target_db
    result['started_at'] = datetime.utcnow().isoformat()
    result['tables'] = [t.copy() for t in MOCK_DATA['tables']]

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console
    ) as progress:
        for table in result['tables']:
            task = progress.add_task(f"Validating {table['name']}", total=100)
            for i in range(0, 101, 10):
                progress.update(task, completed=i)
                time.sleep(0.03)
            if table['status'] == 'passed':
                console.print(f"[green]✅ {table['name']} — {table['source_rows']:,} rows match · checksum match[/green]")
            else:
                console.print(f"[red]❌ {table['name']} — DRIFT DETECTED · source={table['source_rows']:,} target={table['target_rows']:,}[/red]")

    return result


def print_summary(result):
    summary = result['summary']
    console.print("\n" + "─" * 50)
    console.print("[bold magenta]  Meridian — Parity Validation Summary[/bold magenta]")
    console.print("─" * 50)
    console.print(f"  Source DB:  {result['source_db']}")
    console.print(f"  Target DB:  {result['target_db']}")
    console.print(f"  Validated:  {result['started_at']}")
    console.print()
    console.print(f"  Total tables:  {summary['total_tables']}")
    console.print(f"  [green]✅ Passed:      {summary['passed']}[/green]")
    console.print(f"  [red]❌ Failed:      {summary['failed']}[/red]")
    console.print()
    console.print(f"  Source rows:  {summary['total_source_rows']:,}")
    console.print(f"  Target rows:  {summary['total_target_rows']:,}")
    console.print(f"  Drift rows:   {summary['drift_rows']:,}")
    console.print()

    if summary['failed'] > 0:
        console.print("  [bold red]FAILED TABLES:[/bold red]")
        for table in result['tables']:
            if table['status'] == 'failed':
                console.print(f"  [red]❌ {table['name']}[/red]")
                console.print(f"     Source: {table['source_rows']:,} rows")
                console.print(f"     Target: {table['target_rows']:,} rows")
                if 'drift' in table:
                    console.print(f"     Action: {table['drift']['action']}")

    console.print()
    if summary['ready_for_cutover']:
        console.print("  [bold green]✅ PARITY CONFIRMED — ready for cutover[/bold green]")
        console.print("  [green]Next: Run meridian cutover --env[/green]")
    else:
        console.print("  [bold red]🚫 NOT READY FOR CUTOVER[/bold red]")
        console.print(f"  [red]{summary['blocker_reason']}[/red]")

    console.print("─" * 50)


def validate(source_db=None, target_db=None, mock=False,
             source_config=None, target_config=None):
    console.print(f"\n[bold magenta]Meridian — Parity Validator[/bold magenta]")
    console.print(f"  Source: [yellow]{source_db}[/yellow]")
    console.print(f"  Target: [yellow]{target_db}[/yellow]\n")

    if mock:
        return simulate_validation(source_db, target_db)

    if not source_config or not target_config:
        console.print("[red]Real mode requires source and target config — use --env[/red]")
        return None

    return validate_real(source_config, target_config)