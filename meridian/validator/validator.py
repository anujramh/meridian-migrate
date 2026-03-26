import time
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

console = Console()

MOCK_DATA = {
    "validation_id": "val-prod-postgres-01-to-prod-carsdk-cluster-20260326",
    "source_db": None,
    "target_db": None,
    "started_at": None,
    "status": "passed",
    "tables": [
        {
            "name": "users",
            "source_rows": 1250000,
            "target_rows": 1250000,
            "source_checksum": "a1b2c3d4e5f6",
            "target_checksum": "a1b2c3d4e5f6",
            "row_count_match": True,
            "checksum_match": True,
            "sample_match": True,
            "status": "passed"
        },
        {
            "name": "orders",
            "source_rows": 8500000,
            "target_rows": 8500000,
            "source_checksum": "b2c3d4e5f6a1",
            "target_checksum": "b2c3d4e5f6a1",
            "row_count_match": True,
            "checksum_match": True,
            "sample_match": True,
            "status": "passed"
        },
        {
            "name": "products",
            "source_rows": 45000,
            "target_rows": 45000,
            "source_checksum": "c3d4e5f6a1b2",
            "target_checksum": "c3d4e5f6a1b2",
            "row_count_match": True,
            "checksum_match": True,
            "sample_match": True,
            "status": "passed"
        },
        {
            "name": "payments",
            "source_rows": 6200000,
            "target_rows": 6199998,
            "source_checksum": "d4e5f6a1b2c3",
            "target_checksum": "d4e5f6a1b2c4",
            "row_count_match": False,
            "checksum_match": False,
            "sample_match": False,
            "status": "failed",
            "drift": {
                "missing_rows": 2,
                "action": "Re-sync 2 missing rows from source before cutover"
            }
        },
        {
            "name": "sessions",
            "source_rows": 320000,
            "target_rows": 320000,
            "source_checksum": "e5f6a1b2c3d4",
            "target_checksum": "e5f6a1b2c3d4",
            "row_count_match": True,
            "checksum_match": True,
            "sample_match": True,
            "status": "passed"
        }
    ],
    "summary": {
        "total_tables": 5,
        "passed": 4,
        "failed": 1,
        "total_source_rows": 16315000,
        "total_target_rows": 16314998,
        "drift_rows": 2,
        "ready_for_cutover": False,
        "blocker_reason": "1 table failed parity check — 2 rows missing on target"
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
            task = progress.add_task(
                f"Validating {table['name']}",
                total=100
            )
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
    else:
        console.print("  [bold red]🚫 NOT READY FOR CUTOVER[/bold red]")
        console.print(f"  [red]{summary['blocker_reason']}[/red]")

    console.print("─" * 50)


def validate(source_db, target_db, mock=False):
    console.print(f"\n[bold magenta]Meridian — Parity Validator[/bold magenta]")
    console.print(f"  Source: [yellow]{source_db}[/yellow]")
    console.print(f"  Target: [yellow]{target_db}[/yellow]\n")

    if mock:
        return simulate_validation(source_db, target_db)

    console.print("[red]Real mode not yet implemented — use --mock for now[/red]")
    return None