import time
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

console = Console()

MOCK_DATA = {
    "replication_id": "repl-prod-postgres-01-to-prod-carsdk-cluster-20260326",
    "source_db": None,
    "target_db": None,
    "started_at": None,
    "status": "running",
    "mode": "initial_snapshot + cdc",
    "tables": [
        {
            "name": "users",
            "rows_total": 1250000,
            "rows_copied": 0,
            "status": "pending"
        },
        {
            "name": "orders",
            "rows_total": 8500000,
            "rows_copied": 0,
            "status": "pending"
        },
        {
            "name": "products",
            "rows_total": 45000,
            "rows_copied": 0,
            "status": "pending"
        },
        {
            "name": "payments",
            "rows_total": 6200000,
            "rows_copied": 0,
            "status": "pending"
        },
        {
            "name": "sessions",
            "rows_total": 320000,
            "rows_copied": 0,
            "status": "pending"
        }
    ],
    "cdc": {
        "status": "running",
        "lag_seconds": 0,
        "events_captured": 0,
        "events_applied": 0
    },
    "summary": {
        "total_rows": 16315000,
        "rows_copied": 0,
        "progress_pct": 0,
        "elapsed_seconds": 0,
        "estimated_remaining_seconds": 0
    }
}


def simulate_replication(source_db, target_db):
    console.print("[bold yellow]Running in mock mode — simulating live replication[/bold yellow]\n")
    console.print(f"[bold blue]Starting initial snapshot...[/bold blue]")
    console.print(f"[green]CDC stream started — capturing live changes from source[/green]\n")

    result = MOCK_DATA.copy()
    result['source_db'] = source_db
    result['target_db'] = target_db
    result['started_at'] = datetime.utcnow().isoformat()
    result['tables'] = [t.copy() for t in MOCK_DATA['tables']]

    total_rows = sum(t['rows_total'] for t in result['tables'])
    cdc_events = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TextColumn("{task.completed:,}/{task.total:,} rows"),
        TimeElapsedColumn(),
        console=console
    ) as progress:

        for table in result['tables']:
            task = progress.add_task(
                f"Copying {table['name']}",
                total=table['rows_total']
            )
            table['status'] = 'copying'
            rows_done = 0
            chunk_size = max(1, table['rows_total'] // 20)

            while rows_done < table['rows_total']:
                chunk = min(chunk_size, table['rows_total'] - rows_done)
                rows_done += chunk
                table['rows_copied'] = rows_done
                cdc_events += 12
                progress.update(task, completed=rows_done)
                time.sleep(0.05)

            table['status'] = 'done'
            console.print(f"[green]✅ {table['name']} — {table['rows_total']:,} rows copied[/green]")

    console.print(f"\n[bold blue]Applying CDC backlog...[/bold blue]")
    console.print(f"[green]Applied {cdc_events:,} CDC events captured during snapshot[/green]")
    console.print(f"[bold green]CDC stream live — replication lag: 0s[/bold green]")

    result['cdc']['events_captured'] = cdc_events
    result['cdc']['events_applied'] = cdc_events
    result['cdc']['lag_seconds'] = 0
    result['summary']['total_rows'] = total_rows
    result['summary']['rows_copied'] = total_rows
    result['summary']['progress_pct'] = 100
    result['status'] = 'live'

    return result


def print_summary(result):
    console.print("\n" + "─" * 50)
    console.print("[bold magenta]  Meridian — Replication Summary[/bold magenta]")
    console.print("─" * 50)
    console.print(f"  Source DB:  {result['source_db']}")
    console.print(f"  Target DB:  {result['target_db']}")
    console.print(f"  Started:    {result['started_at']}")
    console.print(f"  Mode:       {result['mode']}")
    console.print()
    console.print(f"  Total rows: {result['summary']['total_rows']:,}")
    console.print(f"  Copied:     {result['summary']['rows_copied']:,}")
    console.print(f"  Progress:   {result['summary']['progress_pct']}%")
    console.print()
    console.print(f"  CDC status:          {result['cdc']['status']}")
    console.print(f"  CDC lag:             {result['cdc']['lag_seconds']}s")
    console.print(f"  CDC events captured: {result['cdc']['events_captured']:,}")
    console.print(f"  CDC events applied:  {result['cdc']['events_applied']:,}")
    console.print()
    if result['status'] == 'live':
        console.print("  [bold green]✅ REPLICATION LIVE — ready for parity validation[/bold green]")
    else:
        console.print(f"  [yellow]Status: {result['status']}[/yellow]")
    console.print("─" * 50)


def replicate(source_db, target_db, mock=False):
    console.print(f"\n[bold magenta]Meridian — Replication Engine[/bold magenta]")
    console.print(f"  Source: [yellow]{source_db}[/yellow]")
    console.print(f"  Target: [yellow]{target_db}[/yellow]\n")

    if mock:
        return simulate_replication(source_db, target_db)

    console.print("[red]Real mode not yet implemented — use --mock for now[/red]")
    return None