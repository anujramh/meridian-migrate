import time
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

console = Console()

MOCK_DATA = {
    "cutover_id": "cutover-prod-postgres-01-to-prod-carsdk-cluster-20260326",
    "source_db": None,
    "target_db": None,
    "started_at": None,
    "completed_at": None,
    "status": "completed",
    "steps": [
        {
            "step": 1,
            "name": "Final parity check",
            "status": "passed",
            "duration_seconds": 12,
            "details": "All tables match — 0 drift rows"
        },
        {
            "step": 2,
            "name": "Stop writes to source",
            "status": "passed",
            "duration_seconds": 2,
            "details": "Source database set to read-only"
        },
        {
            "step": 3,
            "name": "Apply final CDC events",
            "status": "passed",
            "duration_seconds": 3,
            "details": "Applied 47 final CDC events — target fully in sync"
        },
        {
            "step": 4,
            "name": "Final checksum verification",
            "status": "passed",
            "duration_seconds": 8,
            "details": "All checksums match — source and target identical"
        },
        {
            "step": 5,
            "name": "Update connection strings",
            "status": "passed",
            "duration_seconds": 1,
            "details": "Application connection strings updated to target"
        },
        {
            "step": 6,
            "name": "Enable writes on target",
            "status": "passed",
            "duration_seconds": 1,
            "details": "Target database accepting reads and writes"
        },
        {
            "step": 7,
            "name": "Health check",
            "status": "passed",
            "duration_seconds": 5,
            "details": "Application responding — error rate 0.0%"
        },
        {
            "step": 8,
            "name": "Disable source database",
            "status": "passed",
            "duration_seconds": 1,
            "details": "Source database disabled — migration complete"
        }
    ],
    "rollback": {
        "available": True,
        "triggered": False,
        "reason": None
    },
    "summary": {
        "total_steps": 8,
        "passed": 8,
        "failed": 0,
        "downtime_seconds": 6,
        "success": True
    }
}


def simulate_cutover(source_db, target_db):
    console.print("[bold yellow]Running in mock mode — simulating cutover[/bold yellow]\n")
    console.print("[bold red]⚠️  CUTOVER STARTING — this is the point of no easy return[/bold red]")
    console.print("[yellow]Rollback available for 30 minutes after cutover completes[/yellow]\n")

    result = MOCK_DATA.copy()
    result['source_db'] = source_db
    result['target_db'] = target_db
    result['started_at'] = datetime.utcnow().isoformat()
    result['steps'] = [s.copy() for s in MOCK_DATA['steps']]

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        TimeElapsedColumn(),
        console=console
    ) as progress:

        for step in result['steps']:
            task = progress.add_task(
                f"Step {step['step']}: {step['name']}",
                total=None
            )
            time.sleep(0.3)
            progress.stop_task(task)
            progress.update(task, visible=False)

            if step['status'] == 'passed':
                console.print(f"[green]✅ Step {step['step']}: {step['name']} — {step['details']}[/green]")
            else:
                console.print(f"[red]❌ Step {step['step']}: {step['name']} — {step['details']}[/red]")
                console.print(f"[bold red]ROLLBACK TRIGGERED — restoring source database[/bold red]")
                result['rollback']['triggered'] = True
                result['rollback']['reason'] = step['details']
                result['status'] = 'rolled_back'
                result['summary']['success'] = False
                break

    result['completed_at'] = datetime.utcnow().isoformat()
    return result


def print_summary(result):
    summary = result['summary']
    console.print("\n" + "─" * 50)
    console.print("[bold magenta]  Meridian — Cutover Summary[/bold magenta]")
    console.print("─" * 50)
    console.print(f"  Source DB:   {result['source_db']}")
    console.print(f"  Target DB:   {result['target_db']}")
    console.print(f"  Started:     {result['started_at']}")
    console.print(f"  Completed:   {result['completed_at']}")
    console.print()
    console.print(f"  Total steps:      {summary['total_steps']}")
    console.print(f"  [green]✅ Passed:         {summary['passed']}[/green]")
    console.print(f"  [red]❌ Failed:         {summary['failed']}[/red]")
    console.print(f"  Downtime:         {summary['downtime_seconds']} seconds")
    console.print()

    if result['rollback']['triggered']:
        console.print("  [bold red]🔄 ROLLBACK TRIGGERED[/bold red]")
        console.print(f"  [red]Reason: {result['rollback']['reason']}[/red]")
        console.print("  [yellow]Source database restored — no data loss[/yellow]")
    elif summary['success']:
        console.print("  [bold green]🎉 CUTOVER COMPLETE — migration successful![/bold green]")
        console.print(f"  [green]Downtime: {summary['downtime_seconds']} seconds[/green]")
        console.print(f"  [green]Rollback available for 30 minutes[/green]")

    console.print("─" * 50)


def cutover(source_db, target_db, mock=False):
    console.print(f"\n[bold magenta]Meridian — Cutover Orchestrator[/bold magenta]")
    console.print(f"  Source: [yellow]{source_db}[/yellow]")
    console.print(f"  Target: [yellow]{target_db}[/yellow]\n")

    if mock:
        return simulate_cutover(source_db, target_db)

    console.print("[red]Real mode not yet implemented — use --mock for now[/red]")
    return None