import json
import os
import psutil
from datetime import datetime
from rich.console import Console

console = Console()

STATE_FILE = "meridian.state.json"
LOG_FILE = "meridian.log"


def get_meridian_version():
    try:
        from meridian import __version__
        return __version__
    except Exception:
        return "0.1.0"


def get_table_info(source_config):
    """Get table inventory with PKs, FKs, indexes and row counts."""
    import psycopg2
    conn = psycopg2.connect(
        host=source_config['host'],
        port=source_config['port'],
        database=source_config['database'],
        user=source_config['user'],
        password=source_config['password'],
        sslmode=source_config.get('sslmode', 'prefer')
    )
    cur = conn.cursor()


    # Get tables with row counts
    cur.execute("""
        SELECT
            t.table_name,
            (xpath('/row/cnt/text()',
                query_to_xml('SELECT COUNT(*) AS cnt FROM public.' || t.table_name,
                false, true, '')))[1]::text::int as row_count,
            c.relpersistence = 'u' as is_unlogged
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
            AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """)
    tables_raw = cur.fetchall()

    # Get primary keys
    cur.execute("""
        SELECT
            tc.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
    """)
    pks = {r[0]: r[1] for r in cur.fetchall()}

    # Get foreign keys
    cur.execute("""
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS references_table,
            ccu.column_name AS references_column,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
    """)
    fks_raw = cur.fetchall()

    # Group FKs by table
    fks = {}
    for row in fks_raw:
        table, col, ref_table, ref_col, constraint = row
        if table not in fks:
            fks[table] = []
        fks[table].append({
            "column": col,
            "references_table": ref_table,
            "references_column": ref_col,
            "constraint_name": constraint
        })

    # Get indexes
    cur.execute("""
        SELECT
            t.relname as table_name,
            i.relname as index_name
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE n.nspname = 'public'
        AND t.relkind = 'r'
        ORDER BY t.relname, i.relname
    """)
    indexes_raw = cur.fetchall()

    indexes = {}
    for table, index in indexes_raw:
        if table not in indexes:
            indexes[table] = []
        indexes[table].append(index)

    conn.close()

    # Build table list
    tables = []
    for table_name, row_count, is_unlogged in tables_raw:
        tables.append({
            "name": table_name,
            "source_rows": row_count,
            "restored_rows": None,
            "status": "pending",
            "primary_key": pks.get(table_name),
            "has_pk": table_name in pks,
            "foreign_keys": fks.get(table_name, []),
            "indexes": indexes.get(table_name, []),
            "is_unlogged": bool(is_unlogged)
        })

    return tables


def compute_restore_order(tables):
    """Compute table restore order based on FK dependencies."""
    # Build dependency graph
    deps = {t['name']: set() for t in tables}
    for table in tables:
        for fk in table['foreign_keys']:
            deps[table['name']].add(fk['references_table'])

    # Topological sort
    order = []
    visited = set()

    def visit(table):
        if table in visited:
            return
        visited.add(table)
        for dep in deps.get(table, set()):
            visit(dep)
        order.append(table)

    for table in deps:
        visit(table)

    return order


def create_state(source_config, target_config, mode="foreground"):
    """Create a new state file for a migration."""
    migration_id = f"mig-{source_config['database']}-to-{target_config['database']}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    console.print("[bold blue]Collecting table inventory...[/bold blue]")
    tables = get_table_info(source_config)
    restore_order = compute_restore_order(tables)

    pre_migration = {t['name']: t['source_rows'] for t in tables}
    pre_migration['total'] = sum(t['source_rows'] for t in tables)

    state = {
        "migration_id": migration_id,
        "meridian_version": get_meridian_version(),
        "mode": "pg_dump_pglogical",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "status": "running",
        "wal_lsn": None,
        "last_error": None,
        "source": {
            "host": source_config['host'],
            "database": source_config['database']
        },
        "target": {
            "host": target_config['host'],
            "database": target_config['database']
        },
        "process": {
            "mode": mode,
            "pid": os.getpid(),
            "started_at": datetime.utcnow().isoformat(),
            "log_file": LOG_FILE
        },
        "restore_order": restore_order,
        "tables": tables,
        "validation": {
            "pre_migration": pre_migration,
            "after_restore": None,
            "after_cdc": None,
            "pre_cutover": None,
            "foreign_key_check": {
                "status": "pending",
                "violations": []
            },
            "index_check": {
                "status": "pending",
                "missing_on_target": []
            }
        },
        "phases": {
            "replication_slot": _empty_phase(),
            "dump": _empty_phase(),
            "restore": _empty_phase(),
            "vacuum": _empty_phase(),
            "provider_setup": _empty_phase(),
            "subscriber_setup": _empty_phase(),
            "cdc_active": _empty_phase(),
            "validate": _empty_phase(),
            "cutover": _empty_phase(),
            "cleanup": _empty_phase()
        },
        "resume": {
            "can_resume": False,
            "reason": None
        }
    }

    save_state(state)
    console.print(f"[green]✅ State file created — {migration_id}[/green]")
    return state


def _empty_phase():
    return {
        "status": "pending",
        "started_at": None,
        "completed_at": None,
        "duration_seconds": None,
        "retry_count": 0,
        "error": None
    }


def save_state(state):
    """Save state to file."""
    state['updated_at'] = datetime.utcnow().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


def load_state():
    """Load existing state file."""
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, 'r') as f:
        return json.load(f)


def is_running(state):
    """Check if migration process is still running."""
    if not state:
        return False
    pid = state.get('process', {}).get('pid')
    if not pid:
        return False
    try:
        return psutil.pid_exists(pid) and psutil.Process(pid).status() != 'zombie'
    except Exception:
        return False


def get_resume_point(state):
    """Find first failed or pending phase to resume from."""
    phases = state.get('phases', {})
    for phase_name, phase_data in phases.items():
        if phase_data['status'] in ('failed', 'pending'):
            return phase_name
    return None


def phase_start(state, phase_name):
    """Mark a phase as started."""
    state['phases'][phase_name]['status'] = 'running'
    state['phases'][phase_name]['started_at'] = datetime.utcnow().isoformat()
    state['phases'][phase_name]['retry_count'] += 1
    save_state(state)
    return state


def phase_complete(state, phase_name, **kwargs):
    """Mark a phase as complete."""
    started = state['phases'][phase_name].get('started_at')
    duration = None
    if started:
        start_dt = datetime.fromisoformat(started)
        duration = round((datetime.utcnow() - start_dt).total_seconds())

    state['phases'][phase_name]['status'] = 'complete'
    state['phases'][phase_name]['completed_at'] = datetime.utcnow().isoformat()
    state['phases'][phase_name]['duration_seconds'] = duration
    state['phases'][phase_name]['error'] = None

    # Store any extra data
    for key, value in kwargs.items():
        state['phases'][phase_name][key] = value

    save_state(state)
    return state


def phase_fail(state, phase_name, error):
    """Mark a phase as failed."""
    state['phases'][phase_name]['status'] = 'failed'
    state['phases'][phase_name]['completed_at'] = datetime.utcnow().isoformat()
    state['phases'][phase_name]['error'] = str(error)
    state['last_error'] = {
        "phase": phase_name,
        "message": str(error),
        "at": datetime.utcnow().isoformat()
    }
    state['status'] = 'failed'
    state['resume'] = {
        "can_resume": True,
        "reason": f"Failed at {phase_name} — re-run to resume"
    }
    save_state(state)
    return state


def migration_complete(state):
    """Mark entire migration as complete."""
    state['status'] = 'complete'
    state['resume'] = {
        "can_resume": False,
        "reason": "Migration complete"
    }
    save_state(state)
    return state


def print_state(state):
    """Print state in human readable format."""
    console.print(f"\n[bold magenta]Meridian — Migration State[/bold magenta]")
    console.print(f"  Migration ID: {state['migration_id']}")
    console.print(f"  Created:      {state['created_at']}")
    console.print(f"  Updated:      {state['updated_at']}")
    console.print(f"  Mode:         {state['mode']}")

    status = state['status']
    if status == 'complete':
        console.print(f"  Status:       [bold green]✅ {status}[/bold green]")
    elif status == 'running':
        console.print(f"  Status:       [bold yellow]⏳ {status}[/bold yellow]")
    elif status == 'failed':
        console.print(f"  Status:       [bold red]❌ {status}[/bold red]")
    else:
        console.print(f"  Status:       {status}")

    if state.get('wal_lsn'):
        console.print(f"  WAL LSN:      {state['wal_lsn']}")

    if state.get('last_error'):
        console.print(f"\n  [bold red]Last error:[/bold red]")
        console.print(f"  Phase:   {state['last_error']['phase']}")
        console.print(f"  Message: {state['last_error']['message']}")
        console.print(f"  At:      {state['last_error']['at']}")

    console.print(f"\n  [bold]Source:[/bold] {state['source']['database']} @ {state['source']['host']}")
    console.print(f"  [bold]Target:[/bold] {state['target']['database']} @ {state['target']['host']}")

    console.print(f"\n  [bold]Tables:[/bold]")
    for table in state.get('tables', []):
        pk = "✅" if table['has_pk'] else "❌"
        fk_count = len(table['foreign_keys'])
        console.print(f"  {pk} {table['name']:<20} {table['source_rows']:>8,} rows  FKs: {fk_count}")

    console.print(f"\n  [bold]Restore order:[/bold] {' → '.join(state.get('restore_order', []))}")

    console.print(f"\n  [bold]Phases:[/bold]")
    for phase_name, phase_data in state.get('phases', {}).items():
        phase_status = phase_data['status']
        duration = f" ({phase_data['duration_seconds']}s)" if phase_data['duration_seconds'] else ""
        retries = f" [retry #{phase_data['retry_count']}]" if phase_data['retry_count'] > 1 else ""

        if phase_status == 'complete':
            console.print(f"  [green]✅ {phase_name:<20} complete{duration}[/green]")
        elif phase_status == 'running':
            console.print(f"  [yellow]⏳ {phase_name:<20} running{retries}[/yellow]")
        elif phase_status == 'failed':
            console.print(f"  [red]❌ {phase_name:<20} failed{retries} — {phase_data.get('error', '')}[/red]")
        else:
            console.print(f"  [dim]⬜ {phase_name:<20} pending[/dim]")

    resume = state.get('resume', {})
    if resume.get('can_resume'):
        console.print(f"\n  [yellow]▶ Resume: {resume['reason']}[/yellow]")
        console.print(f"  [yellow]  Run: meridian replicate --env[/yellow]")

    console.print()

