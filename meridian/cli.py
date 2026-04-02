import os
import click
import json
import boto3
from rich.console import Console
from meridian.scanners import aws
from meridian.scanners import oracle

console = Console()


def load_env_if_needed(env_flag):
    if env_flag:
        from dotenv import load_dotenv
        load_dotenv()


def get_aws_config(env, host=None, port=None, database=None, user=None, password=None, profile=None, region=None):
    load_env_if_needed(env)
    return {
        "host": host or os.getenv('AWS_RDS_HOST'),
        "port": int(port or os.getenv('AWS_RDS_PORT', 5432)),
        "database": database or os.getenv('AWS_RDS_DATABASE'),
        "user": user or os.getenv('AWS_RDS_USER'),
        "password": password or os.getenv('AWS_RDS_PASSWORD'),
        "profile": profile or os.getenv('AWS_PROFILE', 'meridian-readonly'),
        "region": region or os.getenv('AWS_REGION', 'us-east-1'),
        "sslmode": os.getenv('AWS_RDS_SSLMODE', 'prefer'),
        "sslrootcert": os.getenv('AWS_RDS_SSLROOTCERT', '')
    }


def get_oracle_config(env, host=None, port=None, database=None, user=None, password=None, profile=None, compartment=None):
    load_env_if_needed(env)
    return {
        "host": host or os.getenv('ORACLE_PG_HOST'),
        "fqdn": os.getenv('ORACLE_PG_FQDN') or host or os.getenv('ORACLE_PG_HOST'), 
        "port": int(port or os.getenv('ORACLE_PG_PORT', 5432)),
        "database": database or os.getenv('ORACLE_PG_DATABASE'),
        "user": user or os.getenv('ORACLE_PG_USER'),
        "password": password or os.getenv('ORACLE_PG_PASSWORD'),
        "profile": profile or os.getenv('OCI_PROFILE', 'meridian-readonly'),
        "compartment": compartment or os.getenv('OCI_COMPARTMENT'),
        "sslmode": "require"
    }


@click.group()
def cli():
    """Meridian — zero-downtime cross-cloud data migration engine."""
    pass


@cli.command()
@click.option('--profile', default=None, help='AWS profile (default: AWS_PROFILE from .env)')
@click.option('--region', default=None, help='AWS region (default: AWS_REGION from .env)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def scan_aws(profile, region, output, mock, env):
    """Scan AWS account and generate resource inventory."""
    try:
        cfg = get_aws_config(env, profile=profile, region=region)
        inventory = aws.scan(profile=cfg['profile'], region=cfg['region'], mock=mock)

        if output:
            with open(output, 'w') as f:
                json.dump(inventory, f, indent=2, default=str)
            console.print(f"\n[green]Inventory saved to {output}[/green]")
        else:
            console.print("\n[bold]Inventory:[/bold]")
            console.print_json(json.dumps(inventory, indent=2, default=str))

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--profile', default=None, help='OCI profile (default: OCI_PROFILE from .env)')
@click.option('--region', default=None, help='Oracle Cloud region (default: ap-mumbai-1)')
@click.option('--compartment', default=None, help='Oracle compartment OCID (default: OCI_COMPARTMENT from .env)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def scan_oracle(profile, region, compartment, output, mock, env):
    """Scan Oracle Cloud account and generate resource inventory."""
    try:
        cfg = get_oracle_config(env, profile=profile, compartment=compartment)
        inventory = oracle.scan(
            profile=cfg['profile'],
            region=region or 'ap-mumbai-1',
            mock=mock,
            compartment_id=cfg['compartment']
        )

        if output:
            with open(output, 'w') as f:
                json.dump(inventory, f, indent=2, default=str)
            console.print(f"\n[green]Inventory saved to {output}[/green]")
        else:
            console.print("\n[bold]Inventory:[/bold]")
            console.print_json(json.dumps(inventory, indent=2, default=str))

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--profile', default=None, help='AWS profile (default: AWS_PROFILE from .env)')
@click.option('--region', default=None, help='AWS region (default: AWS_REGION from .env)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def map_aws(profile, region, output, mock, env):
    """Map network dependencies for AWS account."""
    try:
        from meridian.mappers import aws_network
        from meridian.scanners import aws as aws_scanner

        cfg = get_aws_config(env, profile=profile, region=region)

        if mock:
            result = aws_network.map_network(None, mock=True)
        else:
            session = boto3.Session(profile_name=cfg['profile'], region_name=cfg['region'])
            rds_instances = aws_scanner.scan_rds(session)
            result = aws_network.map_network(session, rds_instances=rds_instances)

        if output:
            with open(output, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            console.print(f"\n[green]Dependency map saved to {output}[/green]")
        else:
            console.print("\n[bold]Dependency map:[/bold]")
            console.print_json(json.dumps(result, indent=2, default=str))

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--profile', default=None, help='OCI profile (default: OCI_PROFILE from .env)')
@click.option('--compartment', default=None, help='Oracle compartment OCID (default: OCI_COMPARTMENT from .env)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def map_oracle(profile, compartment, output, mock, env):
    """Map network dependencies for Oracle Cloud account."""
    try:
        import oci
        from meridian.mappers import oracle_network
        from meridian.scanners import oracle as oracle_scanner

        cfg = get_oracle_config(env, profile=profile, compartment=compartment)

        if mock:
            result = oracle_network.map_network(mock=True)
        else:
            oci_config = oci.config.from_file(profile_name=cfg['profile'])
            inventory = oracle_scanner.scan(
                profile=cfg['profile'],
                compartment_id=cfg['compartment']
            )
            databases = inventory['resources'].get('postgresql', [])
            result = oracle_network.map_network(
                config=oci_config,
                compartment_id=cfg['compartment'],
                databases=databases
            )

        if output:
            with open(output, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            console.print(f"\n[green]Dependency map saved to {output}[/green]")
        else:
            console.print("\n[bold]Dependency map:[/bold]")
            console.print_json(json.dumps(result, indent=2, default=str))

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--host', default=None, help='RDS endpoint (default: AWS_RDS_HOST from .env)')
@click.option('--port', default=None, help='Database port (default: AWS_RDS_PORT from .env)')
@click.option('--database', default=None, help='Database name (default: AWS_RDS_DATABASE from .env)')
@click.option('--user', default=None, help='Database user (default: AWS_RDS_USER from .env)')
@click.option('--password', default=None, help='Database password (default: AWS_RDS_PASSWORD from .env)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def scan_rds(host, port, database, user, password, output, env):
    """Scan a real AWS RDS PostgreSQL database — tables, extensions, indexes, parameters."""
    try:
        cfg = get_aws_config(env, host=host, port=port, database=database, user=user, password=password)

        if not all([cfg['host'], cfg['database'], cfg['user'], cfg['password']]):
            console.print("[red]Missing credentials — use --env or pass --host --database --user --password[/red]")
            raise click.Abort()

        result = aws.scan_rds_database(
            host=cfg['host'],
            port=cfg['port'],
            database=cfg['database'],
            user=cfg['user'],
            password=cfg['password']
        )

        aws.print_rds_summary(result)

        from datetime import datetime
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-rds-scan-{result['database']}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--host', default=None, help='Oracle PostgreSQL endpoint (default: ORACLE_PG_HOST from .env)')
@click.option('--port', default=None, help='Database port (default: ORACLE_PG_PORT from .env)')
@click.option('--database', default=None, help='Database name (default: ORACLE_PG_DATABASE from .env)')
@click.option('--user', default=None, help='Database user (default: ORACLE_PG_USER from .env)')
@click.option('--password', default=None, help='Database password (default: ORACLE_PG_PASSWORD from .env)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def scan_oracle_db(host, port, database, user, password, output, env):
    """Scan a real Oracle Managed PostgreSQL database — tables, extensions, indexes, parameters."""
    try:
        cfg = get_oracle_config(env, host=host, port=port, database=database, user=user, password=password)

        if not all([cfg['host'], cfg['database'], cfg['user'], cfg['password']]):
            console.print("[red]Missing credentials — use --env or pass --host --database --user --password[/red]")
            raise click.Abort()

        result = oracle.scan_oracle_database(
            host=cfg['host'],
            port=cfg['port'],
            database=cfg['database'],
            user=cfg['user'],
            password=cfg['password']
        )

        oracle.print_oracle_db_summary(result)

        from datetime import datetime
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-oracle-scan-{result['database']}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--source-db', default=None, help='Source DB name (default: AWS_RDS_DATABASE from .env)')
@click.option('--target-db', default=None, help='Target DB name (default: ORACLE_PG_DATABASE from .env)')
@click.option('--source-host', default=None, help='Source host (default: AWS_RDS_HOST from .env)')
@click.option('--target-host', default=None, help='Target host (default: ORACLE_PG_HOST from .env)')
@click.option('--output', default=None, help='Save full report to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def analyze_schema(source_db, target_db, source_host, target_host, output, mock, env):
    """Run pre-flight migration checklist between source and target databases."""
    try:
        from meridian.analyzers import schema_diff
        from datetime import datetime

        src_cfg = get_aws_config(env, host=source_host, database=source_db)
        tgt_cfg = get_oracle_config(env, host=target_host, database=target_db)

        source_db = source_db or src_cfg['database']
        target_db = target_db or tgt_cfg['database']

        source_config = None
        target_config = None

        if not mock:
            if not all([src_cfg['host'], src_cfg['database'], src_cfg['user'], src_cfg['password']]):
                console.print("[red]Missing source credentials — use --env or pass source options[/red]")
                raise click.Abort()
            if not all([tgt_cfg['host'], tgt_cfg['database'], tgt_cfg['user'], tgt_cfg['password']]):
                console.print("[red]Missing target credentials — use --env or pass target options[/red]")
                raise click.Abort()
            source_config = src_cfg
            target_config = tgt_cfg

        result = schema_diff.analyze(
            mock=mock,
            source_db=source_db,
            target_db=target_db,
            source_config=source_config,
            target_config=target_config
        )

        if result is None:
            return

        schema_diff.print_summary(result)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-schema-diff-{source_db}-to-{target_db}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--source-db', default=None, help='Source DB name (default: AWS_RDS_DATABASE from .env)')
@click.option('--target-db', default=None, help='Target DB name (default: ORACLE_PG_DATABASE from .env)')
@click.option('--output', default=None, help='Save replication report to a JSON file')
@click.option('--mock', is_flag=True, help='Simulate replication with mock data')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def replicate(source_db, target_db, output, mock, env):
    """Replicate data from source to target using pglogical CDC."""
    try:
        from meridian.replicator import replicator
        from datetime import datetime

        src_cfg = get_aws_config(env)
        tgt_cfg = get_oracle_config(env)

        source_db = source_db or src_cfg['database']
        target_db = target_db or tgt_cfg['database']

        db_source_config = {
            "host": src_cfg['host'],
            "port": src_cfg['port'],
            "database": src_cfg['database'],
            "user": src_cfg['user'],
            "password": src_cfg['password'],
            "sslmode": src_cfg.get('sslmode', 'prefer')
        }
        db_target_config = {
            "host": tgt_cfg['host'],
            "port": tgt_cfg['port'],
            "database": tgt_cfg['database'],
            "user": tgt_cfg['user'],
            "password": tgt_cfg['password'],
            "sslmode": tgt_cfg.get('sslmode', 'require')
        }

        result = replicator.replicate(
            source_db=source_db,
            target_db=target_db,
            mock=mock,
            source_config=None if mock else db_source_config,
            target_config=None if mock else db_target_config
        )

        if result is None:
            return

        replicator.print_summary(result)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-replication-{source_db}-to-{target_db}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--source-db', default=None, help='Source DB name (default: AWS_RDS_DATABASE from .env)')
@click.option('--target-db', default=None, help='Target DB name (default: ORACLE_PG_DATABASE from .env)')
@click.option('--output', default=None, help='Save validation report to a JSON file')
@click.option('--mock', is_flag=True, help='Simulate validation with mock data')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def validate(source_db, target_db, output, mock, env):
    """Validate data parity between source and target databases."""
    try:
        from meridian.validator import validator
        from datetime import datetime

        src_cfg = get_aws_config(env)
        tgt_cfg = get_oracle_config(env)

        source_db = source_db or src_cfg['database']
        target_db = target_db or tgt_cfg['database']

        db_source_config = {
            "host": src_cfg['host'],
            "port": src_cfg['port'],
            "database": src_cfg['database'],
            "user": src_cfg['user'],
            "password": src_cfg['password'],
            "sslmode": src_cfg.get('sslmode', 'prefer')
        }
        db_target_config = {
            "host": tgt_cfg['host'],
            "port": tgt_cfg['port'],
            "database": tgt_cfg['database'],
            "user": tgt_cfg['user'],
            "password": tgt_cfg['password'],
            "sslmode": tgt_cfg.get('sslmode', 'require')
        }

        result = validator.validate(
            source_db=source_db,
            target_db=target_db,
            mock=mock,
            source_config=None if mock else db_source_config,
            target_config=None if mock else db_target_config
        )

        if result is None:
            return

        validator.print_summary(result)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-validation-{source_db}-to-{target_db}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@cli.command()
@click.option('--source-db', default=None, help='Source DB name (default: AWS_RDS_DATABASE from .env)')
@click.option('--target-db', default=None, help='Target DB name (default: ORACLE_PG_DATABASE from .env)')
@click.option('--output', default=None, help='Save cutover report to a JSON file')
@click.option('--mock', is_flag=True, help='Simulate cutover with mock data')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def cutover(source_db, target_db, output, mock, env):
    """Execute cutover from source to target database."""
    try:
        from meridian.cutover import cutover as cutover_module
        from datetime import datetime

        src_cfg = get_aws_config(env)
        tgt_cfg = get_oracle_config(env)

        source_db = source_db or src_cfg['database']
        target_db = target_db or tgt_cfg['database']

        db_source_config = {
            "host": src_cfg['host'],
            "port": src_cfg['port'],
            "database": src_cfg['database'],
            "user": src_cfg['user'],
            "password": src_cfg['password'],
            "sslmode": src_cfg.get('sslmode', 'prefer')
        }
        db_target_config = {
            "host": tgt_cfg['host'],
            "port": tgt_cfg['port'],
            "database": tgt_cfg['database'],
            "user": tgt_cfg['user'],
            "password": tgt_cfg['password'],
            "sslmode": tgt_cfg.get('sslmode', 'require')
        }

        result = cutover_module.cutover(
            source_db=source_db,
            target_db=target_db,
            mock=mock,
            source_config=None if mock else db_source_config,
            target_config=None if mock else db_target_config
        )

        if result is None:
            return

        cutover_module.print_summary(result)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-cutover-{source_db}-to-{target_db}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

   


@cli.command()
@click.option('--output', default='meridian_schema.sql', help='Schema dump filename')
@click.option('--source-host', default=None, help='Source host (default: AWS_RDS_HOST from .env)')
@click.option('--target-host', default=None, help='Target host (default: ORACLE_PG_HOST from .env)')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def fix_schema(output, source_host, target_host, env):
    """Dump schema from source and restore to target — fixes missing tables blocker."""
    import subprocess

    try:
        src_cfg = get_aws_config(env, host=source_host)
        tgt_cfg = get_oracle_config(env, host=target_host)

        console.print(f"\n[bold magenta]Meridian — Schema Fix[/bold magenta]")
        console.print(f"\n[bold blue]Step 1: Dumping schema from source...[/bold blue]")

        env_vars = {**os.environ, 'PGPASSWORD': src_cfg['password']}

        dump_cmd = [
            'pg_dump',
            '-h', src_cfg['host'],
            '-p', str(src_cfg['port']),
            '-U', src_cfg['user'],
            '-d', src_cfg['database'],
            '--schema-only',
            '--no-owner',
            '--no-privileges',
            '--exclude-schema=pglogical',
            '-f', output
        ]

        result = subprocess.run(dump_cmd, env=env_vars, capture_output=True, text=True)
        if result.returncode != 0:
            console.print(f"[red]Schema dump failed: {result.stderr}[/red]")
            raise click.Abort()
        console.print(f"[green]✅ Schema dumped to {output}[/green]")

        console.print(f"\n[bold blue]Step 2: Restoring schema to target...[/bold blue]")

        env_vars = {**os.environ, 'PGPASSWORD': tgt_cfg['password']}
        restore_cmd = [
            'psql',
            f"host={tgt_cfg['host']} port={tgt_cfg['port']} dbname={tgt_cfg['database']} user={tgt_cfg['user']} sslmode=require",
            '-f', output
        ]

        result = subprocess.run(restore_cmd, env=env_vars, capture_output=True, text=True)
        if result.returncode != 0:
            console.print(f"[red]Schema restore failed: {result.stderr}[/red]")
            raise click.Abort()
        console.print(f"[green]✅ Schema restored to target[/green]")

        console.print(f"\n[bold blue]Step 3: Next steps...[/bold blue]")
        console.print("[yellow]Run: meridian analyze-schema --env[/yellow]")
        console.print("\n[bold green]🎉 Schema fix complete![/bold green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--env', is_flag=True, help='Load credentials from .env file')
@click.option('--output', default=None, help='Save cleanup report to a JSON file')
def cleanup(env, output):
    """Remove pglogical nodes and subscriptions after successful cutover."""
    try:
        from datetime import datetime
        import psycopg2

        src_cfg = get_aws_config(env)
        tgt_cfg = get_oracle_config(env)

        console.print(f"\n[bold magenta]Meridian — Post-cutover Cleanup[/bold magenta]")
        console.print(f"  Source: [yellow]{src_cfg['database']}[/yellow]")
        console.print(f"  Target: [yellow]{tgt_cfg['database']}[/yellow]\n")

        console.print("[bold red]⚠️  This will remove all pglogical replication objects.[/bold red]")
        console.print("[yellow]Only run this after cutover is confirmed successful.[/yellow]\n")

        # Confirm
        confirm = click.confirm("Are you sure you want to cleanup pglogical objects?")
        if not confirm:
            console.print("[yellow]Cleanup cancelled[/yellow]")
            return

        results = []

        def run_sql(host, port, db, user, password, sql, sslmode, description):
            try:
                conn = psycopg2.connect(
                    host=host, port=port, database=db,
                    user=user, password=password, sslmode=sslmode
                )
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(sql)
                conn.close()
                console.print(f"[green]✅ {description}[/green]")
                results.append({"action": description, "status": "success"})
            except Exception as e:
                console.print(f"[yellow]⚠️  {description}: {e}[/yellow]")
                results.append({"action": description, "status": "skipped", "reason": str(e)})

        console.print("[bold blue]Cleaning up target (Oracle Cloud)...[/bold blue]")
        run_sql(
            tgt_cfg['host'], tgt_cfg['port'], tgt_cfg['database'],
            tgt_cfg['user'], tgt_cfg['password'],
            "SELECT pglogical.drop_subscription('meridian_subscription', true)",
            tgt_cfg.get('sslmode', 'require'),
            "Dropped subscription on target"
        )
        run_sql(
            tgt_cfg['host'], tgt_cfg['port'], tgt_cfg['database'],
            tgt_cfg['user'], tgt_cfg['password'],
            "SELECT pglogical.drop_node('meridian_subscriber', true)",
            tgt_cfg.get('sslmode', 'require'),
            "Dropped subscriber node on target"
        )

        console.print("\n[bold blue]Cleaning up source (AWS RDS)...[/bold blue]")
        run_sql(
            src_cfg['host'], src_cfg['port'], src_cfg['database'],
            src_cfg['user'], src_cfg['password'],
            "SELECT pglogical.drop_replication_set('meridian_set')",
            src_cfg.get('sslmode', 'prefer'),
            "Dropped replication set on source"
        )
        run_sql(
            src_cfg['host'], src_cfg['port'], src_cfg['database'],
            src_cfg['user'], src_cfg['password'],
            "SELECT pglogical.drop_node('meridian_provider', true)",
            src_cfg.get('sslmode', 'prefer'),
            "Dropped provider node on source"
        )

        console.print("\n[bold green]🎉 Cleanup complete — migration fully finalized[/bold green]")
        console.print("[green]All pglogical replication objects removed from both databases[/green]")

        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-cleanup-{src_cfg['database']}-to-{tgt_cfg['database']}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump({
                "cleaned_at": datetime.utcnow().isoformat(),
                "source_db": src_cfg['database'],
                "target_db": tgt_cfg['database'],
                "actions": results
            }, f, indent=2)
        console.print(f"\n  Report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def status(env):
    """Show current replication status between source and target."""
    try:
        import psycopg2
        from datetime import datetime

        src_cfg = get_aws_config(env)
        tgt_cfg = get_oracle_config(env)

        console.print(f"\n[bold magenta]Meridian — Replication Status[/bold magenta]")
        console.print(f"  Source: [yellow]{src_cfg['database']}[/yellow] (AWS RDS)")
        console.print(f"  Target: [yellow]{tgt_cfg['database']}[/yellow] (Oracle Cloud)")
        console.print(f"  Checked: [dim]{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC[/dim]\n")

        # Connect to target — subscription lives here
        tgt_conn = psycopg2.connect(
            host=tgt_cfg['host'],
            port=tgt_cfg['port'],
            database=tgt_cfg['database'],
            user=tgt_cfg['user'],
            password=tgt_cfg['password'],
            sslmode=tgt_cfg.get('sslmode', 'require')
        )
        tgt_cur = tgt_conn.cursor()

        # Subscription status
        try:
            tgt_cur.execute("""
                SELECT
                    subscription_name,
                    status,
                    provider_node
                FROM pglogical.show_subscription_status()
            """)
            sub = tgt_cur.fetchone()
        except Exception:
            console.print("[yellow]⚠️  No pglogical node configured on target[/yellow]")
            console.print("[yellow]Run: meridian replicate --env to start replication[/yellow]")
            tgt_conn.close()
            return

        if not sub:
            console.print("[red]❌ No active subscription found[/red]")
            console.print("[yellow]Run: meridian replicate --env to start replication[/yellow]")
            tgt_conn.close()
            return

        sub_name, sub_status, provider = sub

        if sub_status == 'replicating':
            console.print(f"  [green]✅ Status: {sub_status}[/green]")
        elif sub_status == 'initializing':
            console.print(f"  [yellow]⏳ Status: {sub_status}[/yellow]")
        else:
            console.print(f"  [red]❌ Status: {sub_status}[/red]")

        console.print(f"  Subscription:  {sub_name}")
        console.print(f"  Provider:      {provider}")

        # Row counts on target
        tgt_cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        target_tables = [r[0] for r in tgt_cur.fetchall()]
        tgt_conn.close()

        # Row counts on source
        src_conn = psycopg2.connect(
            host=src_cfg['host'],
            port=src_cfg['port'],
            database=src_cfg['database'],
            user=src_cfg['user'],
            password=src_cfg['password'],
            sslmode=src_cfg.get('sslmode', 'prefer')
        )
        src_cur = src_conn.cursor()

        console.print()
        console.print("  [bold]Table parity:[/bold]")

        total_src = 0
        total_tgt = 0

        for table in target_tables:
            # Source count
            src_cur.execute(f"SELECT COUNT(*) FROM {table}")
            src_count = src_cur.fetchone()[0]

            # Target count
            tgt_conn2 = psycopg2.connect(
                host=tgt_cfg['host'],
                port=tgt_cfg['port'],
                database=tgt_cfg['database'],
                user=tgt_cfg['user'],
                password=tgt_cfg['password'],
                sslmode=tgt_cfg.get('sslmode', 'require')
            )
            tgt_cur2 = tgt_conn2.cursor()
            tgt_cur2.execute(f"SELECT COUNT(*) FROM {table}")
            tgt_count = tgt_cur2.fetchone()[0]
            tgt_conn2.close()

            diff = src_count - tgt_count
            total_src += src_count
            total_tgt += tgt_count

            if diff == 0:
                console.print(f"  [green]✅ {table:<20} {src_count:>8,} rows — in sync[/green]")
            else:
                console.print(f"  [yellow]⚠️  {table:<20} source={src_count:,} target={tgt_count:,} lag={diff:,} rows[/yellow]")

        src_conn.close()

        console.print()
        total_diff = total_src - total_tgt
        if total_diff == 0:
            console.print(f"  [bold green]✅ Total: {total_src:,} rows — perfectly in sync[/bold green]")
        else:
            console.print(f"  [bold yellow]⚠️  Total: source={total_src:,} target={total_tgt:,} lag={total_diff:,} rows[/bold yellow]")

        console.print()

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--env', is_flag=True, help='Load credentials from .env file')
@click.option('--interval', default=5, help='Refresh interval in seconds (default: 5)')
@click.option('--alert-lag', default=100, help='Alert when row lag exceeds this threshold (default: 100)')
def monitor(env, interval, alert_lag):
    """Continuously monitor replication lag and parity — Ctrl+C to stop."""
    import psycopg2
    from datetime import datetime

    src_cfg = get_aws_config(env)
    tgt_cfg = get_oracle_config(env)

    console.print(f"\n[bold magenta]Meridian — Live Replication Monitor[/bold magenta]")
    console.print(f"  Source: [yellow]{src_cfg['database']}[/yellow] (AWS RDS)")
    console.print(f"  Target: [yellow]{tgt_cfg['database']}[/yellow] (Oracle Cloud)")
    console.print(f"  Refresh: every {interval}s · Alert lag threshold: {alert_lag} rows")
    console.print(f"  Press [bold]Ctrl+C[/bold] to stop\n")
    console.print("─" * 60)

    iteration = 0

    try:
        while True:
            iteration += 1
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            try:
                # Check subscription status
                tgt_conn = psycopg2.connect(
                    host=tgt_cfg['host'],
                    port=tgt_cfg['port'],
                    database=tgt_cfg['database'],
                    user=tgt_cfg['user'],
                    password=tgt_cfg['password'],
                    sslmode=tgt_cfg.get('sslmode', 'require')
                )
                tgt_cur = tgt_conn.cursor()

                try:
                    tgt_cur.execute("""
                        SELECT subscription_name, status, provider_node
                        FROM pglogical.show_subscription_status()
                    """)
                    sub = tgt_cur.fetchone()
                except Exception:
                    sub = None

                if not sub:
                    console.print(f"[dim]{now}[/dim] [red]❌ No active subscription[/red]")
                    tgt_conn.close()
                    import time
                    time.sleep(interval)
                    continue

                sub_name, sub_status, provider = sub

                # Get table counts
                tgt_cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [r[0] for r in tgt_cur.fetchall()]

                total_src = 0
                total_tgt = 0
                table_stats = []

                src_conn = psycopg2.connect(
                    host=src_cfg['host'],
                    port=src_cfg['port'],
                    database=src_cfg['database'],
                    user=src_cfg['user'],
                    password=src_cfg['password'],
                    sslmode=src_cfg.get('sslmode', 'prefer')
                )
                src_cur = src_conn.cursor()

                for table in tables:
                    src_cur.execute(f"SELECT COUNT(*) FROM {table}")
                    src_count = src_cur.fetchone()[0]

                    tgt_cur.execute(f"SELECT COUNT(*) FROM {table}")
                    tgt_count = tgt_cur.fetchone()[0]

                    diff = src_count - tgt_count
                    total_src += src_count
                    total_tgt += tgt_count
                    table_stats.append((table, src_count, tgt_count, diff))

                src_conn.close()
                tgt_conn.close()

                total_lag = total_src - total_tgt

                # Print status line
                if sub_status == 'replicating' and total_lag == 0:
                    status_icon = "✅"
                    status_color = "green"
                elif sub_status == 'replicating' and total_lag > 0:
                    status_icon = "⏳"
                    status_color = "yellow"
                else:
                    status_icon = "❌"
                    status_color = "red"

                console.print(
                    f"[dim]{now}[/dim] "
                    f"[{status_color}]{status_icon} {sub_status}[/{status_color}] "
                    f"· source=[cyan]{total_src:,}[/cyan] "
                    f"target=[cyan]{total_tgt:,}[/cyan] "
                    f"lag=[{'red' if total_lag > 0 else 'green'}]{total_lag:,}[/{'red' if total_lag > 0 else 'green'}] rows"
                )

                # Alert if lag exceeds threshold
                if total_lag > alert_lag:
                    console.print(f"[bold red]⚠️  ALERT: Replication lag {total_lag:,} rows exceeds threshold {alert_lag:,}[/bold red]")

                # Show per-table detail every 6 iterations (30 seconds)
                if iteration % 6 == 0:
                    console.print()
                    for table, src, tgt, diff in table_stats:
                        if diff == 0:
                            console.print(f"  [green]✅ {table:<20} {src:>8,} rows in sync[/green]")
                        else:
                            console.print(f"  [yellow]⚠️  {table:<20} source={src:,} target={tgt:,} lag={diff:,}[/yellow]")
                    console.print()

            except Exception as e:
                console.print(f"[dim]{now}[/dim] [red]Error: {e}[/red]")

            import time
            time.sleep(interval)

    except KeyboardInterrupt:
        console.print("\n\n[yellow]Monitor stopped[/yellow]")
        console.print("[dim]Run meridian status --env for a one-shot check[/dim]")

if __name__ == '__main__':
    cli()