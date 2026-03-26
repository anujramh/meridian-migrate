import os
import click
import json
import boto3
from rich.console import Console
from meridian.scanners import aws
from meridian.scanners import oracle


console = Console()


@click.group()
def cli():
    """Meridian — zero-downtime cross-cloud data migration engine."""
    pass


@cli.command()
@click.option('--profile', default=None, help='AWS profile name from ~/.aws/credentials')
@click.option('--region', default='us-east-1', help='AWS region to scan')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
def scan_aws(profile, region, output, mock):
    """Scan AWS account and generate resource inventory."""
    try:
        inventory = aws.scan(profile=profile, region=region, mock=mock)

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
@click.option('--profile', default='DEFAULT', help='OCI profile from ~/.oci/config')
@click.option('--region', default='ap-mumbai-1', help='Oracle Cloud region to scan')
@click.option('--compartment', default=None, help='Oracle Cloud compartment OCID (defaults to root)')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
def scan_oracle(profile, region, compartment, output, mock):
    """Scan Oracle Cloud account and generate resource inventory."""
    try:
        inventory = oracle.scan(
            profile=profile,
            region=region,
            mock=mock,
            compartment_id=compartment
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
@click.option('--profile', default=None, help='AWS profile name from ~/.aws/credentials')
@click.option('--region', default='us-east-1', help='AWS region to scan')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
def map_aws(profile, region, output, mock):
    """Map network dependencies for AWS account."""
    try:
        from meridian.mappers import aws_network
        from meridian.scanners import aws as aws_scanner

        if mock:
            result = aws_network.map_network(None, mock=True)
        else:
            session = boto3.Session(profile_name=profile, region_name=region)
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
@click.option('--profile', default='DEFAULT', help='OCI profile from ~/.oci/config')
@click.option('--compartment', default=None, help='Oracle Cloud compartment OCID')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
def map_oracle(profile, compartment, output, mock):
    """Map network dependencies for Oracle Cloud account."""
    try:
        import oci
        from meridian.mappers import oracle_network
        from meridian.scanners import oracle as oracle_scanner

        if mock:
            result = oracle_network.map_network(mock=True)
        else:
            config = oci.config.from_file(profile_name=profile)
            if not compartment:
                compartment = config.get('compartment') or config.get('tenancy')
            inventory = oracle_scanner.scan(
                profile=profile,
                compartment_id=compartment
            )
            databases = inventory['resources'].get('postgresql', [])
            result = oracle_network.map_network(
                config=config,
                compartment_id=compartment,
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
@click.option('--source-db', required=True, help='Source database name on AWS RDS')
@click.option('--target-db', required=True, help='Target database name on Oracle Managed PostgreSQL')
@click.option('--output', default=None, help='Save full report to a JSON file')
@click.option('--mock', is_flag=True, help='Run with mock data, no credentials needed')
def analyze_schema(source_db, target_db, output, mock):
    """Analyze schema compatibility between a specific AWS RDS and Oracle Managed PostgreSQL database."""
    try:
        from meridian.analyzers import schema_diff
        from datetime import datetime

        console.print(f"  Source DB: [bold]{source_db}[/bold]")
        console.print(f"  Target DB: [bold]{target_db}[/bold]")

        result = schema_diff.analyze(
            mock=mock,
            source_db=source_db,
            target_db=target_db
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
@click.option('--source-db', required=True, help='Source database name on AWS RDS')
@click.option('--target-db', required=True, help='Target database name on Oracle Managed PostgreSQL')
@click.option('--output', default=None, help='Save replication report to a JSON file')
@click.option('--mock', is_flag=True, help='Simulate replication with mock data')
def replicate(source_db, target_db, output, mock):
    """Replicate data from AWS RDS PostgreSQL to Oracle Managed PostgreSQL."""
    try:
        from meridian.replicator import replicator
        from datetime import datetime

        result = replicator.replicate(
            source_db=source_db,
            target_db=target_db,
            mock=mock
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
@click.option('--source-db', required=True, help='Source database name on AWS RDS')
@click.option('--target-db', required=True, help='Target database name on Oracle Managed PostgreSQL')
@click.option('--output', default=None, help='Save validation report to a JSON file')
@click.option('--mock', is_flag=True, help='Simulate validation with mock data')
def validate(source_db, target_db, output, mock):
    """Validate data parity between source and target databases."""
    try:
        from meridian.validator import validator
        from datetime import datetime

        result = validator.validate(
            source_db=source_db,
            target_db=target_db,
            mock=mock
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
@click.option('--source-db', required=True, help='Source database name on AWS RDS')
@click.option('--target-db', required=True, help='Target database name on Oracle Managed PostgreSQL')
@click.option('--output', default=None, help='Save cutover report to a JSON file')
@click.option('--mock', is_flag=True, help='Simulate cutover with mock data')
def cutover(source_db, target_db, output, mock):
    """Execute cutover from AWS RDS PostgreSQL to Oracle Managed PostgreSQL."""
    try:
        from meridian.cutover import cutover as cutover_module
        from datetime import datetime

        result = cutover_module.cutover(
            source_db=source_db,
            target_db=target_db,
            mock=mock
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
@click.option('--host', default=None, help='RDS endpoint')
@click.option('--port', default=5432, help='Database port')
@click.option('--database', default=None, help='Database name')
@click.option('--user', default=None, help='Database user')
@click.option('--password', default=None, help='Database password')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def scan_rds(host, port, database, user, password, output, env):
    """Scan a real AWS RDS PostgreSQL database — tables, extensions, indexes, parameters."""
    try:
        from meridian.scanners import aws as aws_scanner

        if env:
            from dotenv import load_dotenv
            load_dotenv()
            host = host or os.getenv('AWS_RDS_HOST')
            port = port or int(os.getenv('AWS_RDS_PORT', 5432))
            database = database or os.getenv('AWS_RDS_DATABASE')
            user = user or os.getenv('AWS_RDS_USER')
            password = password or os.getenv('AWS_RDS_PASSWORD')

        if not all([host, database, user, password]):
            console.print("[red]Missing credentials — use --env to load from .env file[/red]")
            raise click.Abort()

        result = aws_scanner.scan_rds_database(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )


        # Always print clean summary
        aws_scanner.print_rds_summary(result)

        # Always save full report
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
@click.option('--host', default=None, help='Oracle PostgreSQL endpoint')
@click.option('--port', default=5432, help='Database port')
@click.option('--database', default=None, help='Database name')
@click.option('--user', default=None, help='Database user')
@click.option('--password', default=None, help='Database password')
@click.option('--output', default=None, help='Save output to a JSON file')
@click.option('--env', is_flag=True, help='Load credentials from .env file')
def scan_oracle_db(host, port, database, user, password, output, env):
    """Scan a real Oracle Managed PostgreSQL database — tables, extensions, indexes, parameters."""
    try:
        from meridian.scanners import oracle as oracle_scanner

        if env:
            from dotenv import load_dotenv
            load_dotenv()
            host = host or os.getenv('ORACLE_PG_HOST')
            port = port or int(os.getenv('ORACLE_PG_PORT', 5432))
            database = database or os.getenv('ORACLE_PG_DATABASE')
            user = user or os.getenv('ORACLE_PG_USER')
            password = password or os.getenv('ORACLE_PG_PASSWORD')

        if not all([host, database, user, password]):
            console.print("[red]Missing credentials — use --env to load from .env file[/red]")
            raise click.Abort()

        result = oracle_scanner.scan_oracle_database(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        oracle_scanner.print_oracle_db_summary(result)

        from datetime import datetime
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        filename = output or f"meridian-oracle-scan-{result['database']}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        console.print(f"\n  Full report saved to: [bold]{filename}[/bold]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()  
    

if __name__ == '__main__':
    cli()