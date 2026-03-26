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

if __name__ == '__main__':
    cli()