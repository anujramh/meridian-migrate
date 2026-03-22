import boto3
import json
from datetime import datetime
from rich.console import Console
from botocore.exceptions import ClientError, NoCredentialsError, EndpointResolutionError

console = Console()

MOCK_INVENTORY = {
    "rds": [
        {
            "id": "prod-postgres-01",
            "engine": "postgres",
            "engine_version": "15.3",
            "instance_class": "db.t3.medium",
            "status": "available",
            "endpoint": "prod-postgres-01.abc123.us-east-1.rds.amazonaws.com",
            "port": 5432,
            "multi_az": True,
            "storage_gb": 100,
            "region": "us-east-1"
        },
        {
            "id": "staging-postgres-01",
            "engine": "postgres",
            "engine_version": "14.7",
            "instance_class": "db.t3.small",
            "status": "available",
            "endpoint": "staging-postgres-01.abc123.us-east-1.rds.amazonaws.com",
            "port": 5432,
            "multi_az": False,
            "storage_gb": 20,
            "region": "us-east-1"
        },
        {
            "id": "prod-mongo-01",
            "engine": "docdb",
            "engine_version": "6.0",
            "instance_class": "db.r5.large",
            "status": "available",
            "endpoint": "prod-mongo-01.abc123.us-east-1.docdb.amazonaws.com",
            "port": 27017,
            "multi_az": True,
            "storage_gb": 200,
            "region": "us-east-1"
        }
    ],
    "s3": [
        {
            "name": "prod-assets-bucket",
            "created": "2023-01-15T10:30:00",
            "region": "us-east-1"
        },
        {
            "name": "prod-backups-bucket",
            "created": "2023-01-15T10:32:00",
            "region": "us-east-1"
        },
        {
            "name": "staging-assets-bucket",
            "created": "2023-03-22T08:15:00",
            "region": "us-east-1"
        }
    ]
}


def scan_rds(session):
    console.print("[bold blue]Scanning RDS instances...[/bold blue]")
    try:
        rds = session.client('rds')
        response = rds.describe_db_instances()
        instances = []
        for db in response['DBInstances']:
            instances.append({
                "id": db['DBInstanceIdentifier'],
                "engine": db['Engine'],
                "engine_version": db['EngineVersion'],
                "instance_class": db['DBInstanceClass'],
                "status": db['DBInstanceStatus'],
                "endpoint": db.get('Endpoint', {}).get('Address', None),
                "port": db.get('Endpoint', {}).get('Port', None),
                "multi_az": db['MultiAZ'],
                "storage_gb": db['AllocatedStorage'],
                "region": session.region_name
            })
        console.print(f"[green]Found {len(instances)} RDS instance(s)[/green]")
        return instances
    except ClientError as e:
        code = e.response['Error']['Code']
        if code == 'AccessDenied':
            console.print("[yellow]RDS: access denied — check IAM permissions[/yellow]")
        elif code == 'AuthFailure':
            console.print("[red]RDS: authentication failed — check your credentials[/red]")
        else:
            console.print(f"[red]RDS scan failed: {e.response['Error']['Message']}[/red]")
        return []
    except Exception as e:
        console.print(f"[red]RDS scan failed: {str(e)}[/red]")
        return []


def scan_s3(session):
    console.print("[bold blue]Scanning S3 buckets...[/bold blue]")
    try:
        s3 = session.client('s3')
        response = s3.list_buckets()
        buckets = []
        for bucket in response['Buckets']:
            buckets.append({
                "name": bucket['Name'],
                "created": bucket['CreationDate'],
                "region": session.region_name
            })
        console.print(f"[green]Found {len(buckets)} S3 bucket(s)[/green]")
        return buckets
    except ClientError as e:
        code = e.response['Error']['Code']
        if code == 'AccessDenied':
            console.print("[yellow]S3: access denied — check IAM permissions[/yellow]")
        elif code == 'AuthFailure':
            console.print("[red]S3: authentication failed — check your credentials[/red]")
        else:
            console.print(f"[red]S3 scan failed: {e.response['Error']['Message']}[/red]")
        return []
    except Exception as e:
        console.print(f"[red]S3 scan failed: {str(e)}[/red]")
        return []


def scan_mock(region='us-east-1'):
    console.print("[bold yellow]Running in mock mode — no real AWS credentials needed[/bold yellow]\n")
    console.print("[bold blue]Scanning RDS instances...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_INVENTORY['rds'])} RDS instance(s)[/green]")
    console.print("[bold blue]Scanning S3 buckets...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_INVENTORY['s3'])} S3 bucket(s)[/green]")
    return MOCK_INVENTORY


def scan(profile=None, region='us-east-1', mock=False):
    console.print(f"\n[bold magenta]Meridian — AWS Scanner[/bold magenta]")
    console.print(f"Region: [yellow]{region}[/yellow]\n")

    if mock:
        resources = scan_mock(region)
    else:
        try:
            session = boto3.Session(profile_name=profile, region_name=region)
            # Verify credentials are valid
            sts = session.client('sts')
            sts.get_caller_identity()
        except NoCredentialsError:
            console.print("[red]No AWS credentials found[/red]")
            console.print("[yellow]Tip: Run 'aws configure' or use --mock for demo mode[/yellow]")
            raise
        except ClientError as e:
            console.print(f"[red]AWS authentication failed: {e.response['Error']['Message']}[/red]")
            console.print("[yellow]Tip: Check your AWS credentials and permissions[/yellow]")
            raise
        except Exception as e:
            console.print(f"[red]Failed to connect to AWS: {str(e)}[/red]")
            raise

        resources = {
            "rds": scan_rds(session),
            "s3": scan_s3(session)
        }

    inventory = {
        "source": "aws",
        "region": region,
        "scanned_at": datetime.utcnow().isoformat(),
        "mock": mock,
        "resources": resources
    }

    return inventory