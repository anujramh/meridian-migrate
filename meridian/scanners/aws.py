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

def print_rds_summary(result):
    console.print("\n" + "─" * 50)
    console.print("[bold magenta]  Meridian — RDS Database Scan[/bold magenta]")
    console.print("─" * 50)
    console.print(f"  Host:      {result['host']}")
    console.print(f"  Database:  {result['database']}")
    console.print(f"  Version:   {result['version']}")
    console.print()

    console.print("  [bold]Tables:[/bold]")
    for table in result['tables']:
        console.print(f"  📋 {table['name']:<20} {table['row_count']:>10,} rows   {table['size']}")

    console.print()
    console.print("  [bold]Extensions:[/bold]")
    for ext in result['extensions']:
        console.print(f"  🔌 {ext['name']:<30} v{ext['version']}")

    console.print()
    console.print("  [bold]Key parameters:[/bold]")
    for key, val in result['parameters'].items():
        unit = val['unit'] or ''
        console.print(f"  ⚙️  {key:<25} {val['value']} {unit}")

    console.print()
    console.print(f"  [green]✅ {len(result['tables'])} tables · {len(result['extensions'])} extensions · {len(result['indexes'])} indexes[/green]")
    console.print("─" * 50)
    
def scan_rds_database(host, port, database, user, password):
    import psycopg2
    console.print(f"\n[bold magenta]Meridian — RDS Database Scanner[/bold magenta]")
    console.print(f"Host: [yellow]{host}[/yellow]")
    console.print(f"Database: [yellow]{database}[/yellow]\n")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cur = conn.cursor()

        # Get PostgreSQL version
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        console.print(f"[green]Connected — {version.split(',')[0]}[/green]\n")

        # Get tables
        console.print("[bold blue]Scanning tables...[/bold blue]")
        cur.execute("""
            SELECT 
                t.table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(t.table_name))) as size,
                s.n_live_tup as row_count
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
            WHERE t.table_schema = 'public'
            ORDER BY s.n_live_tup DESC NULLS LAST
        """)
        tables = []
        for row in cur.fetchall():
            tables.append({
                "name": row[0],
                "size": row[1],
                "row_count": row[2] or 0
            })
        console.print(f"[green]Found {len(tables)} table(s)[/green]")

        # Get extensions
        console.print("[bold blue]Scanning extensions...[/bold blue]")
        cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")
        extensions = [{"name": r[0], "version": r[1]} for r in cur.fetchall()]
        console.print(f"[green]Found {len(extensions)} extension(s)[/green]")

        # Get indexes
        console.print("[bold blue]Scanning indexes...[/bold blue]")
        cur.execute("""
            SELECT 
                indexname,
                tablename,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        indexes = [{"name": r[0], "table": r[1], "definition": r[2]} for r in cur.fetchall()]
        console.print(f"[green]Found {len(indexes)} index(es)[/green]")

        # Get parameters
        console.print("[bold blue]Scanning connection parameters...[/bold blue]")
        cur.execute("""
            SELECT name, setting, unit
            FROM pg_settings
            WHERE name IN (
                'max_connections', 'shared_buffers', 'work_mem',
                'maintenance_work_mem', 'wal_level', 'max_wal_senders'
            )
        """)
        parameters = {r[0]: {"value": r[1], "unit": r[2]} for r in cur.fetchall()}
        console.print(f"[green]Found {len(parameters)} key parameter(s)[/green]")

        conn.close()

        return {
            "host": host,
            "database": database,
            "version": version.split(',')[0],
            "tables": tables,
            "extensions": extensions,
            "indexes": indexes,
            "parameters": parameters
        }

    except psycopg2.OperationalError as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        console.print("[yellow]Tip: Check host, port, credentials and security group rules[/yellow]")
        raise
    except Exception as e:
        console.print(f"[red]Scan failed: {e}[/red]")
        raise