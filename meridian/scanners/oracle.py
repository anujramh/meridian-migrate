import oci
from datetime import datetime
from rich.console import Console

console = Console()

MOCK_INVENTORY = {
    "compute": [
        {
            "id": "ocid1.instance.oc1.ap-mumbai-1.mock001",
            "name": "prod-app-server-01",
            "shape": "VM.Standard2.2",
            "status": "RUNNING",
            "region": "ap-mumbai-1",
            "availability_domain": "AD-1"
        },
        {
            "id": "ocid1.instance.oc1.ap-mumbai-1.mock002",
            "name": "prod-db-server-01",
            "shape": "VM.Standard2.4",
            "status": "RUNNING",
            "region": "ap-mumbai-1",
            "availability_domain": "AD-1"
        }
    ],

    "databases": [
        {
            "id": "ocid1.dbsystem.oc1.ap-mumbai-1.mock001",
            "name": "prod-oracle-db-01",
            "shape": "VM.Standard2.2",
            "version": "19.0.0.0",
            "status": "AVAILABLE",
            "storage_gb": 256,
            "region": "ap-mumbai-1"
        }
    ],
    "postgresql": [
        {
            "id": "ocid1.dbsystem.oc1.ap-mumbai-1.mock001",
            "name": "prod-postgres-db-01",
            "version": "14.9",
            "status": "ACTIVE",
            "shape": "PostgreSQL.VM.Standard.E4.Flex.2.32GB",
            "region": "ap-mumbai-1"
        },
        {
            "id": "ocid1.dbsystem.oc1.ap-mumbai-1.mock002",
            "name": "staging-postgres-db-01",
            "version": "15.4",
            "status": "ACTIVE",
            "shape": "PostgreSQL.VM.Standard.E4.Flex.2.32GB",
            "region": "ap-mumbai-1"
        }
    ],

    "object_storage": [
        {
            "name": "prod-assets-bucket",
            "namespace": "mytenancy",
            "region": "ap-mumbai-1",
            "created": "2023-01-15T10:30:00"
        },
        {
            "name": "prod-backups-bucket",
            "namespace": "mytenancy",
            "region": "ap-mumbai-1",
            "created": "2023-01-15T10:32:00"
        }
    ]
}


def get_tenancy_id(config):
    return config.get('tenancy')


def scan_compute(compute_client, compartment_id, region):
    console.print("[bold blue]Scanning Compute instances...[/bold blue]")
    try:
        response = compute_client.list_instances(compartment_id=compartment_id)
        instances = []
        for instance in response.data:
            instances.append({
                "id": instance.id,
                "name": instance.display_name if instance.display_name else "unnamed",
                "shape": instance.shape,
                "status": instance.lifecycle_state,
                "region": region,
                "availability_domain": instance.availability_domain
            })
        console.print(f"[green]Found {len(instances)} compute instance(s)[/green]")
        return instances
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            console.print("[yellow]Compute: not authorized or no instances found[/yellow]")
        elif e.status == 401:
            console.print("[red]Compute: authentication failed — check your credentials[/red]")
        else:
            console.print(f"[red]Compute scan failed: {e.message}[/red]")
        return []
    except Exception as e:
        console.print(f"[red]Compute scan failed: {str(e)}[/red]")
        return []


def scan_databases(db_client, compartment_id, region):
    console.print("[bold blue]Scanning Database systems...[/bold blue]")
    try:
        response = db_client.list_db_systems(compartment_id=compartment_id)
        databases = []
        for db in response.data:
            databases.append({
                "id": db.id,
                "name": db.display_name,
                "shape": db.shape,
                "version": db.version,
                "status": db.lifecycle_state,
                "storage_gb": db.data_storage_size_in_gbs,
                "region": region
            })
        console.print(f"[green]Found {len(databases)} database system(s)[/green]")
        return databases
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            console.print("[yellow]Databases: not authorized or no databases found[/yellow]")
        elif e.status == 401:
            console.print("[red]Databases: authentication failed — check your credentials[/red]")
        else:
            console.print(f"[red]Database scan failed: {e.message}[/red]")
        return []
    except Exception as e:
        console.print(f"[red]Database scan failed: {str(e)}[/red]")
        return []


def scan_postgresql(db_client, compartment_id, region):
    console.print("[bold blue]Scanning PostgreSQL databases...[/bold blue]")
    try:
        pg_client = oci.psql.PostgresqlClient(db_client._config)
        response = pg_client.list_db_systems(compartment_id=compartment_id)
        databases = []
        for db in response.data.items:
            databases.append({
                "id": db.id,
                "name": db.display_name,
                "version": db.db_version,
                "status": db.lifecycle_state,
                "shape": db.shape,
                "region": region
            })
        console.print(f"[green]Found {len(databases)} PostgreSQL database(s)[/green]")
        return databases
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            console.print("[yellow]PostgreSQL: not authorized or no databases found[/yellow]")
        elif e.status == 401:
            console.print("[red]PostgreSQL: authentication failed — check your credentials[/red]")
        else:
            console.print(f"[red]PostgreSQL scan failed: {e.message}[/red]")
        return []
    except Exception as e:
        console.print(f"[yellow]PostgreSQL scan skipped: {str(e)}[/yellow]")
        return []
    

def scan_object_storage(os_client, compartment_id, namespace, region):
    console.print("[bold blue]Scanning Object Storage buckets...[/bold blue]")
    try:
        response = os_client.list_buckets(
            namespace_name=namespace,
            compartment_id=compartment_id
        )
        buckets = []
        for bucket in response.data:
            buckets.append({
                "name": bucket.name,
                "namespace": namespace,
                "region": region,
                "created": bucket.time_created.isoformat() if bucket.time_created else None
            })
        console.print(f"[green]Found {len(buckets)} bucket(s)[/green]")
        return buckets
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            console.print("[yellow]Object Storage: not authorized or no buckets found[/yellow]")
        elif e.status == 401:
            console.print("[red]Object Storage: authentication failed — check your credentials[/red]")
        else:
            console.print(f"[red]Object Storage scan failed: {e.message}[/red]")
        return []
    except Exception as e:
        console.print(f"[red]Object Storage scan failed: {str(e)}[/red]")
        return []


def scan_mock(region='ap-mumbai-1'):
    console.print("[bold yellow]Running in mock mode — no real OCI credentials needed[/bold yellow]\n")
    console.print("[bold blue]Scanning Compute instances...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_INVENTORY['compute'])} compute instance(s)[/green]")
    console.print("[bold blue]Scanning Database systems...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_INVENTORY['databases'])} database system(s)[/green]")
    console.print("[bold blue]Scanning Object Storage buckets...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_INVENTORY['object_storage'])} bucket(s)[/green]")
    return MOCK_INVENTORY


def scan(region='ap-mumbai-1', profile='DEFAULT', mock=False, compartment_id=None):
    console.print(f"\n[bold magenta]Meridian — Oracle Cloud Scanner[/bold magenta]")
    console.print(f"Region: [yellow]{region}[/yellow]\n")

    if mock:
        resources = scan_mock(region)
    else:
        try:
            config = oci.config.from_file(profile_name=profile)
        except Exception as e:
            console.print(f"[red]Failed to load OCI config: {str(e)}[/red]")
            console.print("[yellow]Tip: Run 'oci setup config' to set up your credentials[/yellow]")
            raise

        if not compartment_id:
            compartment_id = get_tenancy_id(config)

        # Validate compartment ID before scanning
        try:
            identity_client = oci.identity.IdentityClient(config)
            identity_client.get_compartment(compartment_id=compartment_id)
        except oci.exceptions.ServiceError as e:
            if e.status == 404:
                console.print(f"[red]Invalid compartment ID: {compartment_id}[/red]")
                console.print("[yellow]Tip: Check your compartment OCID and try again[/yellow]")
            elif e.status == 401:
                console.print("[red]Authentication failed — check your credentials[/red]")
            else:
                console.print(f"[red]Compartment validation failed: {e.message}[/red]")
            raise

        try:
            os_client = oci.object_storage.ObjectStorageClient(config)
            namespace = os_client.get_namespace().data
        except Exception as e:
            console.print(f"[red]Failed to connect to Oracle Cloud: {str(e)}[/red]")
            console.print("[yellow]Tip: Check your region and credentials[/yellow]")
            raise

        compute_client = oci.core.ComputeClient(config)
        db_client = oci.database.DatabaseClient(config)

        resources = {
            "compute": scan_compute(compute_client, compartment_id, region),
            "databases": scan_databases(db_client, compartment_id, region),
            "postgresql": scan_postgresql(db_client, compartment_id, region),
            "object_storage": scan_object_storage(os_client, compartment_id, namespace, region)
        }

    inventory = {
        "source": "oracle",
        "region": region,
        "scanned_at": datetime.utcnow().isoformat(),
        "mock": mock,
        "resources": resources
    }

    return inventory




