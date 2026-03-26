import oci
from rich.console import Console

console = Console()

MOCK_DATA = {
    "vcns": [
        {
            "id": "ocid1.vcn.oc1.ap-mumbai-1.mock001",
            "name": "prod-vcn",
            "cidr": "10.0.0.0/16",
            "state": "AVAILABLE",
            "dns_label": "prodvcn"
        },
        {
            "id": "ocid1.vcn.oc1.ap-mumbai-1.mock002",
            "name": "dev-vcn",
            "cidr": "10.1.0.0/16",
            "state": "AVAILABLE",
            "dns_label": "devvcn"
        }
    ],
    "subnets": [
        {
            "id": "ocid1.subnet.oc1.ap-mumbai-1.mock001",
            "name": "prod-private-subnet",
            "vcn_id": "ocid1.vcn.oc1.ap-mumbai-1.mock001",
            "cidr": "10.0.1.0/24",
            "availability_domain": "AP-MUMBAI-1-AD-1",
            "state": "AVAILABLE",
            "public": False
        },
        {
            "id": "ocid1.subnet.oc1.ap-mumbai-1.mock002",
            "name": "prod-public-subnet",
            "vcn_id": "ocid1.vcn.oc1.ap-mumbai-1.mock001",
            "cidr": "10.0.0.0/24",
            "availability_domain": "AP-MUMBAI-1-AD-1",
            "state": "AVAILABLE",
            "public": True
        },
        {
            "id": "ocid1.subnet.oc1.ap-mumbai-1.mock003",
            "name": "dev-private-subnet",
            "vcn_id": "ocid1.vcn.oc1.ap-mumbai-1.mock002",
            "cidr": "10.1.1.0/24",
            "availability_domain": "AP-MUMBAI-1-AD-1",
            "state": "AVAILABLE",
            "public": False
        }
    ],
    "security_lists": [
        {
            "id": "ocid1.securitylist.oc1.ap-mumbai-1.mock001",
            "name": "prod-db-security-list",
            "vcn_id": "ocid1.vcn.oc1.ap-mumbai-1.mock001",
            "state": "AVAILABLE",
            "inbound_rules": [
                {
                    "protocol": "6",
                    "source": "10.0.1.0/24",
                    "source_type": "CIDR_BLOCK",
                    "stateless": False
                },
                {
                    "protocol": "6",
                    "source": "10.0.2.0/24",
                    "source_type": "CIDR_BLOCK",
                    "stateless": False
                }
            ],
            "outbound_rules": [
                {
                    "protocol": "all",
                    "destination": "0.0.0.0/0",
                    "destination_type": "CIDR_BLOCK",
                    "stateless": False
                }
            ]
        },
        {
            "id": "ocid1.securitylist.oc1.ap-mumbai-1.mock002",
            "name": "prod-app-security-list",
            "vcn_id": "ocid1.vcn.oc1.ap-mumbai-1.mock001",
            "state": "AVAILABLE",
            "inbound_rules": [
                {
                    "protocol": "6",
                    "source": "0.0.0.0/0",
                    "source_type": "CIDR_BLOCK",
                    "stateless": False
                }
            ],
            "outbound_rules": [
                {
                    "protocol": "all",
                    "destination": "0.0.0.0/0",
                    "destination_type": "CIDR_BLOCK",
                    "stateless": False
                }
            ]
        }
    ],
    "dependency_map": [
        {
            "resource": "prod-postgres-db-01",
            "type": "postgresql",
            "version": "15.12",
            "depends_on": [
                {
                    "type": "security_list",
                    "id": "ocid1.securitylist.oc1.ap-mumbai-1.mock001",
                    "name": "prod-db-security-list",
                    "inbound_rules_count": 2
                },
                {
                    "type": "subnet",
                    "id": "ocid1.subnet.oc1.ap-mumbai-1.mock001",
                    "name": "prod-private-subnet",
                    "cidr": "10.0.1.0/24"
                }
            ]
        },
        {
            "resource": "staging-postgres-db-01",
            "type": "postgresql",
            "version": "14.9",
            "depends_on": [
                {
                    "type": "security_list",
                    "id": "ocid1.securitylist.oc1.ap-mumbai-1.mock001",
                    "name": "prod-db-security-list",
                    "inbound_rules_count": 2
                },
                {
                    "type": "subnet",
                    "id": "ocid1.subnet.oc1.ap-mumbai-1.mock003",
                    "name": "dev-private-subnet",
                    "cidr": "10.1.1.0/24"
                }
            ]
        }
    ]
}


def scan_vcns(network_client, compartment_id):
    console.print("[bold blue]Scanning VCNs...[/bold blue]")
    try:
        response = network_client.list_vcns(compartment_id=compartment_id)
        vcns = []
        for vcn in response.data:
            vcns.append({
                "id": vcn.id,
                "name": vcn.display_name,
                "cidr": vcn.cidr_block,
                "state": vcn.lifecycle_state,
                "dns_label": vcn.dns_label
            })
        console.print(f"[green]Found {len(vcns)} VCN(s)[/green]")
        return vcns
    except oci.exceptions.ServiceError as e:
        console.print(f"[red]VCN scan failed: {e.message}[/red]")
        return []


def scan_subnets(network_client, compartment_id):
    console.print("[bold blue]Scanning subnets...[/bold blue]")
    try:
        response = network_client.list_subnets(compartment_id=compartment_id)
        subnets = []
        for subnet in response.data:
            subnets.append({
                "id": subnet.id,
                "name": subnet.display_name,
                "vcn_id": subnet.vcn_id,
                "cidr": subnet.cidr_block,
                "availability_domain": subnet.availability_domain,
                "state": subnet.lifecycle_state,
                "public": not subnet.prohibit_public_ip_on_vnic
            })
        console.print(f"[green]Found {len(subnets)} subnet(s)[/green]")
        return subnets
    except oci.exceptions.ServiceError as e:
        console.print(f"[red]Subnet scan failed: {e.message}[/red]")
        return []


def scan_security_lists(network_client, compartment_id):
    console.print("[bold blue]Scanning security lists...[/bold blue]")
    try:
        response = network_client.list_security_lists(compartment_id=compartment_id)
        security_lists = []
        for sl in response.data:
            inbound = []
            for rule in sl.ingress_security_rules:
                inbound.append({
                    "protocol": rule.protocol,
                    "source": rule.source,
                    "source_type": rule.source_type,
                    "stateless": rule.is_stateless
                })
            outbound = []
            for rule in sl.egress_security_rules:
                outbound.append({
                    "protocol": rule.protocol,
                    "destination": rule.destination,
                    "destination_type": rule.destination_type,
                    "stateless": rule.is_stateless
                })
            security_lists.append({
                "id": sl.id,
                "name": sl.display_name,
                "vcn_id": sl.vcn_id,
                "state": sl.lifecycle_state,
                "inbound_rules": inbound,
                "outbound_rules": outbound
            })
        console.print(f"[green]Found {len(security_lists)} security list(s)[/green]")
        return security_lists
    except oci.exceptions.ServiceError as e:
        console.print(f"[red]Security list scan failed: {e.message}[/red]")
        return []


def build_dependency_map(vcns, subnets, security_lists, databases):
    console.print("\n[bold blue]Building dependency map...[/bold blue]")
    dependency_map = []
    for db in databases:
        deps = {
            "resource": db['name'],
            "type": "postgresql",
            "version": db.get('version', 'unknown'),
            "depends_on": []
        }
        for sl in security_lists:
            deps['depends_on'].append({
                "type": "security_list",
                "id": sl['id'],
                "name": sl['name'],
                "inbound_rules_count": len(sl['inbound_rules'])
            })
        for subnet in subnets:
            if not subnet['public']:
                deps['depends_on'].append({
                    "type": "subnet",
                    "id": subnet['id'],
                    "name": subnet['name'],
                    "cidr": subnet['cidr']
                })
        dependency_map.append(deps)
    console.print(f"[green]Mapped {len(dependency_map)} resource dependencies[/green]")
    return dependency_map


def map_mock():
    console.print("[bold yellow]Running in mock mode — no real OCI credentials needed[/bold yellow]\n")
    console.print("[bold blue]Scanning VCNs...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['vcns'])} VCN(s)[/green]")
    console.print("[bold blue]Scanning subnets...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['subnets'])} subnet(s)[/green]")
    console.print("[bold blue]Scanning security lists...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['security_lists'])} security list(s)[/green]")
    return MOCK_DATA

def map_network(config=None, compartment_id=None, databases=None, mock=False):
    console.print(f"\n[bold magenta]Meridian — Oracle Cloud Network Dependency Mapper[/bold magenta]")
    console.print(f"Region: [yellow]{'ap-mumbai-1 (mock)' if mock else 'ap-mumbai-1'}[/yellow]\n")

    if mock:
        return map_mock()

    network_client = oci.core.VirtualNetworkClient(config)
    vcns = scan_vcns(network_client, compartment_id)
    subnets = scan_subnets(network_client, compartment_id)
    security_lists = scan_security_lists(network_client, compartment_id)
    dependency_map = build_dependency_map(
        vcns, subnets, security_lists, databases or []
    )

    return {
        "vcns": vcns,
        "subnets": subnets,
        "security_lists": security_lists,
        "dependency_map": dependency_map
    }