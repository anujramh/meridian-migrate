import boto3
from rich.console import Console
from botocore.exceptions import ClientError

console = Console()

MOCK_DATA = {
    "vpcs": [
        {
            "id": "vpc-0a1b2c3d4e5f67890",
            "cidr": "10.0.0.0/16",
            "is_default": False,
            "state": "available"
        },
        {
            "id": "vpc-0f9e8d7c6b5a43210",
            "cidr": "172.31.0.0/16",
            "is_default": True,
            "state": "available"
        }
    ],
    "subnets": [
        {
            "id": "subnet-0a1b2c3d4e5f67890",
            "vpc_id": "vpc-0a1b2c3d4e5f67890",
            "cidr": "10.0.1.0/24",
            "availability_zone": "us-east-1a",
            "available_ips": 251,
            "public": False
        },
        {
            "id": "subnet-0b2c3d4e5f6789012",
            "vpc_id": "vpc-0a1b2c3d4e5f67890",
            "cidr": "10.0.2.0/24",
            "availability_zone": "us-east-1b",
            "available_ips": 251,
            "public": False
        },
        {
            "id": "subnet-0c3d4e5f678901234",
            "vpc_id": "vpc-0a1b2c3d4e5f67890",
            "cidr": "10.0.0.0/24",
            "availability_zone": "us-east-1a",
            "available_ips": 251,
            "public": True
        }
    ],
    "security_groups": [
        {
            "id": "sg-0a1b2c3d4e5f67890",
            "name": "prod-rds-sg",
            "description": "Security group for production RDS",
            "vpc_id": "vpc-0a1b2c3d4e5f67890",
            "inbound_rules": [
                {
                    "protocol": "tcp",
                    "from_port": 5432,
                    "to_port": 5432,
                    "sources": ["10.0.1.0/24", "10.0.2.0/24"]
                }
            ],
            "outbound_rules": [
                {
                    "protocol": "-1",
                    "from_port": "all",
                    "to_port": "all",
                    "destinations": ["0.0.0.0/0"]
                }
            ]
        },
        {
            "id": "sg-0b2c3d4e5f6789012",
            "name": "prod-app-sg",
            "description": "Security group for application servers",
            "vpc_id": "vpc-0a1b2c3d4e5f67890",
            "inbound_rules": [
                {
                    "protocol": "tcp",
                    "from_port": 80,
                    "to_port": 80,
                    "sources": ["0.0.0.0/0"]
                },
                {
                    "protocol": "tcp",
                    "from_port": 443,
                    "to_port": 443,
                    "sources": ["0.0.0.0/0"]
                }
            ],
            "outbound_rules": [
                {
                    "protocol": "-1",
                    "from_port": "all",
                    "to_port": "all",
                    "destinations": ["0.0.0.0/0"]
                }
            ]
        }
    ],
    "dependency_map": [
        {
            "resource": "prod-postgres-01",
            "type": "rds",
            "engine": "postgres",
            "depends_on": [
                {
                    "type": "security_group",
                    "id": "sg-0a1b2c3d4e5f67890",
                    "name": "prod-rds-sg",
                    "inbound_ports": [5432]
                },
                {
                    "type": "subnet",
                    "id": "subnet-0a1b2c3d4e5f67890",
                    "cidr": "10.0.1.0/24",
                    "availability_zone": "us-east-1a"
                }
            ]
        },
        {
            "resource": "staging-postgres-01",
            "type": "rds",
            "engine": "postgres",
            "depends_on": [
                {
                    "type": "security_group",
                    "id": "sg-0a1b2c3d4e5f67890",
                    "name": "prod-rds-sg",
                    "inbound_ports": [5432]
                },
                {
                    "type": "subnet",
                    "id": "subnet-0b2c3d4e5f6789012",
                    "cidr": "10.0.2.0/24",
                    "availability_zone": "us-east-1b"
                }
            ]
        }
    ]
}


def scan_vpcs(ec2_client):
    console.print("[bold blue]Scanning VPCs...[/bold blue]")
    try:
        response = ec2_client.describe_vpcs()
        vpcs = []
        for vpc in response['Vpcs']:
            vpcs.append({
                "id": vpc['VpcId'],
                "cidr": vpc['CidrBlock'],
                "is_default": vpc['IsDefault'],
                "state": vpc['State']
            })
        console.print(f"[green]Found {len(vpcs)} VPC(s)[/green]")
        return vpcs
    except ClientError as e:
        console.print(f"[red]VPC scan failed: {e.response['Error']['Message']}[/red]")
        return []


def scan_subnets(ec2_client):
    console.print("[bold blue]Scanning subnets...[/bold blue]")
    try:
        response = ec2_client.describe_subnets()
        subnets = []
        for subnet in response['Subnets']:
            subnets.append({
                "id": subnet['SubnetId'],
                "vpc_id": subnet['VpcId'],
                "cidr": subnet['CidrBlock'],
                "availability_zone": subnet['AvailabilityZone'],
                "available_ips": subnet['AvailableIpAddressCount'],
                "public": subnet['MapPublicIpOnLaunch']
            })
        console.print(f"[green]Found {len(subnets)} subnet(s)[/green]")
        return subnets
    except ClientError as e:
        console.print(f"[red]Subnet scan failed: {e.response['Error']['Message']}[/red]")
        return []


def scan_security_groups(ec2_client):
    console.print("[bold blue]Scanning security groups...[/bold blue]")
    try:
        response = ec2_client.describe_security_groups()
        groups = []
        for sg in response['SecurityGroups']:
            inbound = []
            for rule in sg['IpPermissions']:
                inbound.append({
                    "protocol": rule.get('IpProtocol', 'all'),
                    "from_port": rule.get('FromPort', 'all'),
                    "to_port": rule.get('ToPort', 'all'),
                    "sources": [r['CidrIp'] for r in rule.get('IpRanges', [])]
                })
            outbound = []
            for rule in sg['IpPermissionsEgress']:
                outbound.append({
                    "protocol": rule.get('IpProtocol', 'all'),
                    "from_port": rule.get('FromPort', 'all'),
                    "to_port": rule.get('ToPort', 'all'),
                    "destinations": [r['CidrIp'] for r in rule.get('IpRanges', [])]
                })
            groups.append({
                "id": sg['GroupId'],
                "name": sg['GroupName'],
                "description": sg['Description'],
                "vpc_id": sg.get('VpcId', None),
                "inbound_rules": inbound,
                "outbound_rules": outbound
            })
        console.print(f"[green]Found {len(groups)} security group(s)[/green]")
        return groups
    except ClientError as e:
        console.print(f"[red]Security groups scan failed: {e.response['Error']['Message']}[/red]")
        return []


def build_dependency_map(vpcs, subnets, security_groups, rds_instances):
    console.print("\n[bold blue]Building dependency map...[/bold blue]")
    dependency_map = []
    for rds in rds_instances:
        deps = {
            "resource": rds['id'],
            "type": "rds",
            "engine": rds['engine'],
            "depends_on": []
        }
        for sg in security_groups:
            if sg.get('vpc_id'):
                deps['depends_on'].append({
                    "type": "security_group",
                    "id": sg['id'],
                    "name": sg['name'],
                    "inbound_ports": [
                        r.get('from_port') for r in sg['inbound_rules']
                        if r.get('from_port') != 'all'
                    ]
                })
        dependency_map.append(deps)
    console.print(f"[green]Mapped {len(dependency_map)} resource dependencies[/green]")
    return dependency_map


def map_mock():
    console.print("[bold yellow]Running in mock mode — no real AWS credentials needed[/bold yellow]\n")
    console.print("[bold blue]Scanning VPCs...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['vpcs'])} VPC(s)[/green]")
    console.print("[bold blue]Scanning subnets...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['subnets'])} subnet(s)[/green]")
    console.print("[bold blue]Scanning security groups...[/bold blue]")
    console.print(f"[green]Found {len(MOCK_DATA['security_groups'])} security group(s)[/green]")
    console.print("\n[bold blue]Building dependency map...[/bold blue]")
    console.print(f"[green]Mapped {len(MOCK_DATA['dependency_map'])} resource dependencies[/green]")
    return MOCK_DATA


def map_network(session, rds_instances=None, mock=False):
    console.print(f"\n[bold magenta]Meridian — AWS Network Dependency Mapper[/bold magenta]")
    console.print(f"Region: [yellow]{session.region_name if not mock else 'us-east-1 (mock)'}[/yellow]\n")

    if mock:
        return map_mock()

    ec2_client = session.client('ec2')
    vpcs = scan_vpcs(ec2_client)
    subnets = scan_subnets(ec2_client)
    security_groups = scan_security_groups(ec2_client)
    dependency_map = build_dependency_map(
        vpcs, subnets, security_groups, rds_instances or []
    )

    return {
        "vpcs": vpcs,
        "subnets": subnets,
        "security_groups": security_groups,
        "dependency_map": dependency_map
    }