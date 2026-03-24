import boto3
from rich.console import Console
from botocore.exceptions import ClientError

console = Console()


def get_vpc_details(ec2_client, vpc_id):
    try:
        response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        vpc = response['Vpcs'][0]
        return {
            "id": vpc['VpcId'],
            "cidr": vpc['CidrBlock'],
            "is_default": vpc['IsDefault'],
            "state": vpc['State']
        }
    except Exception:
        return {"id": vpc_id}


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

        # Find security groups attached to this RDS
        # In a real scan we'd get this from describe_db_instances
        # For now map by VPC
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


def map_network(session, rds_instances=None):
    console.print(f"\n[bold magenta]Meridian — Network Dependency Mapper[/bold magenta]")
    console.print(f"Region: [yellow]{session.region_name}[/yellow]\n")

    ec2_client = session.client('ec2')

    vpcs = scan_vpcs(ec2_client)
    subnets = scan_subnets(ec2_client)
    security_groups = scan_security_groups(ec2_client)

    dependency_map = build_dependency_map(
        vpcs,
        subnets,
        security_groups,
        rds_instances or []
    )

    return {
        "vpcs": vpcs,
        "subnets": subnets,
        "security_groups": security_groups,
        "dependency_map": dependency_map
    }