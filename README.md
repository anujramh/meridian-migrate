# Meridian

> Zero-downtime cross-cloud data migration engine — free forever.

Migrating data across clouds is painful, manual, and risky. Meridian automates the hardest parts — resource discovery, schema translation, live replication, and parity validation — so you can cut over with confidence and zero downtime.

Built by engineers who have done it the hard way across AWS, GCP, Azure, and Oracle Cloud.

---

## What Meridian does

- **Discovers** all resources in your cloud account automatically — no inventory needed
- **Maps** dependencies between resources before migration starts
- **Validates** schema compatibility between source and target clouds
- **Replicates** data live with CDC-based streaming — zero downtime
- **Validates parity** continuously — blocks cutover if drift is detected
- **Orchestrates cutover** with automated rollback if error rates spike

---

## Supported clouds

| Cloud | Compute | PostgreSQL | MongoDB | Object Storage |
|-------|---------|------------|---------|----------------|
| AWS | ✅ | ✅ | ✅ | ✅ |
| Oracle Cloud | ✅ | ✅ | 🔜 | ✅ |
| GCP | 🔜 | 🔜 | 🔜 | 🔜 |
| Azure | 🔜 | 🔜 | 🔜 | 🔜 |

---

## Supported migration paths

- AWS → GCP
- AWS → Azure
- GCP → Oracle Cloud
- Azure → AWS
- Any → Any (expanding with community contributions)

---

## Quickstart

### Install
```bash
git clone https://github.com/anujramh/meridian-migrate.git
cd meridian-migrate
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

### Try it instantly — no credentials needed
```bash
# Scan AWS (mock mode)
meridian scan-aws --mock

# Scan Oracle Cloud (mock mode)
meridian scan-oracle --mock
```

### Scan a real AWS account
```bash
meridian scan-aws --profile default --region us-east-1
```

### Scan a real Oracle Cloud account
```bash
meridian scan-oracle --profile DEFAULT --region ap-mumbai-1 --compartment <compartment-ocid>
```

### Save inventory to file
```bash
meridian scan-aws --mock --output inventory.json
meridian scan-oracle --mock --output inventory.json
```

---

## Example output
```json
{
  "source": "oracle",
  "region": "ap-mumbai-1",
  "scanned_at": "2026-03-22T09:55:21.800625",
  "mock": false,
  "resources": {
    "compute": [
      {
        "id": "ocid1.instance.oc1..xxxxx",
        "name": "prod-app-server-01",
        "shape": "VM.Standard2.2",
        "status": "RUNNING",
        "region": "ap-mumbai-1"
      }
    ],
    "postgresql": [
      {
        "id": "ocid1.dbsystem.oc1..xxxxx",
        "name": "prod-postgres-db-01",
        "version": "14.9",
        "status": "ACTIVE",
        "region": "ap-mumbai-1"
      }
    ],
    "object_storage": [
      {
        "name": "prod-assets-bucket",
        "region": "ap-mumbai-1"
      }
    ]
  }
}
```

---

## Roadmap

- [x] AWS scanner — RDS, S3
- [x] Oracle Cloud scanner — compute, PostgreSQL, object storage
- [x] Mock mode for both clouds
- [ ] GCP scanner
- [ ] Azure scanner
- [ ] Schema diff and compatibility checker
- [ ] CDC-based live replication engine
- [ ] Parity validator
- [ ] Cutover orchestrator
- [ ] Web dashboard

---

## Contributing

Meridian is fully open source and community driven. All contributions welcome.

- Found a bug? Open an issue
- Want to add a cloud provider? Check `meridian/scanners/`
- Want to add a migration playbook? Check `docs/playbooks/`
- Questions? Start a discussion on GitHub

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Apache 2.0 — free forever. See [LICENSE](LICENSE).