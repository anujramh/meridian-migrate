# Meridian

> Zero-downtime cross-cloud data migration engine — free forever.

Migrating data across clouds is painful, manual, and risky. Meridian automates the entire workflow — from discovery to cutover — so you can migrate with confidence and zero downtime.

Built by engineers who have done it the hard way across AWS, GCP, Azure, and Oracle Cloud.

---

## The complete migration workflow
```bash
# Step 1 — Discover what exists
meridian scan-aws --profile default --region us-east-1
meridian scan-oracle --profile default

# Step 2 — Map network dependencies
meridian map-aws --profile default
meridian map-oracle --profile default

# Step 3 — Analyze schema compatibility
meridian analyze-schema \
  --source-db prod-postgres-01 \
  --target-db prod-carsdk-cluster

# Step 4 — Replicate data live
meridian replicate \
  --source-db prod-postgres-01 \
  --target-db prod-carsdk-cluster

# Step 5 — Validate parity
meridian validate \
  --source-db prod-postgres-01 \
  --target-db prod-carsdk-cluster

# Step 6 — Cutover
meridian cutover \
  --source-db prod-postgres-01 \
  --target-db prod-carsdk-cluster
```

---

## Try it instantly — no credentials needed

Every command supports `--mock` mode. No AWS or Oracle Cloud account required.
```bash
# Install
git clone https://github.com/anujramh/meridian-migrate.git
cd meridian-migrate
python3 -m venv venv
source venv/bin/activate
pip install -e .

# Run the full migration pipeline in mock mode
meridian scan-aws --mock
meridian scan-oracle --mock
meridian map-aws --mock
meridian map-oracle --mock
meridian analyze-schema --source-db prod-postgres-01 --target-db prod-carsdk-cluster --mock
meridian replicate --source-db prod-postgres-01 --target-db prod-carsdk-cluster --mock
meridian validate --source-db prod-postgres-01 --target-db prod-carsdk-cluster --mock
meridian cutover --source-db prod-postgres-01 --target-db prod-carsdk-cluster --mock
```

---

## What Meridian does

| Step | Command | What it does |
|------|---------|--------------|
| Discover | `scan-aws` / `scan-oracle` | Inventory all resources — compute, databases, storage |
| Map | `map-aws` / `map-oracle` | Map network dependencies — VPCs, subnets, security groups |
| Analyze | `analyze-schema` | Detect schema incompatibilities before migration starts |
| Replicate | `replicate` | Live CDC-based data replication — zero downtime |
| Validate | `validate` | Row count, checksum and sample parity checks |
| Cutover | `cutover` | 8-step orchestrated cutover with automatic rollback |

---

## Supported clouds

| Cloud | Compute | PostgreSQL | MongoDB | Object Storage |
|-------|---------|------------|---------|----------------|
| AWS | ✅ | ✅ | ✅ | ✅ |
| Oracle Cloud | ✅ | ✅ | 🔜 | ✅ |
| GCP | 🔜 | 🔜 | 🔜 | 🔜 |
| Azure | 🔜 | 🔜 | 🔜 | 🔜 |

## Supported migration paths

- AWS → Oracle Cloud ✅
- AWS → GCP 🔜
- AWS → Azure 🔜
- GCP → Oracle Cloud 🔜
- Azure → AWS 🔜

---

## Example output

### Schema analysis
```
──────────────────────────────────────────────────
  Meridian — Schema Analysis Summary
──────────────────────────────────────────────────
  Source DB:  prod-postgres-01 (AWS RDS PostgreSQL 14.9)
  Target DB:  prod-carsdk-cluster (Oracle Managed PostgreSQL 15.12)

  ❌ Critical issues:  2
  ⚠️  Warnings:         6
  ✅ Passed checks:    4

  CRITICAL — fix before migrating:
  ❌ Extension timescaledb: not available on Oracle Managed PostgreSQL
  ❌ wal_level: must be set to logical for CDC replication

  🚫 NOT READY TO MIGRATE
──────────────────────────────────────────────────
```

### Cutover
```
──────────────────────────────────────────────────
  Meridian — Cutover Summary
──────────────────────────────────────────────────
  ✅ Step 1: Final parity check — all tables match
  ✅ Step 2: Stop writes to source
  ✅ Step 3: Apply final CDC events
  ✅ Step 4: Final checksum verification
  ✅ Step 5: Update connection strings
  ✅ Step 6: Enable writes on target
  ✅ Step 7: Health check — error rate 0.0%
  ✅ Step 8: Disable source database

  🎉 CUTOVER COMPLETE — 6 seconds downtime
──────────────────────────────────────────────────
```

---

## Project status

This project is under active development. The current release covers the complete mock workflow end to end. Real database connections and live replication engine are in development.

- [x] AWS scanner
- [x] Oracle Cloud scanner
- [x] AWS network dependency mapper
- [x] Oracle Cloud network dependency mapper
- [x] Schema diff analyzer
- [x] Replication engine (mock)
- [x] Parity validator (mock)
- [x] Cutover orchestrator (mock)
- [ ] Real CDC replication engine
- [ ] Real parity validation
- [ ] Real cutover execution
- [ ] GCP scanner
- [ ] Azure scanner
- [ ] Web dashboard

---

## Contributing

Meridian is fully open source and community driven. All contributions welcome.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Apache 2.0 — free forever. See [LICENSE](LICENSE).

