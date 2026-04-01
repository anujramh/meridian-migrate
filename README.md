# Meridian

> Zero-downtime cross-cloud data migration engine — free forever.

Migrating data across clouds is painful, manual, and risky. Meridian automates the entire workflow — from discovery to cutover — so you can migrate with confidence and minimal downtime.

Built by engineers who have done it the hard way across AWS, GCP, Azure, and Oracle Cloud.

---

## The complete migration workflow
```bash
# Step 1 — Pre-flight check
meridian analyze-schema --env

# Step 2 — Fix missing schema on target (if needed)
meridian fix-schema --env

# Step 3 — Replicate data live
meridian replicate --env

# Step 4 — Validate parity
meridian validate --env

# Step 5 — Cutover
meridian cutover --env

# Step 6 — Cleanup replication objects
meridian cleanup --env
```

---

## Try it instantly — no credentials needed

Every command supports `--mock` mode. No cloud accounts required.
```bash
# Install
git clone https://github.com/anujramh/meridian-migrate.git
cd meridian-migrate
python3 -m venv venv
source venv/bin/activate
pip install -e .

# Run full pipeline in mock mode
meridian analyze-schema --mock
meridian replicate --mock --env
meridian validate --mock --env
meridian cutover --mock --env
```

---

## Configuration

Create a `.env` file in the project root:
```env
# AWS RDS Source
AWS_RDS_HOST=your-db.region.rds.amazonaws.com
AWS_RDS_PORT=5432
AWS_RDS_DATABASE=yourdb
AWS_RDS_USER=youruser
AWS_RDS_PASSWORD=yourpassword

# SSL — use disable when connecting over VPN tunnel
# VPN encrypts traffic at network level
AWS_RDS_SSLMODE=disable
AWS_RDS_SSLROOTCERT=

# Oracle Cloud PostgreSQL Target
ORACLE_PG_HOST=your-oracle-host
ORACLE_PG_PORT=5432
ORACLE_PG_DATABASE=postgres
ORACLE_PG_USER=postgres
ORACLE_PG_PASSWORD=yourpassword

# FQDN — required for pglogical subscriber node
# Must be hostname not IP address
ORACLE_PG_FQDN=your-oracle-fqdn.example.com
```

---

## Pre-flight checklist

`meridian analyze-schema --env` runs 15 checks before migration starts:
```
✅ Network reachable — source and target
✅ DNS resolution — hostnames resolve correctly
✅ ORACLE_PG_FQDN set
✅ pglogical installed on source and target
✅ wal_level=logical on source and target
✅ max_wal_senders and max_replication_slots sufficient
✅ All tables have primary keys
✅ No unlogged tables
✅ No large objects
✅ pg_dump and pg_restore available
✅ All source tables exist on target
✅ SSL configuration correct for VPN
⚠️  Sequences — sync manually after CDC catches up
⚠️  DDL changes not replicated by pglogical automatically
```

---

## Real migration output

### Pre-flight check
```
Meridian — Pre-flight Migration Checklist
─────────────────────────────────────────
  Source DB:  meridiandb (PostgreSQL 17.6 on AWS RDS)
  Target DB:  postgres (PostgreSQL 16.8 OCI Optimized)

  ❌ Critical issues:  0
  ⚠️  Warnings:         4
  ✅ Passed checks:    11

  ✅ READY TO REPLICATE WITH PGLOGICAL
```

### Replication
```
Phase 1 — Initial data load (pg_dump + pg_restore)
✅ Data dumped to meridian_data_meridiandb.sql
✅ Data restored to target
✅ VACUUM ANALYZE complete

Phase 2 — Setting up pglogical provider on source
✅ Provider node created
✅ Replication set created
✅ All tables added to replication set

Phase 3 — Setting up pglogical subscriber on target
✅ Subscriber node created
✅ Subscription created — CDC streaming started

Phase 4 — Monitoring replication status
✅ Subscription status: replicating — provider: meridian_provider
✅ CDC replication confirmed active
```

### Parity validation
```
✅ orders   — 5,000 rows · checksum match
✅ products —   500 rows · checksum match
✅ users    — 1,001 rows · checksum match

✅ PARITY CONFIRMED — ready for cutover
```

### Cutover
```
✅ Step 1:  Final parity check — all tables match
✅ Step 2:  Stop writes to source
✅ Step 3:  CDC lag at zero
✅ Step 4:  Final checksum verified
✅ Step 5:  Subscription disabled
✅ Step 6:  Sequences synced
✅ Step 7:  Target writes enabled
✅ Step 8:  Health check passed
✅ Step 9:  Connection strings updated
✅ Step 10: Reverse replication noted

🎉 CUTOVER COMPLETE
```

### Cleanup
```
✅ Dropped subscription on target
✅ Dropped subscriber node on target
✅ Dropped replication set on source
✅ Dropped provider node on source

🎉 Cleanup complete — migration fully finalized
```

---

## Supported clouds

| Cloud | Compute | PostgreSQL | MongoDB | Object Storage |
|-------|---------|------------|---------|----------------|
| AWS | ✅ | ✅ | ✅ | ✅ |
| Oracle Cloud | ✅ | ✅ | 🔜 | ✅ |
| GCP | 🔜 | 🔜 | 🔜 | 🔜 |
| Azure | 🔜 | 🔜 | 🔜 | 🔜 |

## Supported migration paths

- AWS → Oracle Cloud ✅ (tested in production)
- AWS → GCP 🔜
- AWS → Azure 🔜
- GCP → Oracle Cloud 🔜
- Azure → AWS 🔜

---

## Prerequisites

**On source (AWS RDS):**
- PostgreSQL with `wal_level=logical`
- `rds.logical_replication=1` in parameter group
- `rds.force_ssl=0` if connecting over VPN
- pglogical in `shared_preload_libraries`
- `CREATE EXTENSION pglogical`
- All tables must have primary keys

**On target (Oracle Cloud PostgreSQL):**
- pglogical enabled in configuration
- `wal_level=logical`
- `track_commit_timestamp=1`
- `CREATE EXTENSION pglogical`

**Network:**
- VPN tunnel between source and target clouds
- Private DNS zone in target cloud resolving source hostname
- Source security group allowing target IP on port 5432

---

## All commands
```bash
meridian analyze-schema --env        # Pre-flight checklist
meridian fix-schema --env            # Dump + restore schema
meridian scan-rds --env              # Scan AWS RDS database
meridian scan-oracle-db --env        # Scan Oracle PostgreSQL database
meridian scan-aws --env              # Scan AWS resources
meridian scan-oracle --env           # Scan Oracle Cloud resources
meridian map-aws --env               # Map AWS network dependencies
meridian map-oracle --env            # Map Oracle network dependencies
meridian replicate --env             # Start replication
meridian validate --env              # Validate parity
meridian cutover --env               # Execute cutover
meridian cleanup --env               # Remove replication objects
```

All commands support `--mock` for testing without credentials.
All commands support `--env` to load credentials from `.env` file.
All commands accept CLI arguments to override `.env` values.

---

## Project status

- [x] AWS scanner
- [x] Oracle Cloud scanner
- [x] Network dependency mapper
- [x] Pre-flight checklist — 15 checks
- [x] Schema diff analyzer
- [x] pg_dump initial load
- [x] pglogical CDC replication
- [x] Parity validator — row count + checksum
- [x] Cutover orchestrator — 10 steps
- [x] Post-cutover cleanup
- [ ] GCP scanner
- [ ] Azure scanner
- [ ] Reverse replication for rollback
- [ ] Web dashboard
- [ ] Monitoring and alerting

---

## Contributing

Meridian is fully open source and community driven. All contributions welcome.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Apache 2.0 — free forever. See [LICENSE](LICENSE).