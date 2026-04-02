# Meridian

> Zero-downtime cross-cloud data migration engine — free forever.

Migrating data across clouds is painful, manual, and risky. Meridian automates the entire workflow — from discovery to cutover — so you can migrate with confidence and minimal downtime.

Built by engineers who have done it the hard way across AWS, GCP, Azure, and Oracle Cloud.

---

## How it works

Meridian uses a proven pattern for zero data loss migrations:
```
1. Lock WAL position on source (before anything starts)
2. pg_dump — consistent snapshot from locked WAL position
3. pg_restore — load snapshot to target
4. VACUUM ANALYZE — optimize target after restore
5. pglogical CDC — replay all changes from WAL position onwards
6. Monitor — confirm replication is live and lag is zero
7. Validate — row count + checksum parity check
8. Cutover — orchestrated 10-step process
9. Cleanup — remove replication objects
```

**Why WAL position locking matters:**

For a 100GB database, pg_dump takes 2+ hours. During those 2 hours engineers keep writing to the source database. Without WAL locking — those changes are lost silently.

Meridian locks the WAL position BEFORE dump starts. pglogical then replays ALL changes from that exact position — including everything that happened during the dump window. Zero data loss guaranteed.

---

## The complete migration workflow
```bash
# Step 1 — Pre-flight check
meridian analyze-schema --env

# Step 2 — Fix missing schema on target (if needed)
meridian fix-schema --env

# Step 3 — Replicate data live
meridian replicate --env

# Step 4 — Check replication status
meridian status --env

# Step 5 — Monitor live replication lag
meridian monitor --env

# Step 6 — Validate parity
meridian validate --env

# Step 7 — Cutover
meridian cutover --env

# Step 8 — Cleanup replication objects
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
✅ Replication slot created — WAL position locked at B/30000060
✅ Data dumped to meridian_data_meridiandb.sql
✅ Data restored to target
✅ VACUUM ANALYZE complete
✅ Init replication slot cleaned up

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

### Status check
```
Meridian — Replication Status
  Source: meridiandb (AWS RDS)
  Target: postgres (Oracle Cloud)

  ✅ Status: replicating
  Subscription:  meridian_subscription
  Provider:      meridian_provider

  Table parity:
  ✅ orders    5,000 rows — in sync
  ✅ products    500 rows — in sync
  ✅ users     1,001 rows — in sync

  ✅ Total: 6,501 rows — perfectly in sync
```

### Live monitor
```
Meridian — Live Replication Monitor
  Refresh: every 5s · Alert lag threshold: 100 rows

2026-04-02 07:34:21 ✅ replicating · source=6,501 target=6,501 lag=0 rows
2026-04-02 07:34:29 ✅ replicating · source=6,501 target=6,501 lag=0 rows
2026-04-02 07:34:37 ✅ replicating · source=6,501 target=6,501 lag=0 rows
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

## Supported migration paths

| Source | Target | Status |
|--------|--------|--------|
| AWS RDS PostgreSQL | Oracle Cloud PostgreSQL | ✅ Tested in production |
| GCP Cloud SQL | Oracle Cloud PostgreSQL | 🔜 Coming soon |
| Azure PostgreSQL | Oracle Cloud PostgreSQL | 🔜 Coming soon |

## Cloud scanners (discovery only)

| Cloud | Scanner | Network Mapper |
|-------|---------|----------------|
| AWS | ✅ | ✅ |
| Oracle Cloud | ✅ | ✅ |
| GCP | 🔜 | 🔜 |
| Azure | 🔜 | 🔜 |

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
meridian replicate --env             # Start replication
meridian status --env                # One-shot replication status
meridian monitor --env               # Live replication dashboard
meridian validate --env              # Validate parity
meridian cutover --env               # Execute cutover
meridian cleanup --env               # Remove replication objects
meridian scan-rds --env              # Scan AWS RDS database
meridian scan-oracle-db --env        # Scan Oracle PostgreSQL database
meridian scan-aws --env              # Scan AWS resources
meridian scan-oracle --env           # Scan Oracle Cloud resources
meridian map-aws --env               # Map AWS network dependencies
meridian map-oracle --env            # Map Oracle network dependencies
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
- [x] WAL position locking — zero data loss during dump window
- [x] pg_dump initial load
- [x] pglogical CDC replication
- [x] Replication status command
- [x] Live replication monitor with alerts
- [x] Parity validator — row count + checksum
- [x] Cutover orchestrator — 10 steps
- [x] Post-cutover cleanup
- [ ] Background mode — run dump in background
- [ ] State file — resume interrupted migrations
- [ ] GCP scanner
- [ ] Azure scanner
- [ ] Reverse replication for rollback
- [ ] Web dashboard

---

## Contributing

Meridian is fully open source and community driven. All contributions welcome.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Apache 2.0 — free forever. See [LICENSE](LICENSE).