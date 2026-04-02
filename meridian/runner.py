"""
Meridian background runner — called by meridian replicate --bg
Runs the full replication pipeline in background, logs to meridian.log
"""
import os
import sys
import json
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('meridian.log')
    ]
)
log = logging.getLogger('meridian')


def get_aws_config():
    return {
        "host": os.getenv('AWS_RDS_HOST'),
        "port": int(os.getenv('AWS_RDS_PORT', 5432)),
        "database": os.getenv('AWS_RDS_DATABASE'),
        "user": os.getenv('AWS_RDS_USER'),
        "password": os.getenv('AWS_RDS_PASSWORD'),
        "sslmode": os.getenv('AWS_RDS_SSLMODE', 'prefer'),
        "sslrootcert": os.getenv('AWS_RDS_SSLROOTCERT', '')
    }


def get_oracle_config():
    return {
        "host": os.getenv('ORACLE_PG_HOST'),
        "port": int(os.getenv('ORACLE_PG_PORT', 5432)),
        "database": os.getenv('ORACLE_PG_DATABASE'),
        "user": os.getenv('ORACLE_PG_USER'),
        "password": os.getenv('ORACLE_PG_PASSWORD'),
        "sslmode": os.getenv('ORACLE_PG_SSLMODE', 'require'),
        "fqdn": os.getenv('ORACLE_PG_FQDN')
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', help='Path to .env file', default='.env')
    args = parser.parse_args()

    # Load env
    load_dotenv(args.env)

    log.info("=" * 60)
    log.info("Meridian background runner started")
    log.info(f"PID: {os.getpid()}")
    log.info(f"Started at: {datetime.utcnow().isoformat()}")
    log.info("=" * 60)

    src_cfg = get_aws_config()
    tgt_cfg = get_oracle_config()

    log.info(f"Source: {src_cfg['database']} @ {src_cfg['host']}")
    log.info(f"Target: {tgt_cfg['database']} @ {tgt_cfg['host']}")

    try:
        from meridian.replicator.replicator import replicate

        result = replicate(
            source_db=src_cfg['database'],
            target_db=tgt_cfg['database'],
            mock=False,
            source_config=src_cfg,
            target_config=tgt_cfg,
            background=True
        )

        if result:
            log.info("=" * 60)
            log.info("Migration complete!")
            log.info(f"Total rows: {result['summary']['total_rows']:,}")
            log.info(f"CDC status: {result['pglogical']['status']}")
            log.info("=" * 60)

    except Exception as e:
        log.error(f"Migration failed: {e}")
        log.error("Run: meridian state to see current state")
        log.error("Run: meridian replicate --env to resume")
        sys.exit(1)


if __name__ == '__main__':
    main()