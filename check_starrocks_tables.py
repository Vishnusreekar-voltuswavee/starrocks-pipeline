#!/usr/bin/env python3
"""Quick check: How many tables exist in StarRocks for the schema"""

import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.services.database import StarRocksManager

schema_name = "c1s1_billing_crm_DEV_1_s4JNKRDR_dev"
sr = StarRocksManager()

try:
    if not sr.database_exists(schema_name):
        print(f"Database {schema_name} does NOT exist in StarRocks")
        sys.exit(1)
    
    query = f"SHOW TABLES FROM `{schema_name}`"
    with sr.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n{'='*80}")
    print(f"STARROCKS TABLES IN: {schema_name}")
    print(f"{'='*80}\n")
    print(f"Total tables: {len(tables)}")
    print(f"\nFirst 20 tables:")
    for i, table in enumerate(sorted(tables)[:20], 1):
        print(f"  {i}. {table}")
    if len(tables) > 20:
        print(f"\n... and {len(tables) - 20} more tables")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY:")
    print(f"  - StarRocks has: {len(tables)} tables")
    print(f"  - From logs we know:")
    print(f"    • 581 tables succeeded in previous run")
    print(f"    • 94 tables failed")
    print(f"    • Total expected: 675 tables")
    print(f"\n  - MISSING: {675 - len(tables)} tables need to be processed")
    print(f"{'='*80}\n")

finally:
    sr.close()
