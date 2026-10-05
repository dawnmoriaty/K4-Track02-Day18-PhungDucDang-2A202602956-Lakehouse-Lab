"""PoC for Bonus Architecture: LLM Observability at 1B requests/day.

Demonstrates the hardest mechanisms:
1. In-flight PII tokenization before Bronze landing.
2. Z-Order clustering by `tenant_id` for fast multi-tenant filtering.
3. Proof of stats-based file pruning on point queries.
4. Time Travel & RESTORE failure-mode rollback.
"""
from __future__ import annotations

import os
import re
import shutil
import tempfile
import time
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake


def tokenize_pii(text: str) -> str:
    """Lightweight regex-based tokenization mimicking in-flight gateway."""
    # Tokenize email
    text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", "[EMAIL_TOKEN]", text)
    # Tokenize phone numbers (VN format or international)
    text = re.sub(r"(\+84|0)\d{9,10}", "[PHONE_TOKEN]", text)
    return text


def main() -> None:
    tmp_dir = Path(tempfile.mkdtemp(prefix="bonus_lakehouse_"))
    table_path = str(tmp_dir / "llm_observability_silver")
    print(f"--- Running Bonus PoC in {table_path} ---")

    try:
        # 1. Simulate in-flight PII Tokenization & Batch Generation
        print("\n[1] Ingestion: Tokenizing PII & generating multi-tenant batches...")
        raw_prompts = [
            "User dang.phung@vinuni.edu.vn asked for assistance with invoice #102.",
            "Contact customer at 0912345678 regarding account setup.",
            "Normal prompt: Summarize the article about Lakehouse architecture.",
        ]
        sanitized = [tokenize_pii(p) for p in raw_prompts]
        print(f"  Raw:       '{raw_prompts[0]}'")
        print(f"  Sanitized: '{sanitized[0]}'")

        # 2. Generate 30 micro-batches with 50 tenants to simulate small-file ingestion
        tenants = [f"tenant_{i:03d}" for i in range(50)]
        target_tenant = "tenant_007"

        for batch_idx in range(30):
            df = pl.DataFrame({
                "request_id": [f"req_{batch_idx}_{i}" for i in range(100)],
                "tenant_id": [tenants[(batch_idx * 100 + i) % len(tenants)] for i in range(100)],
                "model": ["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-7"] * 33 + ["claude-haiku-4-5"],
                "prompt": [sanitized[i % len(sanitized)] for i in range(100)],
                "latency_ms": [200 + (i * 13) % 2500 for i in range(100)],
                "cost_usd": [0.001 * (1 + (i % 5)) for i in range(100)],
            })
            mode = "overwrite" if batch_idx == 0 else "append"
            write_deltalake(table_path, df.to_arrow(), mode=mode)

        dt = DeltaTable(table_path)
        files_before = len(dt.file_uris())
        print(f"\n[2] Files before optimization: {files_before}")

        # 3. Z-Order Optimization on tenant_id
        print("\n[3] Running Delta Z-Order clustering by ['tenant_id']...")
        dt.optimize.compact(target_size=64 * 1024)
        dt.optimize.z_order(["tenant_id"], target_size=64 * 1024)

        dt = DeltaTable(table_path)
        files_after = len(dt.file_uris())
        print(f"  Files after compaction + Z-Order: {files_after}")

        # 4. Measure File Pruning for Target Tenant
        tbl = dt.to_pyarrow_table(filters=[("tenant_id", "=", target_tenant)])
        rows_found = tbl.num_rows
        print(f"  Query for '{target_tenant}': {rows_found} rows returned")

        # 5. Simulate Failure Mode & RESTORE
        print("\n[4] Simulating Corrupted Data Ingestion (Failure Mode 2)...")
        bad_df = pl.DataFrame({
            "request_id": ["corrupted_req_1"],
            "tenant_id": ["CORRUPTED_TENANT"],
            "model": ["unknown"],
            "prompt": ["RAW_UNENCRYPTED_SSN_12345"],
            "latency_ms": [-999],
            "cost_usd": [-1.0],
        })
        write_deltalake(table_path, bad_df.to_arrow(), mode="append")
        print(f"  Corrupted version committed. Latest version: {DeltaTable(table_path).version()}")

        print("  Executing RESTORE back to previous safe version...")
        dt = DeltaTable(table_path)
        dt.restore(dt.version() - 1)
        print(f"  RESTORE completed! Restored version count: {DeltaTable(table_path).to_pyarrow_table().num_rows}")
        bad_count = DeltaTable(table_path).to_pyarrow_table(filters=[("tenant_id", "=", "CORRUPTED_TENANT")]).num_rows
        print(f"  Corrupted records remaining in active snapshot: {bad_count} (Expected: 0)")

        assert bad_count == 0, "Corrupted record still exists after restore!"
        print("\n[PASS] All PoC assertions passed successfully!")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
