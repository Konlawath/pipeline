"""
Thammasat Data & AI Workshop
Idempotent Batch Pipeline — PostgreSQL version

Usage:
  python scripts/batch_pipeline.py \
    --business_date 2026-06-28 \
    --run_id RUN001 \
    --input_file thammasat_workshop_dataset.xlsx \
    --sheet_name workshop_data

Requirements:
  pip install pandas openpyxl psycopg2-binary python-dotenv
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ─── Config ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
for sub in ["raw", "staging", "trusted", "analytics", "rag", "audit"]:
    os.makedirs(os.path.join(OUTPUT_DIR, sub), exist_ok=True)


# ─── DB Connection ─────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "workshop_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


# ─── Helpers ───────────────────────────────────────────────────────────────
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def checksum(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def log_run(cur, run_id, business_date, start_time, end_time, status,
            file_name, sheet_name, file_checksum,
            source_count, loaded_count, rejected_count, error_message=""):
    cur.execute("""
        INSERT INTO workshop.batch_audit
            (run_id, business_date, start_time, end_time, status,
             input_file_name, sheet_name, input_file_checksum,
             source_count, loaded_count, rejected_count, error_message)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (run_id, business_date, start_time, end_time, status,
          file_name, sheet_name, file_checksum,
          source_count, loaded_count, rejected_count, error_message))


# ─── Step 1 — Read Excel ───────────────────────────────────────────────────
def step_read(input_file: str, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(input_file, sheet_name=sheet_name, dtype=str)
    df = df.fillna("")
    print(f"  [READ] {len(df)} rows, {len(df.columns)} columns")
    return df


# ─── Step 2 — Raw Layer ────────────────────────────────────────────────────
def step_raw(cur, df: pd.DataFrame, run_id, business_date,
             file_name, file_checksum):
    """Insert all source rows as TEXT into raw table."""
    ts = now_utc()
    cols = [c for c in df.columns]
    placeholders = ", ".join(["%s"] * (len(cols) + 5))
    sql = f"""
        INSERT INTO workshop.raw_workshop_data
            (run_id, business_date, input_file_name, input_file_checksum,
             load_timestamp, {", ".join(cols)})
        VALUES ({placeholders})
    """
    rows = [
        (run_id, business_date, file_name, file_checksum, ts) + tuple(str(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    psycopg2.extras.execute_batch(cur, sql, rows)

    # Export CSV
    path = os.path.join(OUTPUT_DIR, "raw", f"raw_{business_date}_{run_id}.csv")
    df.to_csv(path, index=False)
    print(f"  [RAW]  {len(rows)} rows inserted → {path}")
    return df


# ─── Step 3 — Staging Layer ────────────────────────────────────────────────
def step_staging(cur, df: pd.DataFrame, run_id, business_date,
                 file_name, file_checksum):
    """Type-cast, filter to student rows, insert into staging."""

    stg = df[df["record_type"] == "student"].copy()
    rejected = len(df) - len(stg)

    def to_date(val):
        try:
            return pd.to_datetime(val).date() if val else None
        except Exception:
            return None

    def to_num(val, cast=float):
        try:
            return cast(val) if val != "" else None
        except Exception:
            return None

    def to_bool(val):
        return str(val).upper() in ("TRUE", "1", "YES", "Y")

    ts = now_utc()
    rows = []
    for _, r in stg.iterrows():
        rows.append((
            run_id, business_date, file_name, file_checksum, ts, "staged",
            r["record_type"],
            r["entity_id"],
            to_date(r["snapshot_date"]),
            r["student_no"], r["citizen_id"], r["email"],
            r["mobile"], r["student_name"], r["level"],
            r["faculty_or_school"], r["program_id"], r["program_name"],
            r["discipline_cluster"], r["campus"],
            to_num(r["year_of_study"], int),
            r["admission_channel"], r["status"],
            to_num(r["gpa"]),
            to_num(r["credit_earned"], int),
            to_bool(r["is_international"]),
            r["career_interest"],
            to_num(r["expected_salary_thb"]),
            r["internship_interest"], r["behavior_profile"],
            r["teamwork_style"], r["learning_preference"],
            to_num(r["source_row_no"], int),
            r["source_url"],
            r["rag_document_title"], r["rag_document_text"],
            r["rag_keywords"], r["rag_sample_question"],
            r["rag_expected_answer_hint"],
        ))

    sql = """
        INSERT INTO workshop.stg_student_snapshot
            (run_id, business_date, input_file_name, input_file_checksum,
             load_timestamp, record_status,
             record_type, entity_id, snapshot_date,
             student_no, citizen_id, email, mobile, student_name, level,
             faculty_or_school, program_id, program_name,
             discipline_cluster, campus, year_of_study,
             admission_channel, status, gpa, credit_earned, is_international,
             career_interest, expected_salary_thb, internship_interest,
             behavior_profile, teamwork_style, learning_preference,
             source_row_no, source_url,
             rag_document_title, rag_document_text, rag_keywords,
             rag_sample_question, rag_expected_answer_hint)
        VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s,%s,
                %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s,
                %s,%s,%s, %s,%s,%s, %s,%s, %s,%s,%s,%s,%s)
    """
    psycopg2.extras.execute_batch(cur, sql, rows)

    path = os.path.join(OUTPUT_DIR, "staging", f"stg_{business_date}_{run_id}.csv")
    stg.to_csv(path, index=False)
    print(f"  [STG]  {len(rows)} rows staged, {rejected} skipped → {path}")
    return stg, rejected


# ─── Step 4 — Trusted Layer (Upsert) ──────────────────────────────────────
def step_trusted(cur, stg, run_id, business_date, file_name, file_checksum):
    """Upsert into trusted table keyed on (entity_id, snapshot_date)."""
    ts = now_utc()

    def v(val):
        return val if val != "" else None

    def to_date(val):
        try:
            return pd.to_datetime(val).date() if val else None
        except Exception:
            return None

    def to_num(val, cast=float):
        try:
            return cast(val) if val != "" else None
        except Exception:
            return None

    def to_bool(val):
        return str(val).upper() in ("TRUE", "1", "YES", "Y")

    rows = []
    for _, r in stg.iterrows():
        rows.append((
            r["entity_id"],
            to_date(r["snapshot_date"]),
            r["student_no"], r["student_name"], r["level"],
            r["faculty_or_school"], r["program_id"], r["program_name"],
            r["discipline_cluster"], r["campus"],
            to_num(r["year_of_study"], int),
            r["admission_channel"], r["status"],
            to_num(r["gpa"]),
            to_num(r["credit_earned"], int),
            to_bool(r["is_international"]),
            v(r["career_interest"]),
            to_num(r["expected_salary_thb"]),
            v(r["internship_interest"]),
            v(r["behavior_profile"]), v(r["teamwork_style"]),
            v(r["learning_preference"]),
            to_num(r["source_row_no"], int),
            v(r["source_url"]),
            file_name, file_checksum, business_date, run_id, ts, "trusted",
        ))

    sql = """
        INSERT INTO workshop.trusted_student_snapshot
            (entity_id, snapshot_date,
             student_no, student_name, level,
             faculty_or_school, program_id, program_name,
             discipline_cluster, campus, year_of_study,
             admission_channel, status, gpa, credit_earned, is_international,
             career_interest, expected_salary_thb, internship_interest,
             behavior_profile, teamwork_style, learning_preference,
             source_row_no, source_url,
             source_file_name, input_file_checksum,
             business_date, run_id, load_timestamp, record_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (entity_id, snapshot_date)
        DO UPDATE SET
            student_name        = EXCLUDED.student_name,
            status              = EXCLUDED.status,
            gpa                 = EXCLUDED.gpa,
            credit_earned       = EXCLUDED.credit_earned,
            career_interest     = EXCLUDED.career_interest,
            expected_salary_thb = EXCLUDED.expected_salary_thb,
            run_id              = EXCLUDED.run_id,
            load_timestamp      = EXCLUDED.load_timestamp,
            record_status       = EXCLUDED.record_status
    """
    psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"  [TRUSTED] {len(rows)} rows upserted (ON CONFLICT UPDATE)")


# ─── Step 5 — RAG Corpus ──────────────────────────────────────────────────
def step_rag(cur, stg, run_id, business_date):
    """Populate rag_excel_document_chunk; upsert by chunk_id."""
    ts = now_utc()
    rag = stg[stg["rag_document_text"].str.strip() != ""]
    rows = []
    for _, r in rag.iterrows():
        chunk_id = f"chunk_{r['source_row_no']}_{business_date}"
        rows.append((
            chunk_id,
            int(r["source_row_no"]) if r["source_row_no"] else None,
            r["entity_id"], r["student_no"],
            r["rag_document_title"], r["rag_document_text"],
            r["rag_keywords"], r["rag_sample_question"],
            r["rag_expected_answer_hint"],
            r["source_url"],
            business_date, run_id, ts,
        ))

    sql = """
        INSERT INTO workshop.rag_excel_document_chunk
            (chunk_id, source_row_no, entity_id, student_no,
             rag_document_title, rag_document_text, rag_keywords,
             rag_sample_question, rag_expected_answer_hint,
             source_url, business_date, run_id, load_timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (chunk_id) DO UPDATE SET
            rag_document_text        = EXCLUDED.rag_document_text,
            rag_keywords             = EXCLUDED.rag_keywords,
            run_id                   = EXCLUDED.run_id,
            load_timestamp           = EXCLUDED.load_timestamp
    """
    psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"  [RAG]  {len(rows)} chunks upserted")


# ─── Step 6 — Analytics Summary ───────────────────────────────────────────
def step_analytics(cur, business_date, run_id):
    """Compute and store analytics summary from trusted table."""
    cur.execute("""
        INSERT INTO workshop.analytics_student_summary
            (business_date, run_id, generated_at,
             total_students, active_count, leave_count,
             exchange_count, withdrawn_count, graduated_count,
             rangsit_count, tha_phra_chan_count,
             undergraduate_count, postgraduate_count, international_count,
             gpa_min, gpa_max, gpa_avg, gpa_sum,
             credit_min, credit_max, credit_avg, credit_sum,
             salary_min, salary_max, salary_avg)
        SELECT
            %s, %s, NOW(),
            COUNT(*),
            COUNT(*) FILTER (WHERE status = 'Active'),
            COUNT(*) FILTER (WHERE status = 'Leave'),
            COUNT(*) FILTER (WHERE status = 'Exchange'),
            COUNT(*) FILTER (WHERE status = 'Withdrawn'),
            COUNT(*) FILTER (WHERE status = 'Graduated'),
            COUNT(*) FILTER (WHERE campus = 'Rangsit'),
            COUNT(*) FILTER (WHERE campus = 'Tha Phra Chan'),
            COUNT(*) FILTER (WHERE level = 'Undergraduate'),
            COUNT(*) FILTER (WHERE level = 'Postgraduate'),
            COUNT(*) FILTER (WHERE is_international = TRUE),
            MIN(gpa), MAX(gpa), ROUND(AVG(gpa),2), SUM(gpa),
            MIN(credit_earned), MAX(credit_earned),
            ROUND(AVG(credit_earned),1), SUM(credit_earned),
            MIN(expected_salary_thb), MAX(expected_salary_thb),
            ROUND(AVG(expected_salary_thb),0)
        FROM workshop.trusted_student_snapshot
    """, (business_date, run_id))
    print(f"  [ANALYTICS] Summary row inserted")


# ─── Step 7 — Quality Check ───────────────────────────────────────────────
def step_quality(cur, df_source, business_date, run_id):
    """Compare source Excel vs trusted table on key metrics."""
    src = df_source[df_source["record_type"] == "student"].copy()
    src["gpa"] = pd.to_numeric(src["gpa"], errors="coerce")
    src["credit_earned"] = pd.to_numeric(src["credit_earned"], errors="coerce")
    src["expected_salary_thb"] = pd.to_numeric(src["expected_salary_thb"], errors="coerce")

    source_vals = {
        "row_count":   len(src),
        "gpa_sum":     round(float(src["gpa"].sum()), 2),
        "gpa_min":     round(float(src["gpa"].min()), 2),
        "gpa_max":     round(float(src["gpa"].max()), 2),
        "credit_sum":  int(src["credit_earned"].sum()),
        "salary_min":  int(src["expected_salary_thb"].min()),
        "salary_max":  int(src["expected_salary_thb"].max()),
    }

    cur.execute("""
        SELECT
            COUNT(*),
            ROUND(SUM(gpa)::numeric, 2),
            ROUND(MIN(gpa)::numeric, 2),
            ROUND(MAX(gpa)::numeric, 2),
            SUM(credit_earned),
            MIN(expected_salary_thb),
            MAX(expected_salary_thb)
        FROM workshop.trusted_student_snapshot
    """)
    row = cur.fetchone()
    target_vals = {
        "row_count":  int(row[0]),
        "gpa_sum":    float(row[1]),
        "gpa_min":    float(row[2]),
        "gpa_max":    float(row[3]),
        "credit_sum": int(row[4]),
        "salary_min": int(row[5]),
        "salary_max": int(row[6]),
    }

    print("\n  [QUALITY SUMMARY]")
    print(f"  {'Check':<20} {'Source':>12} {'Target':>12} {'Pass?':>6}")
    print("  " + "-" * 54)

    results = []
    all_pass = True
    for key in source_vals:
        s, t = source_vals[key], target_vals[key]
        ok = s == t
        if not ok:
            all_pass = False
        print(f"  {key:<20} {str(s):>12} {str(t):>12} {'✓' if ok else '✗ FAIL':>6}")
        results.append({"check": key, "source": s, "target": t, "pass": bool(ok)})

    audit_path = os.path.join(
        OUTPUT_DIR, "audit", f"quality_summary_{business_date}.json"
    )
    with open(audit_path, "w") as f:
        json.dump(
            {"business_date": str(business_date), "run_id": run_id,
             "all_pass": all_pass, "checks": results},
            f, indent=2
        )
    print(f"\n  Quality summary → {audit_path}")
    return all_pass


# ─── Step 8 — Export CSVs ─────────────────────────────────────────────────
def step_export(cur, business_date):
    """Export trusted and RAG tables to CSV for JamAI / reporting."""
    for table, subdir, filename in [
        ("workshop.trusted_student_snapshot", "trusted", "trusted_student_snapshot.csv"),
        ("workshop.rag_excel_document_chunk", "rag",     "rag_excel_document_chunk.csv"),
    ]:
        cur.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        path = os.path.join(OUTPUT_DIR, subdir, filename)
        pd.DataFrame(rows, columns=cols).to_csv(path, index=False)
        print(f"  [EXPORT] {len(rows)} rows → {path}")


# ─── Main ──────────────────────────────────────────────────────────────────
def run(business_date: str, run_id: str, input_file: str, sheet_name: str):
    print(f"\n{'='*60}")
    print(f"  Thammasat Workshop — PostgreSQL Batch Pipeline")
    print(f"  business_date : {business_date}")
    print(f"  run_id        : {run_id}")
    print(f"  input_file    : {input_file}")
    print(f"  sheet_name    : {sheet_name}")
    print(f"{'='*60}\n")

    start_time = now_utc()
    file_checksum = checksum(input_file)
    file_name = os.path.basename(input_file)
    print(f"  MD5 checksum  : {file_checksum}\n")

    source_count = loaded_count = rejected_count = 0
    error_message = ""
    status = "failed"

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Warn if same file+date already succeeded
                cur.execute("""
                    SELECT run_id FROM workshop.batch_audit
                    WHERE input_file_checksum = %s
                      AND business_date = %s
                      AND status = 'success'
                    LIMIT 1
                """, (file_checksum, business_date))
                prior = cur.fetchone()
                if prior:
                    print(f"  ⚠ Already processed (run={prior[0]}). Re-running (idempotent).\n")

                print("STEP 1 — Read Excel")
                df = step_read(input_file, sheet_name)
                source_count = len(df)

                print("\nSTEP 2 — Raw layer")
                step_raw(cur, df, run_id, business_date, file_name, file_checksum)

                print("\nSTEP 3 — Staging layer")
                stg, rejected_count = step_staging(
                    cur, df, run_id, business_date, file_name, file_checksum
                )

                print("\nSTEP 4 — Trusted layer (upsert)")
                step_trusted(cur, stg, run_id, business_date, file_name, file_checksum)

                print("\nSTEP 5 — RAG corpus")
                step_rag(cur, stg, run_id, business_date)

                print("\nSTEP 6 — Analytics summary")
                step_analytics(cur, business_date, run_id)

                print("\nSTEP 7 — Quality checks")
                all_pass = step_quality(cur, df, business_date, run_id)

                print("\nSTEP 8 — Export CSVs")
                step_export(cur, business_date)

                loaded_count = len(stg)

                if not all_pass:
                    raise ValueError("Quality check failed — source vs target mismatch")

                status = "success"
                log_run(cur, run_id, business_date, start_time, now_utc(),
                        status, file_name, sheet_name, file_checksum,
                        source_count, loaded_count, rejected_count)

        print(f"\n{'='*60}")
        print(f"  ✓ Pipeline completed successfully")
        print(f"  Source: {source_count}  Loaded: {loaded_count}  Rejected: {rejected_count}")
        print(f"{'='*60}\n")

    except Exception as e:
        error_message = str(e)
        status = "failed"
        print(f"\n  ✗ ERROR: {error_message}")
        try:
            with conn.cursor() as cur:
                log_run(cur, run_id, business_date, start_time, now_utc(),
                        status, file_name, sheet_name, file_checksum,
                        source_count, loaded_count, rejected_count, error_message)
            conn.commit()
        except Exception:
            pass
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thammasat Workshop Batch Pipeline")
    parser.add_argument("--business_date", required=True, help="e.g. 2026-06-28")
    parser.add_argument("--run_id",        required=True, help="e.g. RUN001")
    parser.add_argument("--input_file",    required=True, help="path to .xlsx file")
    parser.add_argument("--sheet_name",    default="workshop_data")
    args = parser.parse_args()
    run(args.business_date, args.run_id, args.input_file, args.sheet_name)
