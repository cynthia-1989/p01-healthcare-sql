# ================================================================
# src/query_runner.py
# P01 Healthcare SQL — Query Runner
# ================================================================

import sys
import pathlib
import time
import pandas as pd

_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent

if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import engine, DB_AVAILABLE, SQL_DIR, INDUSTRY, logger


class SQLQueryRunner:
    """
    Executes SQL queries against the healthcare PostgreSQL database.
    Returns SQL results as pandas DataFrames.
    """

    def __init__(self):
        self.industry = INDUSTRY
        self.history = []
        logger.info(f"SQLQueryRunner ready — schema: {self.industry}, db_available: {DB_AVAILABLE}")

    def run(self, sql: str, params: dict = None) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a DataFrame.
        """
        if not DB_AVAILABLE or engine is None:
            logger.error("[SQL] Database not available. Check DB_URL in .env.")
            return pd.DataFrame()

        sql = sql.replace("{industry}", self.industry)
        start_time = time.time()

        try:
            df = pd.read_sql(sql, engine, params=params)

            duration_ms = round((time.time() - start_time) * 1000, 1)

            self.history.append({
                "sql_preview": sql[:100].strip(),
                "rows": len(df),
                "cols": len(df.columns),
                "duration_ms": duration_ms,
                "status": "success",
            })

            logger.info(
                f"[SQL] Query complete — {len(df):,} rows × {len(df.columns)} columns | {duration_ms} ms"
            )

            return df

        except Exception as e:
            duration_ms = round((time.time() - start_time) * 1000, 1)

            self.history.append({
                "sql_preview": sql[:100].strip(),
                "rows": 0,
                "cols": 0,
                "duration_ms": duration_ms,
                "status": f"error: {str(e)[:100]}",
            })

            logger.error(f"[SQL] Query failed: {e}")
            return pd.DataFrame()

    def run_file(self, filename: str) -> pd.DataFrame:
        """
        Load a SQL file from the sql/ folder and execute it.
        """
        sql_path = SQL_DIR / filename

        if not sql_path.exists():
            logger.error(f"[SQL] File not found: {sql_path}")
            return pd.DataFrame()

        logger.info(f"[SQL] Loading SQL file: {filename}")

        sql_text = sql_path.read_text(encoding="utf-8")
        return self.run(sql_text)

    def demo_basics(self) -> None:
        """
        Run basic healthcare table checks.
        """
        demos = [
            (
                "Sample patients",
                f"""
                SELECT
                    patient_id,
                    first_name,
                    last_name,
                    gender,
                    city,
                    insurance_type
                FROM {self.industry}.patients
                LIMIT 10;
                """
            ),
            (
                "Sample appointments",
                f"""
                SELECT
                    appointment_id,
                    patient_id,
                    doctor_id,
                    appointment_date,
                    status,
                    visit_type,
                    fee
                FROM {self.industry}.appointments
                LIMIT 10;
                """
            ),
            (
                "Sample billing records",
                f"""
                SELECT
                    bill_id,
                    patient_id,
                    appointment_id,
                    amount_charged,
                    insurance_paid,
                    patient_paid,
                    payment_status
                FROM {self.industry}.billing
                LIMIT 10;
                """
            ),
        ]

        for title, sql in demos:
            print(f"\n── {title}:")
            df = self.run(sql)
            if not df.empty:
                print(df.to_string(index=False))

    def demo_aggregation(self) -> None:
        """
        Run healthcare billing aggregation.
        """
        sql = f"""
        SELECT
            payment_status,
            COUNT(bill_id) AS total_bills,
            SUM(amount_charged) AS total_amount_charged,
            SUM(insurance_paid) AS total_insurance_paid,
            SUM(patient_paid) AS total_patient_paid,
            AVG(amount_charged) AS average_bill_amount
        FROM {self.industry}.billing
        GROUP BY payment_status
        ORDER BY total_amount_charged DESC;
        """

        print("\n── Billing Aggregation by Payment Status:")
        df = self.run(sql)
        if not df.empty:
            print(df.to_string(index=False))

    def demo_joins(self) -> None:
        """
        Run joined healthcare extract preview.
        """
        sql = f"""
        SELECT
            p.patient_id,
            p.first_name AS patient_first_name,
            p.last_name AS patient_last_name,
            p.city,
            p.insurance_type,

            a.appointment_id,
            a.appointment_date,
            a.status AS appointment_status,
            a.visit_type,
            a.fee,

            b.bill_id,
            b.amount_charged,
            b.insurance_paid,
            b.patient_paid,
            b.payment_status,

            doc.first_name AS doctor_first_name,
            doc.last_name AS doctor_last_name,
            doc.specialization,

            d.dept_name

        FROM {self.industry}.patients p
        JOIN {self.industry}.appointments a
            ON p.patient_id = a.patient_id
        JOIN {self.industry}.billing b
            ON a.appointment_id = b.appointment_id
        JOIN {self.industry}.doctors doc
            ON a.doctor_id = doc.doctor_id
        JOIN {self.industry}.departments d
            ON doc.dept_id = d.dept_id
        LIMIT 10;
        """

        print("\n── Healthcare Joined Extract Preview:")
        df = self.run(sql)
        if not df.empty:
            print(df.to_string(index=False))

    def __str__(self) -> str:
        return f"SQLQueryRunner(industry={self.industry!r}, queries_run={len(self.history)})"

    def __repr__(self) -> str:
        return f"SQLQueryRunner(industry={self.industry!r})"