"""PostgreSQL run state. A session lock gives one service ownership and crash recovery."""

from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class StateError(Exception):
    pass


class ProcessingState:
    def __init__(self, **connection_options):
        self.options = connection_options
        self.connection = None
        self._staging_ready = False

    def open(self):
        self._staging_ready = False
        try:
            self.connection = psycopg.connect(
                **self.options,
                autocommit=True,
                connect_timeout=10,
                row_factory=dict_row,
                options="-c statement_timeout=10000",
            )
            locked = self.connection.execute(
                "SELECT pg_try_advisory_lock(1953067887, 1) AS locked"
            ).fetchone()["locked"]
            if not locked:
                raise StateError(
                    "Another aggregation service owns processing state"
                )
            self.connection.execute(
                """
                UPDATE aggregation.processing_state SET status='interrupted',
                    last_finished_at=clock_timestamp(), consecutive_failures=consecutive_failures+1,
                    last_error=%s, updated_at=clock_timestamp()
                WHERE status='running'
            """,
                (
                    Jsonb(
                        {
                            "step": "service",
                            "message": "Previous service stopped before completing the run",
                        }
                    ),
                ),
            )
        except psycopg.Error:
            self.close()
            raise StateError(
                "Processing-state storage unavailable; check database settings and apply database/istsos_aggregation.sql"
            ) from None
        except StateError:
            self.close()
            raise

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def execute(self, sql, parameters):
        try:
            return self.connection.execute(sql, parameters)
        except psycopg.Error as error:
            # Losing this connection also loses ownership: exit and recover at
            # restart instead of continuing under a potentially stale lease.
            raise StateError(
                "Cannot update processing state; stopping service for recovery: "
                f"{type(error).__name__}: {str(error).strip()}"
            ) from None

    def start(self, aggregation_id):
        run_id = uuid4()
        row = self.execute(
            """
            UPDATE aggregation.processing_state s SET status='running',
                run_id=%s, last_started_at=clock_timestamp(), last_finished_at=NULL,
                last_error=NULL, updated_at=clock_timestamp()
            FROM aggregation.datastream_configuration c
            WHERE s.aggregation_id=%s AND c.id=s.aggregation_id AND c.enabled
            RETURNING s.aggregation_id
        """,
            (run_id, aggregation_id),
        ).fetchone()
        return run_id if row else None

    def finish(self, aggregation_id, run_id, status, error=None):
        if status not in {"success", "failed", "interrupted"}:
            raise ValueError("Invalid terminal processing status")
        self.execute(
            """
            UPDATE aggregation.processing_state SET status=%s,
                last_finished_at=clock_timestamp(), last_error=%s,
                last_successful_at=CASE WHEN %s='success' THEN clock_timestamp() ELSE last_successful_at END,
                consecutive_failures=CASE WHEN %s='success' THEN 0 ELSE consecutive_failures+1 END,
                updated_at=clock_timestamp()
            WHERE aggregation_id=%s AND run_id=%s AND status='running'
        """,
            (
                status,
                Jsonb(error) if error else None,
                status,
                status,
                aggregation_id,
                run_id,
            ),
        )

    def discovery_failed(self, message):
        """Mark every enabled configuration failed: the list could not be read."""
        self.execute(
            """
            UPDATE aggregation.processing_state s SET status='failed', run_id=%s,
                last_started_at=clock_timestamp(), last_finished_at=clock_timestamp(),
                last_error=%s, consecutive_failures=consecutive_failures+1,
                updated_at=clock_timestamp()
            FROM aggregation.datastream_configuration c
            WHERE c.id=s.aggregation_id AND c.enabled
        """,
            (
                uuid4(),
                Jsonb({"step": "configuration_read", "message": message}),
            ),
        )

    def checkpoint(self, aggregation_id):
        row = self.execute(
            "SELECT checkpoint FROM aggregation.processing_state WHERE aggregation_id=%s",
            (aggregation_id,),
        ).fetchone()
        return row["checkpoint"] if row else None

    def daily_rows(self, datastream_id, start, end):
        """Saved daily buckets starting in [start, end), for the rollup."""
        return self.execute(
            """
            SELECT bucket_start, count_total, count_accepted, count_numeric,
                sum_value, min_value, max_value, expected_count, result_quality
            FROM aggregation.observations_1day
            WHERE datastream_id=%s AND bucket_start >= %s AND bucket_start < %s
            ORDER BY bucket_start
        """,
            (datastream_id, start, end),
        ).fetchall()

    def source_context(self):
        versioned = self.execute(
            """
            SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='sensorthings' AND table_name='Observation'
                AND column_name='systemTimeValidity') AS enabled
        """,
            (),
        ).fetchone()["enabled"]
        expression = (
            "aggregation.safe_change_watermark()"
            if versioned
            else "date_trunc('second',clock_timestamp())"
        )
        cutoff = self.execute(f"SELECT {expression} AS cutoff", ()).fetchone()[
            "cutoff"
        ]
        return cutoff, versioned

    def save_buckets(self, aggregation_id, run_id, table, rows, checkpoint):
        """COPY a result batch, merge with one upsert, and commit its checkpoint."""
        from psycopg import sql

        from aggregation.calculation import TABLES, CalculationError

        if table not in TABLES.values():
            raise ValueError("Unknown aggregation result table")
        if not rows:
            return
        columns = list(rows[0])
        revision = rows[0]["configuration_updated_at"]
        if any(
            set(row) != set(columns)
            or row["configuration_updated_at"] != revision
            for row in rows
        ):
            raise CalculationError(
                "Bulk results must have matching columns and configuration revision"
            )
        try:
            with self.connection.transaction():
                current = self.connection.execute(
                    """
                    SELECT s.aggregation_id, c.datastream_id FROM aggregation.processing_state s
                    JOIN aggregation.datastream_configuration c ON c.id=s.aggregation_id
                    WHERE s.aggregation_id=%s AND s.run_id=%s AND s.status='running'
                        AND c.enabled AND c.updated_at=%s::timestamptz
                    FOR UPDATE OF s, c
                    """,
                    (aggregation_id, run_id, revision),
                ).fetchone()
                if current is None:
                    raise CalculationError(
                        "Configuration changed or run ownership was lost during calculation"
                    )
                if any(
                    row["datastream_id"] != current["datastream_id"]
                    for row in rows
                ):
                    raise CalculationError(
                        "Bulk results reference a different Datastream"
                    )
                if not self._staging_ready:
                    # All six result tables share this schema. The staging
                    # table is session-local and automatically emptied on commit.
                    self.connection.execute("""
                        CREATE TEMP TABLE aggregation_result_buffer
                        (LIKE aggregation.observations_10min INCLUDING DEFAULTS)
                        ON COMMIT DELETE ROWS
                    """)
                identifiers = sql.SQL(",").join(map(sql.Identifier, columns))
                with (
                    self.connection.cursor() as cursor,
                    cursor.copy(
                        sql.SQL(
                            "COPY pg_temp.aggregation_result_buffer ({}) FROM STDIN"
                        ).format(identifiers)
                    ) as copy,
                ):
                    for row in rows:
                        copy.write_row(
                            tuple(row[column] for column in columns)
                        )
                assignments = sql.SQL(", ").join(
                    sql.SQL("{}=EXCLUDED.{}").format(
                        sql.Identifier(column), sql.Identifier(column)
                    )
                    for column in columns
                    if column not in ("datastream_id", "bucket_start")
                )
                self.connection.execute(
                    sql.SQL("""
                    INSERT INTO aggregation.{} ({}) SELECT {} FROM pg_temp.aggregation_result_buffer WHERE true
                    ON CONFLICT (datastream_id,bucket_start) DO UPDATE SET {}, calculated_at=clock_timestamp()
                """).format(
                        sql.Identifier(table),
                        identifiers,
                        identifiers,
                        assignments,
                    )
                )
                self.connection.execute(
                    "UPDATE aggregation.processing_state SET checkpoint=%s, updated_at=clock_timestamp() WHERE aggregation_id=%s AND run_id=%s",
                    (Jsonb(checkpoint), aggregation_id, run_id),
                )
            self._staging_ready = True
        except (psycopg.OperationalError, psycopg.InterfaceError):
            raise StateError(
                "Bulk result transaction failed; restart required to recover state ownership"
            ) from None
        except psycopg.Error:
            raise CalculationError(
                "Bulk result transaction rejected; previous results and checkpoint preserved"
            ) from None

    def complete(self, aggregation_id, run_id, checkpoint):
        with self.connection.transaction():
            self.execute(
                """UPDATE aggregation.processing_state SET checkpoint=%s
                WHERE aggregation_id=%s AND run_id=%s AND status='running'""",
                (Jsonb(checkpoint), aggregation_id, run_id),
            )
            self.finish(aggregation_id, run_id, "success")
