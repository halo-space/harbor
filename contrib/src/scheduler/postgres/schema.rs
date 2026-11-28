use std::collections::{HashMap, HashSet};

use harbor::scheduler;
use sqlx::{PgPool, Postgres, Row as _};

use super::error::sqlx as sql_error;

const TABLES: &[&str] = &[
    "traces",
    "requests",
    "queues",
    "failed_workers",
    "completions",
    "trace_stats",
    "workers",
];

const COLUMNS: &[(&str, &str, &str, bool)] = &[
    ("traces", "id", "character varying", false),
    ("traces", "task_id", "character varying", false),
    ("traces", "snapshot", "jsonb", false),
    (
        "traces",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "traces",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
    ("requests", "id", "character varying", false),
    ("requests", "task_id", "character varying", false),
    ("requests", "trace_id", "character varying", false),
    ("requests", "node", "character varying", false),
    ("requests", "mode", "character varying", false),
    ("requests", "priority", "integer", false),
    ("requests", "snapshot", "jsonb", false),
    ("requests", "snapshot_hash", "bytea", false),
    ("requests", "state", "smallint", false),
    ("requests", "version", "bigint", false),
    ("requests", "next_time", "bigint", false),
    ("requests", "leased_by", "character varying", false),
    ("requests", "lease_time", "bigint", false),
    ("requests", "retry_count", "integer", false),
    ("requests", "max_retry_count", "integer", false),
    ("requests", "ack_version", "bigint", true),
    (
        "requests",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "requests",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
    ("queues", "sequence", "bigint", false),
    ("queues", "request_id", "character varying", false),
    ("queues", "mode", "character varying", false),
    ("queues", "priority", "integer", false),
    ("queues", "next_time", "bigint", false),
    (
        "queues",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "queues",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
    ("failed_workers", "request_id", "character varying", false),
    ("failed_workers", "worker_id", "character varying", false),
    ("failed_workers", "position", "integer", false),
    (
        "failed_workers",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "failed_workers",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
    ("completions", "request_id", "character varying", false),
    ("completions", "version", "bigint", false),
    ("completions", "task_id", "character varying", false),
    ("completions", "trace_id", "character varying", false),
    ("completions", "node", "character varying", false),
    ("completions", "worker_id", "character varying", false),
    ("completions", "state", "smallint", false),
    ("completions", "error", "text", true),
    ("completions", "start_time", "bigint", true),
    ("completions", "end_time", "bigint", true),
    (
        "completions",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "completions",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
    ("trace_stats", "trace_id", "character varying", false),
    ("trace_stats", "name", "character varying", false),
    ("trace_stats", "total", "bigint", false),
    ("trace_stats", "done", "bigint", false),
    ("trace_stats", "filter", "bigint", false),
    ("trace_stats", "dedup", "bigint", false),
    ("trace_stats", "validate", "bigint", false),
    ("trace_stats", "download", "bigint", false),
    (
        "trace_stats",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "trace_stats",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
    ("workers", "worker_id", "character varying", false),
    ("workers", "host", "character varying", false),
    ("workers", "ip", "character varying", true),
    ("workers", "version", "character varying", false),
    ("workers", "modes", "jsonb", false),
    ("workers", "concurrency", "integer", false),
    ("workers", "claim_limit", "integer", false),
    ("workers", "heartbeat_timeout", "bigint", false),
    ("workers", "last_heartbeat", "bigint", false),
    ("workers", "token", "character varying", false),
    ("workers", "offline_time", "bigint", true),
    (
        "workers",
        "created_time",
        "timestamp without time zone",
        false,
    ),
    (
        "workers",
        "updated_time",
        "timestamp without time zone",
        false,
    ),
];

const UNIQUE_KEYS: &[(&str, &[&str])] = &[
    ("traces_pkey", &["id"]),
    ("requests_pkey", &["id"]),
    ("queues_pkey", &["sequence"]),
    ("uq_queues_request", &["request_id"]),
    ("failed_workers_pkey", &["request_id", "worker_id"]),
    ("uq_failed_workers_position", &["request_id", "position"]),
    ("completions_pkey", &["request_id", "version"]),
    ("trace_stats_pkey", &["trace_id", "name"]),
    ("workers_pkey", &["worker_id"]),
];

pub(super) async fn validate(pool: &PgPool, database: &str) -> Result<(), scheduler::Error> {
    let rows = sqlx::query::<Postgres>(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'",
    )
    .fetch_all(pool)
    .await
    .map_err(sql_error)?;
    let tables = rows
        .into_iter()
        .map(|row| row.try_get::<String, _>("table_name").map_err(sql_error))
        .collect::<Result<HashSet<_>, _>>()?;
    let missing = TABLES
        .iter()
        .copied()
        .filter(|table| !tables.contains(*table))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(scheduler::Error::Message(format!(
            "PostgreSQL Scheduler database {database} is missing required tables: {}",
            missing.join(", ")
        )));
    }

    let rows = sqlx::query::<Postgres>(
        "SELECT table_name, column_name, data_type, is_nullable \
         FROM information_schema.columns WHERE table_schema = current_schema()",
    )
    .fetch_all(pool)
    .await
    .map_err(sql_error)?;
    let mut columns = HashMap::new();
    for row in rows {
        let table = row.try_get::<String, _>("table_name").map_err(sql_error)?;
        let column = row.try_get::<String, _>("column_name").map_err(sql_error)?;
        let kind = row.try_get::<String, _>("data_type").map_err(sql_error)?;
        let nullable = row.try_get::<String, _>("is_nullable").map_err(sql_error)?;
        columns.insert((table, column), (kind, nullable == "YES"));
    }
    for (table, name, expected_kind, optional) in COLUMNS {
        let Some((kind, nullable)) = columns.get(&(table.to_string(), name.to_string())) else {
            return Err(scheduler::Error::Message(format!(
                "PostgreSQL Scheduler database {database} is missing required column {table}.{name}"
            )));
        };
        if !kind.eq_ignore_ascii_case(expected_kind) || *nullable != *optional {
            return Err(scheduler::Error::Message(format!(
                "PostgreSQL Scheduler column {table}.{name} has incompatible definition"
            )));
        }
    }

    validate_unique_keys(pool).await
}

async fn validate_unique_keys(pool: &PgPool) -> Result<(), scheduler::Error> {
    let rows = sqlx::query::<Postgres>(
        "SELECT tc.constraint_name, kcu.column_name, kcu.ordinal_position \
         FROM information_schema.table_constraints tc \
         JOIN information_schema.key_column_usage kcu \
           ON kcu.constraint_name = tc.constraint_name \
          AND kcu.table_schema = tc.table_schema \
          AND kcu.table_name = tc.table_name \
         WHERE tc.table_schema = current_schema() \
           AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') \
         ORDER BY tc.constraint_name, kcu.ordinal_position",
    )
    .fetch_all(pool)
    .await
    .map_err(sql_error)?;
    let mut actual = HashMap::<String, Vec<String>>::new();
    for row in rows {
        let name = row
            .try_get::<String, _>("constraint_name")
            .map_err(sql_error)?;
        let column = row.try_get::<String, _>("column_name").map_err(sql_error)?;
        actual.entry(name).or_default().push(column);
    }
    for (name, expected) in UNIQUE_KEYS {
        let matches = actual.get(*name).is_some_and(|columns| {
            columns
                .iter()
                .map(String::as_str)
                .eq(expected.iter().copied())
        });
        if !matches {
            return Err(scheduler::Error::Message(format!(
                "PostgreSQL Scheduler is missing required unique key {name}"
            )));
        }
    }
    Ok(())
}
