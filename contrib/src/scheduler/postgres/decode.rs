use harbor::scheduler;
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use super::error::sqlx as sql_error;

pub(super) fn string(row: &PgRow, column: &str) -> Result<String, scheduler::Error> {
    row.try_get::<String, _>(column).map_err(sql_error)
}
