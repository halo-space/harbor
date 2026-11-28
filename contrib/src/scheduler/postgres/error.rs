use harbor::scheduler;

pub(super) fn sqlx(error: sqlx::Error) -> scheduler::Error {
    if transient(&error) {
        scheduler::Error::Unavailable(format!("PostgreSQL Scheduler operation failed: {error}"))
    } else {
        scheduler::Error::Message(format!("PostgreSQL Scheduler operation failed: {error}"))
    }
}

pub(super) fn message(error: impl std::fmt::Display) -> scheduler::Error {
    scheduler::Error::Message(error.to_string())
}

fn transient(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(_) => database_code(error).is_some_and(|code| transient_code(&code)),
        _ => false,
    }
}

fn transient_code(code: &str) -> bool {
    matches!(
        code,
        // Serialization/deadlock, connection, and server availability errors.
        "40001" | "40P01" | "53300" | "57P01" | "08001" | "08003" | "08006"
    )
}

pub(super) fn database_code(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(error) => error.code().map(|code| code.into_owned()),
        _ => None,
    }
}
