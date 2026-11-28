use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};

const DEFAULT_URL: &str = "postgres://postgres:123456@127.0.0.1:5432/postgres";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SCHEMA: &str = include_str!("../../sql/postgres/schema.sql");

pub(super) struct Server {
    admin_url: url::Url,
    admin: PgPool,
}

pub(super) struct Database {
    name: Option<String>,
    url: String,
    pool: Option<PgPool>,
    admin: PgPool,
}

impl Server {
    pub(super) async fn connect() -> Option<Self> {
        let (url, required) = match std::env::var("CRAWLER_POSTGRES_URL") {
            Ok(url) => (url, true),
            Err(std::env::VarError::NotPresent) => (DEFAULT_URL.to_string(), false),
            Err(error) => panic!("invalid CRAWLER_POSTGRES_URL: {error}"),
        };
        let admin_url = url::Url::parse(&url)
            .unwrap_or_else(|error| panic!("invalid CRAWLER_POSTGRES_URL {url:?}: {error}"));
        if !matches!(admin_url.scheme(), "postgres" | "postgresql") {
            panic!("CRAWLER_POSTGRES_URL must use the postgres or postgresql scheme");
        }
        let admin = match tokio::time::timeout(
            CONNECT_TIMEOUT,
            PgPoolOptions::new()
                .max_connections(4)
                .acquire_timeout(CONNECT_TIMEOUT)
                .connect(admin_url.as_str()),
        )
        .await
        {
            Ok(Ok(pool)) => pool,
            Ok(Err(error)) => return unavailable(&url, &error, required),
            Err(_) => return unavailable_timeout(&url, required),
        };
        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            sqlx::query::<Postgres>("SELECT 1").execute(&admin),
        )
        .await
        {
            Ok(Ok(_)) => Some(Self { admin_url, admin }),
            Ok(Err(error)) => unavailable(&url, &error, required),
            Err(_) => unavailable_timeout(&url, required),
        }
    }

    pub(super) async fn database(&self, label: &str) -> Database {
        self.create_database(label, true).await
    }

    #[allow(dead_code)]
    pub(super) async fn empty_database(&self, label: &str) -> Database {
        self.create_database(label, false).await
    }

    async fn create_database(&self, label: &str, install_schema: bool) -> Database {
        let name = database_name(label);
        sqlx::query::<Postgres>(sqlx::AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
            .execute(&self.admin)
            .await
            .unwrap_or_else(|error| {
                panic!("failed to create PostgreSQL test database {name}: {error}")
            });

        let mut database_url = self.admin_url.clone();
        database_url.set_path(&format!("/{name}"));
        let url = database_url.to_string();
        let pool = match PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(CONNECT_TIMEOUT)
            .connect(&url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => {
                drop_database(&self.admin, &name).await;
                panic!("failed to connect to PostgreSQL test database {name}: {error}");
            }
        };
        if install_schema && let Err(error) = sqlx::raw_sql(SCHEMA).execute(&pool).await {
            pool.close().await;
            drop_database(&self.admin, &name).await;
            panic!("failed to install PostgreSQL Scheduler schema in {name}: {error}");
        }

        Database {
            name: Some(name),
            url,
            pool: Some(pool),
            admin: self.admin.clone(),
        }
    }
}

impl Database {
    pub(super) fn url(&self) -> &str {
        &self.url
    }

    pub(super) async fn remove(mut self) {
        let name = self
            .name
            .take()
            .expect("PostgreSQL test database was already removed");
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        drop_database(&self.admin, &name).await;
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let Some(name) = self.name.take() else {
            return;
        };
        let pool = self.pool.take();
        let admin = self.admin.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(pool) = pool {
                    pool.close().await;
                }
                let _ = drop_database(&admin, &name).await;
            });
        }
    }
}

fn database_name(label: &str) -> String {
    let mut label = label
        .chars()
        .filter_map(|value| {
            if value.is_ascii_alphanumeric() {
                Some(value.to_ascii_lowercase())
            } else if value == '-' || value == '_' {
                Some('_')
            } else {
                None
            }
        })
        .take(16)
        .collect::<String>();
    if label.is_empty() {
        label.push_str("schema");
    }
    format!("harbor_test_{label}_{}", uuid::Uuid::now_v7().simple())
}

async fn drop_database(admin: &PgPool, name: &str) {
    sqlx::query::<Postgres>(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"
    )))
    .execute(admin)
    .await
    .unwrap_or_else(|error| panic!("failed to drop PostgreSQL test database {name}: {error}"));
}

fn unavailable(url: &str, error: &sqlx::Error, required: bool) -> Option<Server> {
    if required {
        panic!("configured PostgreSQL at {url} is unavailable: {error}");
    }
    eprintln!(
        "skipping PostgreSQL Scheduler integration test: PostgreSQL at {url} is unavailable: {error}"
    );
    None
}

fn unavailable_timeout(url: &str, required: bool) -> Option<Server> {
    if required {
        panic!("configured PostgreSQL at {url} did not respond within {CONNECT_TIMEOUT:?}");
    }
    eprintln!(
        "skipping PostgreSQL Scheduler integration test: PostgreSQL at {url} did not respond within {CONNECT_TIMEOUT:?}"
    );
    None
}
