use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use halo_harbor_contrib::scheduler::postgres::Postgres;
use harbor::Scheduler as _;
use harbor::scheduler::Lease;

use super::{conformance, fixture, server};

const DATABASES: usize = 17;
const TIMING: conformance::Timing = conformance::Timing::new(
    Duration::from_secs(2),
    Duration::from_millis(250),
    Duration::from_millis(300),
    Duration::from_millis(300),
    Duration::from_millis(200),
);

#[tokio::test]
async fn all_operations_conform() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let mut databases = Vec::with_capacity(DATABASES);
    for index in 0..DATABASES {
        databases.push(server.database(&format!("contract-{index}")).await);
    }
    let urls = Arc::new(Mutex::new(
        databases
            .iter()
            .map(|database| database.url().to_string())
            .collect::<VecDeque<_>>(),
    ));
    let lease = Lease::new(TIMING.lease().0, TIMING.lease().1).unwrap();
    conformance::run(
        {
            let urls = urls.clone();
            move || isolated(&urls, lease)
        },
        false,
        TIMING,
    )
    .await;
    conformance::lease(isolated(&urls, lease), TIMING).await;

    for database in databases {
        database.remove().await;
    }
    assert!(urls.lock().unwrap().is_empty());
}

fn isolated(urls: &Arc<Mutex<VecDeque<String>>>, lease: Lease) -> Postgres {
    let url = urls
        .lock()
        .unwrap()
        .pop_front()
        .expect("PostgreSQL conformance fixture exhausted its isolated databases");
    Postgres::new(url)
        .unwrap()
        .with_worker_id("worker-a")
        .unwrap()
        .with_worker_host("crawler-test-host")
        .unwrap()
        .with_worker_version("test")
        .unwrap()
        .with_modes([harbor::net::Mode::Http, harbor::net::Mode::Browser])
        .unwrap()
        .with_lease(lease)
        .unwrap()
}

#[tokio::test]
async fn claims_and_settles_a_request() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("basic").await;
    let scheduler = fixture::scheduler(database.url(), "worker-basic");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;

    scheduler
        .push(harbor::payload::Payload::new().requests(vec![fixture::request("basic")]))
        .await
        .unwrap();
    let request = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler.ack(&fixture::processing(&request)).await.unwrap();
    scheduler
        .success(&fixture::success(&request))
        .await
        .unwrap();
    assert!(!scheduler.has_pending_requests().await.unwrap());

    scheduler.close().await.unwrap();
    database.remove().await;
}
