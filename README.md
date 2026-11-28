# Harbor

[简体中文](README.zh-CN.md) | English

`Harbor` is a Rust crawler runtime for code-driven and YAML Rules-driven workloads. The same Engine
contract supports local execution and durable Redis, MySQL 9, or PostgreSQL scheduling.

## Highlights

- Code mode and YAML Rules mode share the same Spider, Request, Response, Item, Middleware, and Engine.
- Memory, Redis, MySQL, and PostgreSQL Scheduler implementations use one scheduling contract.
- HTTP downloading includes retries, bounded bodies, redirects, cookies, proxy/TLS settings, and
  deterministic response decoding.
- Browser downloading uses `thirtyfour` with Chrome/Chromium, rendered DOM responses, Session cookie
  reuse, and optional CSS waits.
- CSS selection, deterministic CSS healing, RFC 9535 JSONPath, and explicit OpenAI-compatible JSON-object extraction are built in.
- Items use an independent Store contract; JSONL is the default local implementation.
- Optional `fastrace` instrumentation traces each Request independently without changing business
  `trace_id` or crawler control flow.

## Quick Start

Requirements: Rust 1.97.0 or later.

Run the complete code-mode example:

```bash
cargo run -p examples --bin basic
```

Run the Rules example:

```bash
cargo run -p examples --bin rules
```

The examples also include a real fund data graph: `examples/rules-eastmoney-funds-daily.yaml`
collects the paged ranking plus daily detail pages, while
`examples/rules-eastmoney-funds-history.yaml` is intended for one initial historical-net-value
run. The daily/one-time schedule is created by the external Scheduler or Cron service; the Rules DSL
only describes the Request graph.

`halo-harbor-worker` is a generic Worker library. The application assembles it with a concrete
Scheduler through `Engine::with_scheduler(...)`; Memory, Redis, MySQL, and PostgreSQL are replaceable Scheduler
implementations and are not selected through Worker environment variables.
Each Scheduler owns its own registration, heartbeat, claiming, recovery, and settlement behavior.

The smallest Engine assembly is:

```rust
let mut engine = halo::harbor::engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

`start()` keeps the Worker alive while the queue is empty and shuts down cleanly on SIGINT/SIGTERM.
The default runtime uses the in-memory Scheduler and writes Items as JSONL below `./data/items/`.
Redis, MySQL, or PostgreSQL replaces only the scheduling dependency; Item persistence remains independently
replaceable.

For an external application, the core runtime and contrib implementations are separate crates:

```toml
[dependencies]
halo = { package = "halo-harbor", version = "0.1.10" }
halo_harbor_contrib = { package = "halo-harbor-contrib", version = "0.4.0" }
```

Use the core and contrib APIs through their respective crate roots:

```rust
use halo::harbor::Request;
use halo_harbor_contrib::scheduler::redis::Redis;
```

PostgreSQL scheduling uses the same contract:

```rust
use halo_harbor_contrib::scheduler::postgres::Postgres;

let scheduler = Postgres::new("postgres://user:password@127.0.0.1:5432/harbor")?;
```

Install `contrib/sql/postgres/schema.sql` before opening the Scheduler. The PostgreSQL backend does
not create or migrate operator-owned tables automatically.

`halo_harbor_contrib` is not re-exported as `halo::harbor_contrib`; the two crates remain
independent until a future facade is designed.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `halo-harbor` | Core runtime, Engine, Scheduler contracts, HTTP downloader, Rules, selectors, and Item Store |
| `halo-harbor-macros` | Spider, handler, and Item derive macros |
| `halo-harbor-contrib` | Redis/MySQL/PostgreSQL Schedulers and distributed middleware implementations |
| `halo-harbor-worker` | Generic Worker library that runs the shared Engine with injected Scheduler, Downloader, Spider, Middleware, and Store |
| `examples` | Executable code-mode and Rules-mode examples |

## Documentation

- [Usage guide](docs/usage.md)
- [Architecture and current capabilities](docs/architecture.md)
- [Code-mode example](examples/src/bin/basic.rs)
- [Rules example](examples/src/bin/rules.rs) and [Rules configuration](examples/rules-newspaper.yaml)

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features runtime-tracing
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

Real-network, browser, and external-service acceptance tests are excluded from the default test flow. Run them explicitly with `--ignored` after preparing the required dependencies, for example:

```bash
cargo test --test eastmoney_funds -- --ignored --nocapture
cargo test --test json_selector -- --ignored --nocapture
cargo test --test x_profile -- --ignored --nocapture
cargo test --test x_account -- --ignored --nocapture
```

Browser tests require `WEBDRIVER_URL`; X API account tests require `X_USERNAME` and `X_BEARER_TOKEN`. A failure in an external acceptance test only reports an issue with that real service or environment; it does not indicate a failure in the default deterministic test suite.

Published versions are `halo-harbor 0.1.10`, `halo-harbor-contrib 0.4.0`,
`halo-harbor-macros 0.1.10`, and `halo-harbor-worker 0.2.10`. The project uses the MIT license.
