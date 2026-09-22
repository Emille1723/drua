mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{library_data_dir, reset_library_db_state, TestRepo};
use drua_library::{CommitAttribution, Library, LibraryConfig};

const PG_CON: &str = "postgres://user:password@localhost:5432/drua";

/// Ticker effectively disabled: convergence can only come from the
/// cross-replica `library_head_changed` PG NOTIFY wake-up.
const FETCH_INTERVAL_MS: u64 = 3_600_000;

async fn pool() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| PG_CON.to_string());
    sqlx::PgPool::connect(&url).await.expect("connect to pg")
}

async fn init_replica(
    test_name: &str,
    replica: &str,
    repo_url: &str,
    pool: &sqlx::PgPool,
    start_poll: bool,
) -> (Library, job::Jobs) {
    let data_dir = library_data_dir(test_name).join(replica);
    let embedder = Arc::new(code_assistant_core::embedder::Embedder::new().expect("embedder"));
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("job config");
    let mut jobs = job::Jobs::init(job_config).await.expect("jobs init");
    let config = LibraryConfig {
        data_dir: data_dir.to_string_lossy().to_string(),
        repo_url: repo_url.to_string(),
        fetch_interval_ms: FETCH_INTERVAL_MS,
    };
    let library = Library::init(pool, &config, embedder, &mut jobs, None)
        .await
        .expect("library init");
    if start_poll {
        jobs.start_poll().await.expect("start poll");
    }
    // `jobs` must outlive the test: dropping it drops the sync-job
    // initializer holding the fetcher's tick receiver, and the fetcher
    // exits on the first failed send.
    (library, jobs)
}

// Considerations
// One replica to read, one replica to write
// The false-positive (cross_replica_notify.rs) test considers eventual convergence
// Reading replica must read immediately after write ack leveraging no sleeps, no retries & no polling
// Test across rounds as the script (read-your-write-repro.sh) does:
// - Consider catching up the reader before comparison occurs on the current round
// - Prevent test round op until the reading_replica file read is caught up to the previous round's expected content
// - Reading replica read catchup inbetween testing rounds has been since removed
// - Capture when a stale read occurs
// - Assert that no stale reads should be found across the testing rounds

#[tokio::test]
#[ignore = "requires postgres + writes to tests/.library; run with --ignored"]
async fn write_on_one_replica_is_visible_on_peer_without_ticker() {
    let test_name = "write_on_one_replica_is_visible_on_peer_without_ticker";
    let _ = tracing_subscriber::fmt()
        .with_env_filter("drua_library=debug,info")
        .try_init();
    let fixture = TestRepo::init(&[("README.md", "init\n")]);
    let pool = pool().await;
    reset_library_db_state(&pool).await;

    // Only replica A polls jobs, so A is guaranteed to execute the
    // library.write job; B has no poller and a disabled ticker, so it
    // can only converge via the `library_head_changed` PG NOTIFY.
    let repo_url = fixture.path().to_string_lossy().to_string();
    let (writing_replica, writing_replica_jobs) = init_replica(test_name, "writing_replica", &repo_url, &pool, false).await;
    let (reading_replica, reading_replica_jobs) = init_replica(test_name, "reading_replica", &repo_url, &pool, false).await;

    let slug = "notify";
    let path = format!("spaces/{slug}/doc.md");
    let content_base = "test-round-";
    let doc_rel_path = "doc.md";
    let base_round = 1;
    let round_cap = 6;
    let total_rounds = round_cap - base_round;
    let mut rounds_passed = 0;

    // create space
    writing_replica
        .spaces()
        .create(slug.into(), None, CommitAttribution::library_default())
        .await
        .expect("create space");

    // create the file
    writing_replica
        .spaces()
        .write_file(
            slug,
            doc_rel_path,
            format!("{content_base}{base_round}"),
            CommitAttribution::library_default(),
        )
        .await
        .expect("initial write");

    let mut stale_reads_record: Vec<i32> = Vec::new();
    let mut no_content_record: Vec<i32> = Vec::new();
    let mut read_error_record: Vec<i32> = Vec::new();

    for round in base_round..round_cap
    {
        println!("\n");
        let write_content = format!("{content_base}{round}");

        // Write
        writing_replica
            .spaces()
            .write_file(
                slug,
                doc_rel_path,
                write_content.clone(),
                CommitAttribution::library_default(),
            )
            .await
            .expect("write");

        // Read & comparison
        // assessment advises to  consider: space().read_file()
        match reading_replica.spaces().read_file(slug, doc_rel_path).await {
        // match reading_replica.read_blob_at_head(&path).await {
            Ok(Some(content)) => {
                println!("Comparison Window");

                match String::from_utf8(content.clone()) {
                    Ok(content_as_string) => {
                        println!("Expected Content: {write_content}");
                        println!("Retrieved Content: {content_as_string}");
                    }
                    Err(err) => {
                        eprintln!("Failed to decode retrieved content as UTF-8: {err}");
                    }
                }

                if content == write_content.as_bytes() {
                    rounds_passed+=1;
                    println!(
                        "round {round}: FRESH \
                        (read on READING_REPLICA returned what WRITING_REPLICA just wrote)"
                    );
                } else {
                    stale_reads_record.push(round);
                    println!(
                        "round {round}: STALE \
                        (write acked on WRITING_REPLICA; read on READING_REPLICA returned old content)"
                    );
                }
            }

            Ok(None) => {
                no_content_record.push(round);
                println!("round {round}: NO CONTENT");
            }

            Err(err) => {
                read_error_record.push(round);
                eprintln!("round {round}: READ ERROR: {}", err);
            }
        }

        println!("\n");
    }

    // Observe any stale rounds
    // so far the race won is non-deterministic: it will lose but not guaranteed all of the time
    // from the context of repro'ing the script to capture the bug in code: all of the rounds will not fail to read the write
    assert!(rounds_passed == total_rounds, "READ-YOUR-WRITE VIOLATED: {}/{} rounds violated.\nStale reads found for the following rounds: {:?}\nNo content found for the following rounds: {:?}\nRead error for the following rounds: {:?}", (total_rounds - rounds_passed), total_rounds, stale_reads_record, no_content_record, read_error_record);

    println!("read-your-write held in all {} rounds.", total_rounds);
}
