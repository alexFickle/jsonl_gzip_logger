use jsonl_gzip_logger::{init, read};
use log::{Level, LevelFilter};
use rusty_fork::rusty_fork_test;
use std::path::{Path, PathBuf};

/// Creates a path for a test.
/// The given name must be unique across all tests.
fn path(name: &str) -> PathBuf {
    let mut path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    path.set_extension("jsonl.gzip");
    path
}

// init() can only be called once per process, so
// have to run each of these tests in their own process.
rusty_fork_test! {

#[test]
fn empty_log() {
    let path = path("empty");
    init(&path, LevelFilter::Info).unwrap();
    log::logger().flush();

    let iter = read(&path).unwrap();

    assert_eq!(0, iter.count());
}

#[test]
fn one_log() {
    let path = path("one");
    init(&path, LevelFilter::Info).unwrap();
    log::info!(target: "foo", "This is a log!");
    log::logger().flush();

    let mut iter = read(&path).unwrap();

    let entry = iter.next().unwrap();
    assert_eq!(Level::Info, entry.level);
    assert_eq!("foo", entry.target);
    assert_eq!("This is a log!", entry.body);

    assert_eq!(0, iter.count());
}


#[test]
fn two_logs() {
    let path = path("two");
    init(&path, LevelFilter::Trace).unwrap();
    log::info!(target: "foo", "This is foo log!");
    log::debug!(target: "bar", "This is bar log!");
    log::logger().flush();

    let mut iter = read(&path).unwrap();

    let entry = iter.next().unwrap();
    assert_eq!(Level::Info, entry.level);
    assert_eq!("foo", entry.target);
    assert_eq!("This is foo log!", entry.body);

    let entry = iter.next().unwrap();
    assert_eq!(Level::Debug, entry.level);
    assert_eq!("bar", entry.target);
    assert_eq!("This is bar log!", entry.body);

    assert_eq!(0, iter.count());
}

}
