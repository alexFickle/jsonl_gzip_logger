use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use log::{Level, LevelFilter};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Arguments,
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::Mutex,
    time::{Duration, Instant},
};
use thiserror::Error;

/// A log from a log file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// Time offset of this log entry from the start of logging.
    pub offset: Duration,
    /// Logging level of this log entry.
    pub level: Level,
    /// Target of this log entry.
    pub target: String,
    /// Message of this log entry.
    pub body: String,
}

/// Internal type that serializes the same as LogEntry.
#[derive(Serialize)]
struct LogEntryArgs<'a> {
    offset: Duration,
    level: Level,
    target: &'a str,
    body: Arguments<'a>,
}

/// Logger that logs to a .jsonl.gz file.
struct Logger {
    start: Instant,
    dest: Mutex<GzEncoder<File>>,
}

/// Error type for [`init`].
#[derive(Error, Debug)]
pub enum InitError {
    /// Creating the log file failed.
    #[error("failed to create log file: {0}")]
    CreateFileError(#[from] std::io::Error),
    /// Failed to globally install the logger.
    #[error("{0}")]
    SetLoggerError(#[from] log::SetLoggerError),
}

/// Creates and installs a global logger that logs to a new .jsonl.gz file at
/// the given path.
pub fn init<P: AsRef<std::path::Path>>(path: P, level: LevelFilter) -> Result<(), InitError> {
    let logger = Box::new(Logger {
        start: Instant::now(),
        dest: Mutex::new(GzEncoder::new(File::create(path)?, Compression::fast())),
    });
    log::set_boxed_logger(logger)?;
    log::set_max_level(level);
    Ok(())
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let entry = LogEntryArgs {
                offset: self.start.elapsed(),
                level: record.level(),
                target: record.target(),
                body: *record.args(),
            };
            if let Ok(mut writer) = self.dest.lock() {
                serde_json::to_writer(&mut *writer, &entry).unwrap();
                writer.write_all(&[b'\n']).unwrap();
            }
        }
    }

    fn flush(&self) {
        if let Ok(mut writer) = self.dest.lock() {
            writer.flush().unwrap();
        }
    }
}

/// Iterator that reads over the entries in a .jsonl.gz log file.
pub struct LogEntryIter {
    source: BufReader<GzDecoder<File>>,
    buffer: Vec<u8>,
}

impl Iterator for LogEntryIter {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.buffer.clear();
        self.source.read_until(b'\n', &mut self.buffer).ok()?;
        if self.buffer.last() != Some(&b'\n') {
            // last line of the log was truncated, ignore it
            return None;
        }
        serde_json::from_slice(&self.buffer[..]).ok()
    }
}

/// Opens a .jsonl.gz log file to be read by a [`LogEntryIter`].
pub fn read<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<LogEntryIter> {
    Ok(LogEntryIter {
        source: BufReader::new(GzDecoder::new(File::open(path)?)),
        buffer: Vec::new(),
    })
}

#[cfg(test)]
mod test {
    use super::*;

    /// The JSON representation of a LogEntry must be stable.
    #[test]
    fn log_entry_json_stability() {
        let json = "{\"offset\":{\"secs\":123456,\"nanos\":987654321},\"level\":\"WARN\",\"target\":\"foo.bar.baz\",\"body\":\"Log information goes here!\"}";
        let parsed: LogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(Duration::new(123_456, 987_654_321), parsed.offset);
        assert_eq!(Level::Warn, parsed.level);
        assert_eq!("foo.bar.baz", parsed.target);
        assert_eq!("Log information goes here!", parsed.body);
    }

    /// A serialized LogEntry must deserialize to an equivalent LogEntry.
    #[test]
    fn log_entry_json_round_trip() {
        let entry = LogEntry {
            offset: Duration::new(120, 123_456_789),
            level: Level::Error,
            target: " my target 123 ".to_string(),
            body: "This is the body of the log. \nfoobarbaz ".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry.offset, parsed.offset);
        assert_eq!(entry.level, parsed.level);
        assert_eq!(entry.target, parsed.target);
        assert_eq!(entry.body, parsed.body);
    }

    /// A serialiazed LogEntryArgs must deserialize to an equivalent LogEntry.
    #[test]
    fn log_entry_args_to_log_entry() {
        let json = serde_json::to_string(&LogEntryArgs {
            offset: Duration::new(20, 100),
            level: Level::Debug,
            target: "test.foo.bar",
            body: format_args!("{} + {} == {}", 1, 2, 3),
        })
        .unwrap();
        let entry: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(Duration::new(20, 100), entry.offset);
        assert_eq!(Level::Debug, entry.level);
        assert_eq!("test.foo.bar", entry.target);
        assert_eq!("1 + 2 == 3", entry.body);
    }

    /// The JSON representation of all `log::Level`s must be stable.
    mod log_level_json_serialization_stability {
        use super::*;

        /// Creates JSON from a LogEntry with the given log level.
        fn entry_json_with_level(level: Level) -> serde_json::Value {
            let entry = LogEntry {
                offset: Duration::default(),
                level,
                target: "target".to_string(),
                body: "body".to_string(),
            };
            serde_json::to_value(entry).unwrap()
        }

        #[test]
        fn trace() {
            assert_eq!(
                "TRACE",
                entry_json_with_level(Level::Trace).as_object().unwrap()["level"]
            )
        }

        #[test]
        fn debug() {
            assert_eq!(
                "DEBUG",
                entry_json_with_level(Level::Debug).as_object().unwrap()["level"]
            )
        }

        #[test]
        fn info() {
            assert_eq!(
                "INFO",
                entry_json_with_level(Level::Info).as_object().unwrap()["level"]
            )
        }

        #[test]
        fn warn() {
            assert_eq!(
                "WARN",
                entry_json_with_level(Level::Warn).as_object().unwrap()["level"]
            )
        }

        #[test]
        fn error() {
            assert_eq!(
                "ERROR",
                entry_json_with_level(Level::Error).as_object().unwrap()["level"]
            )
        }

        /// Creates JSON from a LogEntryArgs with the given log level.
        fn entry_args_json_with_level(level: Level) -> serde_json::Value {
            serde_json::to_value(LogEntryArgs {
                offset: Duration::default(),
                level,
                target: "target",
                body: format_args!("body"),
            })
            .unwrap()
        }

        #[test]
        fn trace_args() {
            assert_eq!(
                "TRACE",
                entry_args_json_with_level(Level::Trace)
                    .as_object()
                    .unwrap()["level"]
            )
        }

        #[test]
        fn debug_args() {
            assert_eq!(
                "DEBUG",
                entry_args_json_with_level(Level::Debug)
                    .as_object()
                    .unwrap()["level"]
            )
        }

        #[test]
        fn info_args() {
            assert_eq!(
                "INFO",
                entry_args_json_with_level(Level::Info).as_object().unwrap()["level"]
            )
        }

        #[test]
        fn warn_args() {
            assert_eq!(
                "WARN",
                entry_args_json_with_level(Level::Warn).as_object().unwrap()["level"]
            )
        }

        #[test]
        fn error_args() {
            assert_eq!(
                "ERROR",
                entry_args_json_with_level(Level::Error)
                    .as_object()
                    .unwrap()["level"]
            )
        }
    }

    /// The JSON representation of all `log::Level`s must be stable.
    mod log_level_json_deserialization_stability {
        use super::*;

        /// Creates a LogEntry from a JSON string with the given log level.
        fn entry_with_level(level: &str) -> LogEntry {
            let json = format!("{{\"offset\":{{\"secs\":123456,\"nanos\":987654321}},\"level\":\"{}\",\"target\":\"foo.bar.baz\",\"body\":\"Log information goes here!\"}}", level);
            serde_json::from_str(&json).unwrap()
        }

        #[test]
        fn trace() {
            assert_eq!(Level::Trace, entry_with_level("TRACE").level);
        }

        #[test]
        fn debug() {
            assert_eq!(Level::Debug, entry_with_level("DEBUG").level);
        }

        #[test]
        fn info() {
            assert_eq!(Level::Info, entry_with_level("INFO").level);
        }

        #[test]
        fn warn() {
            assert_eq!(Level::Warn, entry_with_level("WARN").level);
        }

        #[test]
        fn error() {
            assert_eq!(Level::Error, entry_with_level("ERROR").level);
        }
    }
}
