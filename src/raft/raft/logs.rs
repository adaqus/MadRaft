use super::LogEntry;
use serde::{Deserialize, Serialize};
use std::ops::{Index, RangeFrom};

pub type LogIndex = usize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Log {
    offset: usize,
    entries: Vec<LogEntry>,
}

impl Default for Log {
    fn default() -> Self {
        Log::new()
    }
}

impl Log {
    pub fn new() -> Self {
        Log {
            offset: 1,
            entries: vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        index
            .checked_sub(self.offset)
            .and_then(|i| self.entries.get(i))
    }

    pub fn push(&mut self, log: LogEntry) -> LogIndex {
        self.entries.push(log);
        self.offset += 1;
        self.offset
    }

    pub fn prev_log(&self, index: usize) -> Option<&LogEntry> {
        self.get(index - 1)
    }

    pub fn last_log_term(&self) -> usize {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    // Clear all entries from the log starting from the given index
    pub fn clear_from(&mut self, index: LogIndex) {
        if index < self.offset {
            self.entries.truncate(index - 1);
        }
    }
}

impl Index<usize> for Log {
    type Output = LogEntry;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap_or_else(|| {
            panic!(
                "index {} out of range {}..{}",
                index,
                self.offset,
                self.len()
            )
        })
    }
}

impl Index<RangeFrom<usize>> for Log {
    type Output = [LogEntry];
    fn index(&self, range: RangeFrom<usize>) -> &Self::Output {
        let start = range.start.checked_sub(self.offset).expect("out of range");
        &self.entries[start..]
    }
}
