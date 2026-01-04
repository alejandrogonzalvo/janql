pub mod builder;
pub mod reader;

pub use builder::SSTableBuilder;
pub use reader::{SSTableReader, SearchResult};

pub(crate) const BLOCK_SIZE: usize = 4 * 1024; // 4KB
pub(crate) const TOMBSTONE: u32 = u32::MAX;
