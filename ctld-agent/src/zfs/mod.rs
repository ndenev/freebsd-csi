pub mod dataset;
pub mod error;

pub use dataset::{Dataset, ZfsManager};
pub use error::{Result, ZfsError};
