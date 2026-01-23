pub mod dataset;
pub mod error;
pub mod properties;

pub use dataset::{Capacity, Dataset, ZfsManager};
// Re-export for module API
#[allow(unused_imports)]
pub use error::{Result, ZfsError};
pub use properties::VolumeMetadata;
