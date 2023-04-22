//! thread pool

mod naive;
mod shared_queue;

pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

use crate::err::Result;

/// ThreadPool
pub trait ThreadPool {
    /// new a thread pool with size
    fn new(size: usize) -> Result<Self>
    where
        Self: Sized;

    /// spawn
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
