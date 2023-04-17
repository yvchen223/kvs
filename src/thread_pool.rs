//! thread pool

mod naive_thread_pool;

pub use naive_thread_pool::NaiveThreadPool;


use crate::err::{Result, Error};

/// ThreadPool
pub trait ThreadPool {
    /// new a thread pool with size
    fn new(size: usize) -> Result<Self> where Self: Sized;

    /// spawn
    fn spawn<F>(&self, job: F)
    where F: FnOnce() + Send + 'static;
}

