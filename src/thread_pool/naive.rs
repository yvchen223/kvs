use crate::err::Result;
use crate::thread_pool::ThreadPool;
use std::thread;

/// NaiveThreadPool
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(size: usize) -> Result<Self> {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
