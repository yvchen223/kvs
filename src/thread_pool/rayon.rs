//! ThreadPool type from rayon crate

use crate::thread_pool::ThreadPool;
use crate::{Error, Result};

/// RayonThreadPool type from rayon crate
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(size: usize) -> Result<Self>
    where
        Self: Sized,
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(size)
            .build()
            .map_err(|e| Error::StringError(format!("{:?}", e)))?;
        Ok(RayonThreadPool { pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}
