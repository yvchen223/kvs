use crate::err::Result;
use crate::thread_pool::ThreadPool;
use log::{error, info};
use std::panic::AssertUnwindSafe;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::{panic, thread};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// SharedQueueThreadPool
pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(size: usize) -> Result<Self> {
        assert!(size > 0);

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));

        let mut workers = Vec::with_capacity(size);
        for i in 0..size {
            let worker = Worker::new(i, Arc::clone(&rx))?;
            workers.push(worker);
        }

        Ok(SharedQueueThreadPool {
            workers,
            sender: Some(tx),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);
        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}
impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                handle.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Result<Self> {
        let handle = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();
            match message {
                Ok(job) => {
                    if panic::catch_unwind(AssertUnwindSafe(job)).is_err() {
                        error!("worker-{} panic", id);
                    }
                }
                Err(_) => {
                    break;
                }
            }
        });
        Ok(Worker {
            id,
            thread: Some(handle),
        })
    }
}
