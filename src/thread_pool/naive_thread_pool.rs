use crate::err::Result;
use crate::thread_pool::ThreadPool;
use log::info;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// NaiveThreadPool
pub struct NaiveThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(size: usize) -> Result<Self> {
        assert!(size > 0);

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));

        let mut workers = Vec::with_capacity(size);
        for i in 0..size {
            let worker = Worker::new(i, Arc::clone(&rx))?;
            workers.push(worker);
        }

        Ok(NaiveThreadPool {
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
impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            info!("worker-{} shutting down", worker.id);
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
                    job();
                }
                Err(_) => {
                    info!("worker-{} disconnection; shutting down", id);
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
