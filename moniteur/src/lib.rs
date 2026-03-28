#![allow(unused)]

use anyhow::{Result, bail};
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet, LocalSet};
use tracing::{Instrument, error, info_span, instrument};

pub use moniteur_macros::*;

pub trait Worker: Send + 'static {
    type Config: Clone + Send + PartialEq;

    /// Returns the name of the worker
    fn name(config: &Self::Config) -> &str;

    /// Initializes the worker
    fn init(config: &Self::Config) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    /// Runs the worker
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized;
}

#[derive(Clone)]
pub struct SupervisorPolicies {
    pub retry: RetryPolicy,
    pub stabilization_delay_s: u64,
}

impl Default for SupervisorPolicies {
    fn default() -> Self {
        Self {
            stabilization_delay_s: 30,
            retry: RetryPolicy::default(),
        }
    }
}

#[derive(Clone)]
pub enum RetryMode {
    UntilFailure,
    UntilSuccess,
    MaxCount(usize),
    Forever,
}

#[derive(Clone)]
pub struct RetryPolicy {
    retry_mode: RetryMode,
    initial_delay_s: u64,
    max_delay_s: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            retry_mode: RetryMode::Forever,
            initial_delay_s: 1,
            max_delay_s: 60,
        }
    }
}

#[derive(Default, Clone)]
pub struct WorkerStatus {
    pub worker_state: WorkerState,
    pub generation: usize,
    pub last_start: Option<Instant>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum WorkerState {
    #[default]
    NeverRan,
    Initializing,
    Running,
    Reseting,
    Failed,
    Complete,
}

impl Display for WorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            WorkerState::NeverRan => "NeverRan",
            WorkerState::Initializing => "Initializing",
            WorkerState::Running => "Running",
            WorkerState::Reseting => "Reseting",
            WorkerState::Failed => "Failed",
            WorkerState::Complete => "Complete",
        };
        write!(f, "{status}")
    }
}

pub struct WorkerContext<W: Worker> {
    pub label: String,
    pub config: W::Config,
    pub status_tx: watch::Sender<WorkerStatus>,
}

impl<W: Worker> WorkerContext<W> {
    pub fn new(label: impl Into<String>, config: impl Into<W::Config>) -> Self {
        let (status_tx, status_rx) = watch::channel(WorkerStatus::default());
        Self {
            label: label.into(),
            config: config.into(),
            status_tx,
        }
    }
}

/// The `Supervisor` takes a list of `WorkerContext`, a set of `SupervisorPolicies`, and once
/// `start()` is called, will run the workers as defined.
pub struct Supervisor<W: Worker> {
    pub enable: bool,
    pub label: String,
    pub policies: SupervisorPolicies,
    pub manifest: HashMap<String, WorkerContext<W>>,
    pub workers: HashMap<String, WorkerHandle<W>>,
}

impl<W: Worker> Supervisor<W> {
    pub async fn run(&mut self) {
        loop {
            self.reconcile().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn register_worker(&mut self, label: &str, config: W::Config) {
        let worker_ctx = WorkerContext::<W>::new(label, config.clone());
        self.manifest.insert(label.to_string(), worker_ctx);
    }

    async fn reconcile(&mut self) {
        // Clear all workers if supervisor is disabled
        if !self.enable {
            for (_label, handle) in self.workers.drain() {
                handle.task_handle.abort();
            }
            return;
        }

        // Clear out-of-sync workers
        self.workers
            .retain(|label, worker| match self.manifest.get(label) {
                Some(c) if c.config == worker.config => true,
                _ => {
                    worker.task_handle.abort();
                    false
                }
            });

        // Build missing workers
        for (label, m) in &self.manifest {
            if !self.workers.contains_key(label) {
                let config = m.config.clone();
                let label = label.clone();
                let status_tx = m.status_tx.clone();
                let handle = tokio::spawn(async move {
                    status_tx.send_modify(|ws| {
                        ws.worker_state = WorkerState::Initializing;
                        ws.generation += 1;
                    });
                    let mut new = W::init(&config).await?;
                    status_tx.send_modify(|ws| {
                        ws.worker_state = WorkerState::Running;
                    });
                    new.run().await
                });
                let wh = WorkerHandle {
                    label: label.clone(),
                    config: m.config.clone(),
                    task_handle: handle,
                    status_rx: m.status_tx.subscribe(),
                };
                self.workers.insert(label, wh);
            }
        }
    }
}

pub struct WorkerHandle<W: Worker> {
    pub label: String,
    pub config: W::Config,
    pub task_handle: JoinHandle<Result<()>>,
    pub status_rx: watch::Receiver<WorkerStatus>,
}

impl<W: Worker> WorkerHandle<W> {
    /// Waits for the worker to be in the given `WorkerState`
    pub async fn wait_for_state(&self, state: WorkerState) {
        let mut status_rx = self.status_rx.clone();
        status_rx.mark_changed();
        while status_rx.changed().await.is_ok() {
            let curr = status_rx.borrow_and_update();
            if curr.worker_state == state {
                return;
            }
        }
    }

    /// Waits for the worker to be in the `Running` state
    pub async fn running(&self) {
        self.wait_for_state(WorkerState::Running).await
    }

    /// Immediately returns `true` if the worker is in the `Running` state
    pub fn is_running(&self) -> bool {
        self.status_rx.borrow().worker_state == WorkerState::Running
    }

    /// Waits for the worker to be in the `Failed` state
    pub async fn failed(&self) {
        self.wait_for_state(WorkerState::Failed).await
    }

    /// Immediately returns `true` if the worker is in the `Failed` state
    pub fn is_failed(&self) -> bool {
        self.status_rx.borrow().worker_state == WorkerState::Failed
    }
}

#[cfg(test)]
mod test;
