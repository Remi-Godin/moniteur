use anyhow::{Result, bail};
use std::fmt::Display;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{Instrument, error, info_span, instrument};

pub use moniteur_macros::*;

pub trait Worker: Send + Sync + 'static {
    /// Returns the name of the worker
    fn name(&self) -> &str;

    /// Initializes the worker
    fn init(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Runs the worker
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Resets the worker state
    fn reset(&mut self) -> impl Future<Output = Result<()>> + Send;
}

pub trait WorkerDispatcher: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn init(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn reset(&mut self) -> impl Future<Output = Result<()>> + Send;
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
    worker_state: WorkerState,
    generation: usize,
    last_start: Option<Instant>,
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

pub struct WorkerContext<D> {
    dispatcher: D,
    status_tx: watch::Sender<WorkerStatus>,
}

impl<D: WorkerDispatcher> WorkerContext<D> {
    /// Creates a new `WorkerContext` from the provided `Dispatcher`
    pub fn new(dispatcher: D) -> Self {
        let (status_tx, _status_rx) = watch::channel(WorkerStatus::default());
        Self {
            dispatcher,
            status_tx,
        }
    }

    /// Waits for the worker to be in the given `WorkerState`
    pub async fn wait_for_state(&self, state: WorkerState) {
        let mut status_rx = self.status_tx.subscribe();
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
        self.status_tx.borrow().worker_state == WorkerState::Running
    }

    /// Waits for the worker to be in the `Failed` state
    pub async fn failed(&self) {
        self.wait_for_state(WorkerState::Failed).await
    }

    /// Immediately returns `true` if the worker is in the `Failed` state
    pub fn is_failed(&self) -> bool {
        self.status_tx.borrow().worker_state == WorkerState::Failed
    }
}

/// The `Supervisor` takes a list of `WorkerContext`, a set of `SupervisorPolicies`, and once
/// `start()` is called, will run the workers as defined.
pub struct Supervisor<D: WorkerDispatcher> {
    pub label: String,
    pub policies: SupervisorPolicies,
    pub undispatched_workers: Vec<WorkerContext<D>>,
    pub dispatched_workers: Vec<WorkerHandle>,
}

pub struct WorkerHandle {
    pub task_handle: JoinHandle<Result<()>>,
    pub status_rx: watch::Receiver<WorkerStatus>,
}

impl<D: WorkerDispatcher> Supervisor<D> {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            policies: SupervisorPolicies::default(),
            undispatched_workers: Vec::new(),
            dispatched_workers: Vec::new(),
        }
    }

    pub fn register_new_worker_context(&mut self, worker_context: WorkerContext<D>) {
        self.undispatched_workers.push(worker_context);
    }

    pub async fn start(&mut self) {
        let mut handles = Vec::with_capacity(self.undispatched_workers.len());
        for w in &mut self.undispatched_workers.drain(..) {
            let policies = self.policies.clone();
            let label = self.label.clone();
            let worker = w.dispatcher;
            let status_rx = w.status_tx.subscribe();
            let handle = tokio::spawn(async move {
                Supervisor::start_worker_supervision(label, w.status_tx, policies, worker).await
            });
            handles.push(WorkerHandle {
                task_handle: handle,
                status_rx,
            });
        }
        self.dispatched_workers.extend(handles);
    }

    /// Starts the supervision of all workers controlled by this `Supervisor`
    #[instrument(name = "supervisor", skip(status_tx, _policies, dispatcher))]
    pub async fn start_worker_supervision(
        label: String,
        status_tx: watch::Sender<WorkerStatus>,
        _policies: SupervisorPolicies,
        mut dispatcher: D,
    ) -> Result<()> {
        let SupervisorPolicies {
            stabilization_delay_s,
            retry:
                RetryPolicy {
                    retry_mode,
                    initial_delay_s,
                    max_delay_s,
                },
        } = _policies;

        let initial_delay_s = initial_delay_s.max(1);
        let mut curr_retry_time = initial_delay_s;
        loop {
            status_tx.send_modify(|ws| {
                ws.generation += 1;
                ws.last_start = Some(Instant::now());
            });
            let span = info_span!("worker", label=%dispatcher.name(), generation=%status_tx.borrow().generation);

            // Init
            status_tx.send_modify(|ws| ws.worker_state = WorkerState::Initializing);
            let mut has_error = false;
            if let Err(e) = dispatcher.init().instrument(span.clone()).await {
                has_error = true;
                error!(error=%e, "Task init failed");
            } else {
                // Run
                status_tx.send_modify(|ws| ws.worker_state = WorkerState::Running);
                if let Err(e) = dispatcher.run().instrument(span.clone()).await {
                    has_error = true;
                    error!(error=%e, "Task run failed");
                }
            }

            // Reset retry time if the worker has stabilized
            if let Some(last_start) = status_tx.borrow().last_start
                && last_start.elapsed() > Duration::from_secs(stabilization_delay_s)
            {
                curr_retry_time = initial_delay_s;
            }

            // Reset
            status_tx.send_modify(|ws| ws.worker_state = WorkerState::Reseting);
            if let Err(e) = dispatcher.reset().instrument(span.clone()).await {
                has_error = true;
                error!(error=%e, "Task reset failed");
                status_tx.send_modify(|ws| ws.worker_state = WorkerState::Failed);
            }

            match retry_mode {
                RetryMode::UntilFailure => {
                    if has_error {
                        bail!("RetryPolicy::UntilFailure triggered due to worker failure");
                    }
                }
                RetryMode::MaxCount(max_count) => {
                    if status_tx.borrow().generation >= max_count {
                        bail!("RetryPolicy::MaxCount triggered due to max count achieved")
                    }
                }
                RetryMode::UntilSuccess => {
                    if !has_error {
                        break;
                    }
                }
                RetryMode::Forever => (),
            }
            tokio::time::sleep(Duration::from_secs(curr_retry_time)).await;
            if curr_retry_time == 1 {
                curr_retry_time = 2;
            } else {
                curr_retry_time = curr_retry_time.saturating_mul(2).min(max_delay_s);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test;
