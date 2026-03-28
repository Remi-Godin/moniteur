use crate::worker::*;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

enum SupervisorCommand<C> {
    /// Add or update a new entry to the manifest
    UpdateManifest { label: String, spec: WorkerSpec<C> },
    /// Delete an entry from the manifest
    DeleteManifest { label: String },
    /// Disable the supervisor (stops all workers)
    DisableSupervisor,
    /// Enable the supervisor (starts all workers)
    EnableSupervisor,
    /// Get a subscription to the status channel of the desired worker
    Subscribe {
        label: String,
        reply: oneshot::Sender<Option<watch::Receiver<WorkerStatus>>>,
    },
    /// Removes all terminated worker from the satus hub
    Purge,
}

#[derive(Clone)]
pub struct SupervisorHandle<W: Worker> {
    outbox: mpsc::Sender<SupervisorCommand<W::Config>>,
    supervisor_handle: Arc<JoinHandle<()>>,
}

impl<W: Worker> SupervisorHandle<W> {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let supervisor: Supervisor<W> = Supervisor {
            enable: true,
            manifest: HashMap::new(),
            registry: HashMap::new(),
            status_hub: HashMap::new(),
            inbox: rx,
            workloads: tokio::task::JoinSet::new(),
        };
        let task = tokio::spawn(async move {
            supervisor.run().await;
        });

        SupervisorHandle {
            outbox: tx,
            supervisor_handle: Arc::new(task),
        }
    }

    pub async fn update(&self, label: &str, spec: WorkerSpec<W::Config>) -> Result<()> {
        self.outbox
            .send(SupervisorCommand::UpdateManifest {
                label: label.into(),
                spec: spec,
            })
            .await
            .map_err(|e| anyhow!(e.to_string()))
    }

    pub async fn subscribe(
        &self,
        label: impl Into<String>,
    ) -> Option<watch::Receiver<WorkerStatus>> {
        let (reply, reply_rx) = oneshot::channel();
        self.outbox
            .send(SupervisorCommand::Subscribe {
                label: label.into(),
                reply,
            })
            .await;
        reply_rx.await.unwrap_or_default()
    }

    pub async fn delete(&self, label: &str) -> Result<()> {
        self.outbox
            .send(SupervisorCommand::DeleteManifest {
                label: label.into(),
            })
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        Ok(())
    }
}

struct Supervisor<W: Worker> {
    enable: bool,
    /// The desired state
    manifest: HashMap<String, WorkerSpec<W::Config>>,
    /// The active workers
    registry: HashMap<String, WorkerControl<W::Config>>,
    /// Status channels for workers
    status_hub: HashMap<String, watch::Sender<WorkerStatus>>,
    /// Supervisor command inbox
    inbox: mpsc::Receiver<SupervisorCommand<W::Config>>,
    /// The tasks spawned by the supervisor
    workloads: JoinSet<(String, Result<()>)>,
}

impl<W: Worker> Supervisor<W> {
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.inbox.recv() => {
                    self.handle_command(cmd).await;
                    self.reconcile().await;
                },
                Some(result) = self.workloads.join_next() => {
                    let (label, exit_result) = result.expect("TASK PANIC");
                    self.handle_exit(label, exit_result).await;
                    self.reconcile().await;
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: SupervisorCommand<W::Config>) {
        match cmd {
            SupervisorCommand::UpdateManifest { label, spec } => {
                self.manifest.insert(label, spec);
            }
            SupervisorCommand::DeleteManifest { label } => {
                self.manifest.remove(&label);
            }
            SupervisorCommand::DisableSupervisor => todo!(),
            SupervisorCommand::EnableSupervisor => todo!(),
            SupervisorCommand::Subscribe { label, reply } => {
                if let Some(status) = self.status_hub.get(&label) {
                    reply.send(Some(status.subscribe()));
                } else {
                    reply.send(None);
                }
            }
            SupervisorCommand::Purge => {
                self.status_hub
                    .retain(|label, status| status.borrow().state != WorkerState::Terminated);
            }
        }
    }

    async fn reconcile(&mut self) {
        self.registry
            .retain(|label, wc| match self.manifest.get(label) {
                // Handle out-of-sync workers
                Some(spec) if spec.config != wc.config => {
                    if let Some(status) = self.status_hub.get(label) {
                        status.send_modify(|s| s.state = WorkerState::Terminated);
                    }
                    wc.cancel.cancel();
                    false
                }
                // Handle orphan workers
                None => {
                    if let Some(status) = self.status_hub.get(label) {
                        status.send_modify(|s| s.state = WorkerState::Terminated);
                    }
                    wc.cancel.cancel();
                    false
                }
                // Keep in-sync workers
                _ => true,
            });

        let to_spawn: Vec<_> = self
            .manifest
            .iter()
            .filter(|(label, _)| !self.registry.contains_key(*label))
            .map(|(label, spec)| (label.clone(), spec.clone()))
            .collect();

        for (label, spec) in to_spawn {
            self.spawn_worker(label, spec).await;
        }
    }

    async fn spawn_worker(&mut self, label: String, spec: WorkerSpec<W::Config>) {
        let cancel = CancellationToken::new();
        let (status_tx, status_rx) = match self.status_hub.get(&label) {
            Some(r) => (r.clone(), r.subscribe()),
            None => watch::channel(WorkerStatus::default()),
        };

        self.status_hub
            .entry(label.clone())
            .or_insert(status_tx.clone());

        // Create the Control handle for the Registry
        let control = WorkerControl {
            cancel: cancel.clone(),
            status: status_rx,
            config: spec.config.clone(),
        };
        self.registry.insert(label.clone(), control);

        // Prepare context for the task
        let ctx = WorkerContext {
            label: label.clone(),
            cancel,
            status: status_tx,
        };

        // Spawn the lifecycle wrapper into the JoinSet
        let config = spec.config.clone();
        self.workloads.spawn(async move {
            let res = Self::run_worker_lifecycle(label.clone(), spec, ctx).await;
            (label, res)
        });
    }

    /// The "Worker Wrapper" - Handles the transition between Init and Run
    async fn run_worker_lifecycle(
        label: String,
        spec: WorkerSpec<W::Config>,
        ctx: WorkerContext,
    ) -> Result<()> {
        let mut retry_count = 0;
        let mut current_delay = spec.restart_policy.init_retry_delay_s;

        loop {
            // 1. Attempt Initialization
            ctx.status
                .send_modify(|s| s.state = WorkerState::Initializing);

            let start_time = std::time::Instant::now();

            match W::init(spec.config.clone()).await {
                Ok(workload) => {
                    // 2. Running Phase
                    ctx.status.send_modify(|s| {
                        s.state = WorkerState::Running;
                        s.generation += 1;
                        s.last_error = None;
                    });

                    // Execute the user logic
                    let run_result = workload.run(ctx.clone()).await;

                    // 3. Post-Run Analysis (Stabilization)
                    let run_duration = start_time.elapsed().as_secs();
                    if run_duration >= spec.restart_policy.stabilization_time_s {
                        // Worker was stable! Reset backoff for the next potential crash.
                        current_delay = spec.restart_policy.init_retry_delay_s;
                        retry_count = 0;
                    }

                    match run_result {
                        Ok(_) if matches!(spec.restart_policy.mode, RetryMode::UntilFailure) => {
                            break;
                        }
                        Ok(_) => {} // Continue to retry loop if mode allows
                        Err(e) => {
                            ctx.status.send_modify(|s| {
                                s.state = WorkerState::Crashed;
                                s.last_error = Some(e.to_string());
                            });
                        }
                    }
                }
                Err(e) => {
                    ctx.status.send_modify(|s| {
                        s.state = WorkerState::Crashed;
                        s.last_error = Some(format!("Init failed: {}", e));
                    });
                }
            }

            // 4. Check Retry Policy
            retry_count += 1;
            if !Self::should_retry(&spec.restart_policy, retry_count) {
                break;
            }

            // 5. Wait with Backoff (Interruptible by Cancellation)
            let delay = tokio::time::Duration::from_secs(current_delay);

            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    ctx.status.send_modify(|s| s.state = WorkerState::Terminated);
                    return Ok(());
                }
                _ = tokio::time::sleep(delay) => {
                    // Calculate next delay (Exponential Backoff)
                    current_delay = std::cmp::min(
                        current_delay * 2,
                        spec.restart_policy.max_retry_delay_s
                    );
                }
            }
        }

        ctx.status
            .send_modify(|s| s.state = WorkerState::Terminated);
        Ok(())
    }

    fn should_retry(policy: &RestartPolicy, count: u32) -> bool {
        match policy.mode {
            RetryMode::Forever => true,
            RetryMode::RetryCount(max) => count <= max,
            _ => true, // Simplified for brevity
        }
    }

    async fn handle_exit(&mut self, label: String, exit_result: Result<()>) {
        // Ensure the worker is removed from the registry so reconcile() sees the gap
        self.registry.remove(&label);

        match exit_result {
            Ok(_) => println!("Worker {} exited cleanly", label),
            Err(e) => println!("Worker {} failed: {:?}", label, e),
        }
    }
}
