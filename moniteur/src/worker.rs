use anyhow::Result;
use std::future::Future;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct RestartPolicy {
    pub mode: RetryMode,
    /// Initial time delay for the first retry
    pub init_retry_delay_s: u64,
    /// Maximum time delay between retries
    pub max_retry_delay_s: u64,
    /// How long does a worker has to run before the retry delay is reset to `init_retry_delay_s`
    pub stabilization_time_s: u64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            mode: RetryMode::Forever,
            init_retry_delay_s: 1,
            max_retry_delay_s: 30,
            stabilization_time_s: 30,
        }
    }
}

#[derive(Clone, Default)]
pub enum RetryMode {
    #[default]
    Forever,
    UntilFailure,
    UntilSuccess,
    RetryCount(u32),
}

pub trait Worker: Send + Sync + 'static {
    type Config: Clone + PartialEq + Send + Sync + 'static;
    type Workload: Workload;

    fn init(config: Self::Config) -> impl Future<Output = Result<Self::Workload>> + Send;
}

pub trait Workload: Send + 'static {
    fn run(self, ctx: WorkerContext) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone)]
pub struct WorkerSpec<C> {
    pub restart_policy: RestartPolicy,
    pub config: C,
}

pub struct WorkerControl<C> {
    pub config: C,
    pub cancel: CancellationToken,
    pub status: watch::Receiver<WorkerStatus>,
}

#[derive(Clone)]
pub struct WorkerContext {
    pub label: String,
    pub cancel: CancellationToken,
    pub status: watch::Sender<WorkerStatus>,
}

#[derive(Default)]
pub struct WorkerStatus {
    pub state: WorkerState,
    pub generation: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Default, PartialEq)]
pub enum WorkerState {
    #[default]
    Initializing,
    Running,
    Crashed,
    Terminated,
}
