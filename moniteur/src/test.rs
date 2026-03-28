use crate::*;

#[derive(Clone, PartialEq, Debug)]
pub struct MockConfig {
    pub id: String,
    pub fail_on_run: bool,
}

pub struct MockWorker {
    config: MockConfig,
}

impl Worker for MockWorker {
    type Config = MockConfig;

    fn name(config: &Self::Config) -> &str {
        &config.id
    }

    async fn init(config: &Self::Config) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        let (status_tx, status_rx) = watch::channel(WorkerStatus::default());
        let config = self.config.clone();
        let label = config.id.clone();

        let task_handle = tokio::spawn(async move {
            status_tx.send_modify(|s| s.worker_state = WorkerState::Running);

            if config.fail_on_run {
                tokio::time::sleep(Duration::from_millis(50)).await;
                status_tx.send_modify(|s| s.worker_state = WorkerState::Failed);
                bail!("Injected Failure");
            }

            // Stay alive until aborted
            std::future::pending::<Result<()>>().await
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_construction_and_dispatch() {
        let mut sup = Supervisor::<MockWorker> {
            enable: true,
            label: "test-sup".into(),
            policies: SupervisorPolicies::default(),
            manifest: HashMap::new(),
            workers: HashMap::new(),
        };

        let config = MockConfig {
            id: "worker-1".into(),
            fail_on_run: false,
        };
        sup.register_worker("worker-1", config);

        // Run reconciliation
        sup.reconcile().await;

        assert!(
            sup.workers.contains_key("worker-1"),
            "Worker should be in the active map"
        );
        let handle = sup.workers.get("worker-1").unwrap();

        // Wait for the worker to report it is running
        handle.running().await;
        assert!(handle.is_running());
    }

    #[tokio::test]
    async fn test_reconciliation_config_update() {
        let mut sup = Supervisor::<MockWorker> {
            enable: true,
            label: "test-sup".into(),
            policies: SupervisorPolicies::default(),
            manifest: HashMap::new(),
            workers: HashMap::new(),
        };

        // 1. Initial Start
        let config_v1 = MockConfig {
            id: "worker-1".into(),
            fail_on_run: false,
        };
        sup.register_worker("worker-1", config_v1.clone());
        sup.reconcile().await;

        let original_task_id = sup
            .workers
            .get("worker-1")
            .unwrap()
            .task_handle
            .abort_handle();

        // 2. Update Config (Change fail_on_run to true)
        // Note: Realistically, you'd update the manifest.
        // Here we clear and re-register for simplicity.
        sup.manifest.clear();
        let config_v2 = MockConfig {
            id: "worker-1".into(),
            fail_on_run: true,
        };
        sup.register_worker("worker-1", config_v2);

        // This is the core "todo!()" logic in your reconcile:
        // If config changed, kill and restart.
        sup.reconcile().await;

        let new_worker = sup.workers.get("worker-1").unwrap();
        assert_ne!(new_worker.config, config_v1, "Config should be updated");
        // In a real reconciler, the task ID would change because we aborted the old one.
    }

    #[tokio::test]
    async fn test_worker_failure_detection() {
        let mut sup = Supervisor::<MockWorker> {
            enable: true,
            label: "test-sup".into(),
            policies: SupervisorPolicies::default(),
            manifest: HashMap::new(),
            workers: HashMap::new(),
        };

        let config = MockConfig {
            id: "fail-worker".into(),
            fail_on_run: true,
        };
        sup.register_worker("fail-worker", config);

        sup.run().await;
        let handle = sup.workers.get("fail-worker").unwrap();

        // Wait for the task to hit the injected error
        handle.failed().await;
        assert!(handle.is_failed());
    }
}
