#![allow(unused)]
pub mod supervisor;
pub mod worker;

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    use crate::{
        supervisor::SupervisorHandle,
        worker::{RestartPolicy, Worker, WorkerSpec, WorkerState, Workload},
    };
    use std::{collections::HashMap, time::Duration};

    #[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
    pub enum MockBehavior {
        Success { delay_ms: u64 },
        FailInit,
        FailRun { delay_ms: u64 },
        Panic,
        Hang,
    }

    #[derive(Clone, PartialEq, Serialize, Deserialize)]
    pub struct MockConfig {
        pub id: u64,
        pub behavior: MockBehavior,
    }

    #[derive(Clone)]
    pub struct MockWorker;
    impl Worker for MockWorker {
        type Config = MockConfig;
        type Workload = MockWorkload;

        async fn init(config: Self::Config) -> anyhow::Result<Self::Workload> {
            if config.behavior == MockBehavior::FailInit {
                anyhow::bail!("Init Failed");
            }
            Ok(MockWorkload { config })
        }
    }

    pub struct MockWorkload {
        config: MockConfig,
    }
    impl Workload for MockWorkload {
        async fn run(self, ctx: crate::worker::WorkerContext) -> anyhow::Result<()> {
            match self.config.behavior {
                MockBehavior::Success { delay_ms } => {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    Ok(())
                }
                MockBehavior::FailRun { delay_ms } => {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    anyhow::bail!("Run Failed")
                }
                MockBehavior::Panic => panic!("User Code Panic"),
                MockBehavior::Hang => {
                    std::future::pending::<()>().await;
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn test_spawn_and_run() {
        let handle = SupervisorHandle::<MockWorker>::new();
        let label = "test-worker".to_string();

        // 1. Send command
        handle
            .update(
                &label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 1,
                        behavior: MockBehavior::Success { delay_ms: 50 },
                    },
                },
            )
            .await;

        // 3. Observe status
        let mut rx = handle.subscribe(&label).await.expect("Worker should exist");

        // Wait for "Running" state
        rx.wait_for(|s| s.state == WorkerState::Running)
            .await
            .unwrap();
        assert_eq!(rx.borrow().generation, 1);
    }

    #[tokio::test]
    async fn test_spawn_and_observe_status() {
        let handle = SupervisorHandle::<MockWorker>::new();

        let label = "test-worker";
        let spec = WorkerSpec {
            config: MockConfig {
                id: 1,
                behavior: MockBehavior::Success { delay_ms: 50 },
            },
            restart_policy: RestartPolicy::default(),
        };

        handle.update(&label, spec).await;

        // 2. Subscribe to status
        let mut rx = handle.subscribe(label).await.unwrap();

        // 3. Wait for Running state
        // Note: This requires a helper or manual loop on rx.changed()
        while rx.borrow().state != WorkerState::Running {
            rx.changed().await.unwrap();
        }

        assert_eq!(rx.borrow().generation, 1);
        assert_eq!(rx.borrow().state, WorkerState::Running);
    }

    #[tokio::test]
    async fn test_reconciliation_on_config_change() {
        let handle = SupervisorHandle::<MockWorker>::new();

        let label = "test-worker";

        // 1. Start with version 1
        handle
            .update(
                label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 1,
                        behavior: MockBehavior::Hang,
                    },
                },
            )
            .await
            .unwrap();
        let mut rx = handle.subscribe(label).await.unwrap();

        // Wait for gen 1
        while rx.borrow().generation != 1 {
            rx.changed().await.unwrap();
        }

        // 2. Update to version 2 (Diffing logic should see ID changed)
        handle
            .update(
                label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 2,
                        behavior: MockBehavior::Hang,
                    },
                },
            )
            .await
            .unwrap();

        // 3. Verify it reached generation 2
        // The supervisor should have cancelled gen 1 and spawned gen 2
        tokio::time::timeout(Duration::from_secs(1), rx.wait_for(|i| i.generation == 2))
            .await
            .unwrap();

        assert_eq!(rx.borrow().state, WorkerState::Running);
    }

    #[tokio::test]
    async fn test_reconciliation_on_config_remove() {
        let handle = SupervisorHandle::<MockWorker>::new();

        let label = "test-worker";

        // 1. Start with version 1
        handle
            .update(
                label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 1,
                        behavior: MockBehavior::Hang,
                    },
                },
            )
            .await
            .unwrap();
        let mut rx = handle.subscribe(label).await.unwrap();

        // Wait for gen 1
        while rx.borrow().generation != 1 {
            rx.changed().await.unwrap();
        }

        // 2. Update to version 2 (Diffing logic should see ID changed)
        handle.delete(label).await.unwrap();

        // 3. Verify it reached generation 2
        rx.wait_for(|s| s.state == WorkerState::Terminated).await;

        assert_eq!(rx.borrow().state, WorkerState::Terminated);
    }

    #[tokio::test]
    async fn test_no_reconciliation_on_config_unchanged() {
        let handle = SupervisorHandle::<MockWorker>::new();

        let label = "test-worker";

        // 1. Start with version 1
        handle
            .update(
                label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 1,
                        behavior: MockBehavior::Hang,
                    },
                },
            )
            .await
            .unwrap();
        let mut rx = handle.subscribe(label).await.unwrap();

        // Wait for gen 1
        while rx.borrow().generation != 1 {
            rx.changed().await.unwrap();
        }

        // 2. Update to version 2 (Diffing logic should see ID changed)
        handle
            .update(
                label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 1,
                        behavior: MockBehavior::Hang,
                    },
                },
            )
            .await
            .unwrap();

        // 3. Verify it reached generation 2
        // The supervisor should have cancelled gen 1 and spawned gen 2
        tokio::time::sleep(Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(1), rx.wait_for(|i| i.generation == 1))
            .await
            .unwrap();

        assert_eq!(rx.borrow().state, WorkerState::Running);
    }

    #[tokio::test]
    async fn test_reconciliation_on_failure() {
        let handle = SupervisorHandle::<MockWorker>::new();

        let label = "test-worker";

        handle
            .update(
                label,
                WorkerSpec {
                    restart_policy: RestartPolicy::default(),
                    config: MockConfig {
                        id: 1,
                        behavior: MockBehavior::FailRun { delay_ms: 1000 },
                    },
                },
            )
            .await
            .unwrap();
        let mut rx = handle.subscribe(label).await.unwrap();
        // Should reach generation 1 immediately
        rx.wait_for(|i| i.generation == 1).await.unwrap();

        // Should reach generation 2 in about 2 second (retry delay + fail delay)
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|i| i.generation == 2))
            .await
            .unwrap();
    }
}
