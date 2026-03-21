use super::*;
use anyhow::bail;
use moniteur_macros::WorkerRegistry;
use std::sync::Arc;
use tokio::sync::Mutex;
#[derive(Clone, WorkerRegistry)]
pub enum WorkerKind {
    Good(GoodTask),
    Fail(FailingInit),
    NestedSuccess(NestedTaskSuccess),
    NestedNestedSuccess(NestedNestedTaskSuccess),
    NestedFail(NestedTaskFail),
    NestedNestedFail(NestedNestedTaskFail),
}

#[derive(Clone)]
pub struct GoodTask {
    pub(crate) name: String,
}

impl Worker for GoodTask {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct FailingInit {
    pub(crate) name: String,
}

impl Worker for FailingInit {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self) -> Result<()> {
        bail!("Fail init")
    }

    async fn run(&mut self) -> Result<()> {
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub enum Data {
    Before,
    After,
}

#[derive(Clone)]
pub struct NestedTaskSuccess {
    pub name: String,
    pub inner_data: Arc<Mutex<Data>>,
}

impl NestedTaskSuccess {
    pub fn new(name: String) -> Self {
        Self {
            name,
            inner_data: Arc::new(Mutex::new(Data::Before)),
        }
    }
}

impl Worker for NestedTaskSuccess {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let w = WorkerKind::NestedNestedSuccess(NestedNestedTaskSuccess {
            name: "Failing init".to_string(),
            inner_data: self.inner_data.clone(),
        });
        let wc = WorkerContext::new(w);
        let mut sup = Supervisor {
            label: "".to_string(),
            workers: vec![wc],
            policies: SupervisorPolicies {
                retry: RetryPolicy {
                    retry_mode: RetryMode::MaxCount(3),
                    max_delay_s: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        };
        sup.start().await;
        let w = sup.workers.first_mut().unwrap();
        let handle = w.task_handle.as_mut().unwrap();
        let res = handle.await.unwrap();
        assert_eq!(w.status_tx.borrow().generation, 3);
        assert!(res.is_err());

        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct NestedNestedTaskSuccess {
    name: String,
    inner_data: Arc<Mutex<Data>>,
}

impl Worker for NestedNestedTaskSuccess {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let mut inner_data = self.inner_data.lock().await;
        *inner_data = Data::After;
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct NestedTaskFail {
    pub name: String,
    pub inner_data: Arc<Mutex<Data>>,
}

impl NestedTaskFail {
    pub fn new(name: String) -> Self {
        Self {
            name,
            inner_data: Arc::new(Mutex::new(Data::Before)),
        }
    }
}

impl Worker for NestedTaskFail {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let w = WorkerKind::NestedNestedFail(NestedNestedTaskFail {
            name: "Failing init".to_string(),
        });
        let wc = WorkerContext::new(w);
        let mut sup = Supervisor {
            label: "".to_string(),
            workers: vec![wc],
            policies: SupervisorPolicies {
                retry: RetryPolicy {
                    retry_mode: RetryMode::MaxCount(1),
                    ..Default::default()
                },
                ..Default::default()
            },
        };
        sup.start().await;
        let w = sup.workers.first_mut().unwrap();
        let handle = w.task_handle.as_mut().unwrap();
        let res = handle.await.unwrap();
        assert!(res.is_err());
        bail!("Failure")
    }

    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct NestedNestedTaskFail {
    name: String,
}

impl Worker for NestedNestedTaskFail {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self) -> Result<()> {
        bail!("Failure")
    }

    async fn run(&mut self) -> Result<()> {
        bail!("Failure")
    }

    async fn reset(&mut self) -> Result<()> {
        bail!("Failure")
    }
}
