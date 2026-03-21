use super::*;
use anyhow::bail;
use moniteur_macros::WorkerRegistry;

#[derive(Clone, WorkerRegistry)]
pub enum WorkerKind {
    Good(GoodTask),
    Fail(FailingInit),
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
