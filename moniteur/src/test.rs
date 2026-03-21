use super::*;

mod test_data;
use test_data::*;

#[tokio::test]
async fn failing_task_1() {
    let w = WorkerKind::Fail(FailingInit {
        name: "Failing init".to_string(),
    });
    let wc = WorkerContext::new(w);
    let mut sup = Supervisor {
        label: "".to_string(),
        workers: vec![wc],
        policies: SupervisorPolicies {
            retry: RetryPolicy {
                retry_mode: RetryMode::UntilFailure,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    sup.start().await;
    let w = sup.workers.first_mut().unwrap();
    let handle = w.task_handle.as_mut().unwrap();
    let res = handle.await.unwrap();
    assert_eq!(w.status_tx.borrow().generation, 1);
    assert!(res.is_err());
}

#[tokio::test]
async fn failing_task_2() {
    let w = WorkerKind::Fail(FailingInit {
        name: "Failing init".to_string(),
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
}

#[tokio::test]
async fn success_task_1() {
    let w = WorkerKind::Good(GoodTask {
        name: "Good task".to_string(),
    });
    let wc = WorkerContext::new(w);
    let mut sup = Supervisor {
        label: "".to_string(),
        workers: vec![wc],
        policies: SupervisorPolicies {
            retry: RetryPolicy {
                retry_mode: RetryMode::UntilSuccess,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    sup.start().await;
    let w = sup.workers.first_mut().unwrap();
    let handle = w.task_handle.as_mut().unwrap();
    let res = handle.await.unwrap();
    assert_eq!(w.status_tx.borrow().generation, 1);
    assert!(res.is_ok());
}

#[tokio::test]
async fn nested_task_success_1() {
    let w = WorkerKind::NestedSuccess(NestedTaskSuccess::new("Nested".to_string()));
    let wc = WorkerContext::new(w);
    let mut sup = Supervisor {
        label: "".to_string(),
        workers: vec![wc],
        policies: SupervisorPolicies {
            retry: RetryPolicy {
                retry_mode: RetryMode::UntilSuccess,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    sup.start().await;
    let w = sup.workers.first_mut().unwrap();
    let handle = w.task_handle.as_mut().unwrap();
    let res = handle.await.unwrap();
    assert_eq!(w.status_tx.borrow().generation, 1);
    if let WorkerKind::NestedSuccess(worker) = &w.dispatcher {
        assert!(*worker.inner_data.try_lock().unwrap() == Data::After);
    }

    assert!(res.is_ok());
}

#[tokio::test]
async fn nested_task_fail_1() {
    let w = WorkerKind::NestedFail(NestedTaskFail::new("Nested".to_string()));
    let wc = WorkerContext::new(w);
    let mut sup = Supervisor {
        label: "".to_string(),
        workers: vec![wc],
        policies: SupervisorPolicies {
            retry: RetryPolicy {
                retry_mode: RetryMode::UntilFailure,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    sup.start().await;
    let w = sup.workers.first_mut().unwrap();
    let handle = w.task_handle.as_mut().unwrap();
    let res = handle.await.unwrap();
    assert_eq!(w.status_tx.borrow().generation, 1);
    if let WorkerKind::NestedFail(worker) = &w.dispatcher {
        assert!(*worker.inner_data.try_lock().unwrap() == Data::Before);
    }

    assert!(res.is_err());
}
