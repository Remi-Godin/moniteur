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
        undispatched_workers: vec![wc],
        dispatched_workers: Vec::new(),
        policies: SupervisorPolicies {
            retry: RetryPolicy {
                retry_mode: RetryMode::UntilFailure,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    sup.start().await;
    let w = sup.dispatched_workers.first_mut().unwrap();
    let handle = &mut w.task_handle;
    let res = handle.await.unwrap();
    assert_eq!(w.status_rx.borrow().generation, 1);
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
        undispatched_workers: vec![wc],
        dispatched_workers: Vec::new(),
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
    let w = sup.dispatched_workers.first_mut().unwrap();
    let handle = &mut w.task_handle;
    let res = handle.await.unwrap();
    assert_eq!(w.status_rx.borrow().generation, 3);
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
        undispatched_workers: vec![wc],
        dispatched_workers: Vec::new(),
        policies: SupervisorPolicies {
            retry: RetryPolicy {
                retry_mode: RetryMode::UntilSuccess,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    sup.start().await;
    let w = sup.dispatched_workers.first_mut().unwrap();
    let handle = &mut w.task_handle;
    let res = handle.await.unwrap();
    assert_eq!(w.status_rx.borrow().generation, 1);
    assert!(res.is_ok());
}
