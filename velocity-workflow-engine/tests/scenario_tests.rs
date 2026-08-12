//! Real-world scenario tests for the VELOCITY-WorkFlow engine.
//!
//! Each test simulates a realistic Temporal-like workflow use case, exercising
//! signals, queries, child workflows, saga compensation, and multi-step lifecycles.

use std::time::Instant;

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use velocity_workflow_engine::saga::{SagaOrchestrator, SagaStepDefinition};

// ═══════════════════════════════════════════════════════════════════════════════
// Helper: run a multi-step workflow to completion
// ═══════════════════════════════════════════════════════════════════════════════

fn complete_all_steps(engine: &WorkflowEngine, key: u64, step_results: &[&[u8]]) {
    for (i, result) in step_results.iter().enumerate() {
        engine.complete_step(key, i as u32, result.to_vec());
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Scenario Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_payment_processing_saga() {
    let start = Instant::now();
    println!("test_payment_processing_saga: multi-step payment with compensation");
    let engine = WorkflowEngine::new();
    let orchestrator = SagaOrchestrator::new();

    // Saga: charge card → debit bank → send receipt
    let steps = vec![
        SagaStepDefinition::new("charge_card", 100)
            .with_compensation(200, Some(b"refund_card".to_vec())),
        SagaStepDefinition::new("debit_bank", 101)
            .with_compensation(201, Some(b"refund_bank".to_vec())),
        SagaStepDefinition::new("send_receipt", 102)
            .with_compensation(202, Some(b"cancel_receipt".to_vec())),
    ];
    let saga_id = orchestrator.create_saga(42, steps);

    // Start the payment workflow
    let wf_key = engine.start_workflow(1001, 1, 0, 42, 3, Some(b"payment:amount=99.99".to_vec()));

    // Complete first two steps successfully
    orchestrator.complete_step(saga_id, 0, Some(b"card_charged".to_vec()));
    engine.complete_step(wf_key, 0, b"card_charged".to_vec());
    orchestrator.complete_step(saga_id, 1, Some(b"bank_debited".to_vec()));
    engine.complete_step(wf_key, 1, b"bank_debited".to_vec());

    // Third step fails — triggers compensation
    let compensations = orchestrator.fail_step(saga_id, 2);
    assert_eq!(
        compensations.len(),
        2,
        "Should compensate 2 completed steps"
    );
    assert_eq!(compensations[0].0, 201); // refund_bank (reverse order)
    assert_eq!(compensations[1].0, 200); // refund_card

    // Execute compensations as workflows
    for (comp_type, _input) in &compensations {
        let comp_key = engine.start_workflow(*comp_type, *comp_type, 0, 42, 1, None);
        engine.complete_step(comp_key, 0, b"compensated".to_vec());
        engine.complete_workflow(comp_key, None);
    }

    engine.fail_workflow(wf_key);
    assert_eq!(engine.get_status(wf_key), WorkflowStatus::Failed);
    println!(
        "  payment saga compensated in {:?} — {} workflows created",
        start.elapsed(),
        engine.workflow_count()
    );
    engine.shutdown();
}

#[test]
fn test_order_fulfillment_workflow() {
    let start = Instant::now();
    println!("test_order_fulfillment_workflow: order → payment → shipping → notification");
    let engine = WorkflowEngine::new();

    let order_key =
        engine.start_workflow(2001, 1, 0, 42, 4, Some(b"order:SKU-12345,qty=2".to_vec()));

    // Step 0: Validate order
    engine.complete_step(order_key, 0, b"order_validated".to_vec());
    // Step 1: Process payment
    engine.complete_step(order_key, 1, b"payment_processed:TXN-789".to_vec());
    // Step 2: Ship
    engine.complete_step(order_key, 2, b"shipped:TRACK-ABC".to_vec());
    // Step 3: Notify customer
    engine.complete_step(order_key, 3, b"notification_sent".to_vec());

    // Customer sends a status query signal
    engine.signal_workflow(order_key, 1, b"query_status".to_vec());
    assert!(engine.has_signal(order_key, 1));

    engine.complete_workflow(order_key, Some(b"order_fulfilled".to_vec()));
    assert_eq!(engine.get_status(order_key), WorkflowStatus::Completed);
    println!("  order fulfilled in {:?}", start.elapsed());
    engine.shutdown();
}

#[test]
fn test_user_signup_with_verification() {
    println!("test_user_signup_with_verification: signup → email verification → activation");
    let engine = WorkflowEngine::new();

    let signup_key =
        engine.start_workflow(3001, 1, 0, 42, 3, Some(b"signup:user@example.com".to_vec()));

    // Step 0: Create account
    engine.complete_step(signup_key, 0, b"account_created:UID-100".to_vec());
    // Step 1: Send verification email (schedule timer for timeout)
    engine.complete_step(signup_key, 1, b"email_sent".to_vec());
    let timer_id = engine.schedule_timer(signup_key, 3_600_000); // 1 hour timeout

    // Simulate verification signal arriving before timeout
    engine.signal_workflow(signup_key, 10, b"verification_token:ABC123".to_vec());
    assert!(engine.has_signal(signup_key, 10));
    let token = engine.take_signal(signup_key, 10).unwrap();
    assert_eq!(token, b"verification_token:ABC123");

    // Step 2: Activate account
    engine.complete_step(signup_key, 2, b"account_activated".to_vec());
    engine.complete_workflow(signup_key, Some(b"user_active".to_vec()));
    assert_eq!(engine.get_status(signup_key), WorkflowStatus::Completed);

    // Cancel the timeout timer (no longer needed)
    engine.timer_engine().cancel(timer_id);
    println!("  user signup completed");
    engine.shutdown();
}

#[test]
fn test_batch_processing_pipeline() {
    println!("test_batch_processing_pipeline: ingest → validate → transform → load");
    let engine = WorkflowEngine::new();
    let batch_size = 100u64;

    // Parent workflow orchestrating batch
    let parent_key = engine.start_workflow(4001, 1, 0, 42, 4, Some(b"batch:file.csv".to_vec()));

    // Step 0: Ingest
    engine.complete_step(
        parent_key,
        0,
        format!("ingested:{} records", batch_size).into_bytes(),
    );
    // Step 1: Validate — spawn child workflows for each record
    for i in 0..batch_size {
        let child_key = engine.start_child_workflow(
            parent_key,
            4100 + i,
            2,
            42,
            1,
            Some(format!("record-{}", i).into_bytes()),
        );
        engine.complete_step(child_key, 0, b"valid".to_vec());
        engine.complete_workflow(child_key, None);
    }
    engine.complete_step(
        parent_key,
        1,
        format!("validated:{} records", batch_size).into_bytes(),
    );
    // Step 2: Transform
    engine.complete_step(parent_key, 2, b"transformed".to_vec());
    // Step 3: Load
    engine.complete_step(parent_key, 3, b"loaded".to_vec());

    engine.complete_workflow(parent_key, Some(b"batch_complete".to_vec()));
    assert_eq!(engine.get_status(parent_key), WorkflowStatus::Completed);
    println!(
        "  batch of {} records processed, {} total workflows",
        batch_size,
        engine.workflow_count()
    );
    engine.shutdown();
}

#[test]
fn test_scheduled_report_generation() {
    println!("test_scheduled_report_generation: cron-triggered report workflow");
    let engine = WorkflowEngine::new();

    // Register a cron schedule that fires every minute
    let schedule_id = engine
        .register_cron("* * * * *", 5001, 0, 42, 3, 0)
        .unwrap();
    assert!(schedule_id > 0);

    // Advance time to trigger the cron
    let fired_keys = engine.process_cron_fires(1);
    assert!(!fired_keys.is_empty(), "Cron should have fired");

    // Process the triggered workflow
    for key in &fired_keys {
        engine.complete_step(*key, 0, b"data_collected".to_vec());
        engine.complete_step(*key, 1, b"report_generated".to_vec());
        engine.complete_step(*key, 2, b"report_distributed".to_vec());
        engine.complete_workflow(*key, Some(b"report_complete".to_vec()));
        assert_eq!(engine.get_status(*key), WorkflowStatus::Completed);
    }
    println!(
        "  scheduled report generated, {} fires processed",
        fired_keys.len()
    );
    engine.shutdown();
}

#[test]
fn test_approval_workflow() {
    println!("test_approval_workflow: submit → review → approve/reject with timeout");
    let engine = WorkflowEngine::new();

    let approval_key = engine.start_workflow(
        6001,
        1,
        0,
        42,
        3,
        Some(b"approval:request-001,amount=5000".to_vec()),
    );

    // Step 0: Submit request
    engine.complete_step(approval_key, 0, b"submitted".to_vec());
    // Step 1: Review (waiting for signal from approver)
    engine.complete_step(approval_key, 1, b"under_review".to_vec());

    // Schedule timeout for approval
    let timeout_timer = engine.schedule_timer(approval_key, 86_400_000); // 24h

    // Approver sends approval signal
    engine.signal_workflow(approval_key, 20, b"decision:approved,by:manager-1".to_vec());
    assert!(engine.has_signal(approval_key, 20));

    // Step 2: Process approval
    engine.complete_step(approval_key, 2, b"approved".to_vec());
    engine.complete_workflow(approval_key, Some(b"request_approved".to_vec()));
    assert_eq!(engine.get_status(approval_key), WorkflowStatus::Completed);

    engine.timer_engine().cancel(timeout_timer);
    println!("  approval workflow completed");
    engine.shutdown();
}

#[test]
fn test_inventory_management() {
    println!("test_inventory_management: stock check → reserve → fulfill → reorder");
    let engine = WorkflowEngine::new();

    let inv_key = engine.start_workflow(
        7001,
        1,
        0,
        42,
        4,
        Some(b"inventory:SKU-ABC,qty=10".to_vec()),
    );

    // Step 0: Check stock
    engine.complete_step(inv_key, 0, b"stock_available:100".to_vec());
    // Step 1: Reserve items
    engine.complete_step(inv_key, 1, b"reserved:10".to_vec());

    // External signal: shipping confirmation
    engine.signal_workflow(inv_key, 30, b"shipping_confirmed:TRACK-XYZ".to_vec());

    // Step 2: Fulfill order
    engine.complete_step(inv_key, 2, b"fulfilled".to_vec());
    // Step 3: Check reorder threshold
    engine.complete_step(inv_key, 3, b"reorder_triggered:threshold=20".to_vec());

    engine.complete_workflow(inv_key, Some(b"inventory_processed".to_vec()));
    assert_eq!(engine.get_status(inv_key), WorkflowStatus::Completed);
    println!("  inventory management completed");
    engine.shutdown();
}

#[test]
fn test_customer_onboarding() {
    println!("test_customer_onboarding: KYC → credit check → account creation");
    let engine = WorkflowEngine::new();
    let orchestrator = SagaOrchestrator::new();

    let steps = vec![
        SagaStepDefinition::new("kyc_verification", 100)
            .with_compensation(200, Some(b"cancel_kyc".to_vec())),
        SagaStepDefinition::new("credit_check", 101)
            .with_compensation(201, Some(b"reverse_credit_check".to_vec())),
        SagaStepDefinition::new("create_account", 102)
            .with_compensation(202, Some(b"close_account".to_vec())),
    ];
    let saga_id = orchestrator.create_saga(42, steps);

    let onboard_key =
        engine.start_workflow(8001, 1, 0, 42, 3, Some(b"onboarding:customer-123".to_vec()));

    // KYC passes
    orchestrator.complete_step(saga_id, 0, Some(b"kyc_passed".to_vec()));
    engine.complete_step(onboard_key, 0, b"kyc_passed".to_vec());

    // Credit check passes
    orchestrator.complete_step(saga_id, 1, Some(b"credit_score:750".to_vec()));
    engine.complete_step(onboard_key, 1, b"credit_passed".to_vec());

    // Account created
    orchestrator.complete_step(saga_id, 2, Some(b"account_created:ACC-456".to_vec()));
    engine.complete_step(onboard_key, 2, b"account_created".to_vec());

    engine.complete_workflow(onboard_key, Some(b"customer_onboarded".to_vec()));
    assert_eq!(engine.get_status(onboard_key), WorkflowStatus::Completed);
    println!("  customer onboarding completed via saga");
    engine.shutdown();
}

#[test]
fn test_data_pipeline_orchestration() {
    println!("test_data_pipeline_orchestration: extract → transform → load with retries");
    let engine = WorkflowEngine::new();

    let pipeline_key = engine.start_workflow(
        9001,
        1,
        0,
        42,
        3,
        Some(b"pipeline:source=db,target=warehouse".to_vec()),
    );

    // Step 0: Extract — schedule with activity timeouts
    engine.schedule_activity_with_timeouts(
        pipeline_key,
        0,
        300,
        b"extract".to_vec(),
        5000,
        30000,
        60000,
        10000,
    );
    engine.complete_step(pipeline_key, 0, b"extracted:10000_rows".to_vec());

    // Step 1: Transform
    engine.schedule_activity_with_timeouts(
        pipeline_key,
        1,
        301,
        b"transform".to_vec(),
        5000,
        60000,
        120000,
        10000,
    );
    engine.complete_step(pipeline_key, 1, b"transformed:10000_rows".to_vec());

    // Step 2: Load
    engine.schedule_activity_with_timeouts(
        pipeline_key,
        2,
        302,
        b"load".to_vec(),
        5000,
        120000,
        300000,
        10000,
    );
    engine.complete_step(pipeline_key, 2, b"loaded:10000_rows".to_vec());

    engine.complete_workflow(pipeline_key, Some(b"pipeline_complete".to_vec()));
    assert_eq!(engine.get_status(pipeline_key), WorkflowStatus::Completed);
    println!("  data pipeline completed with activity timeouts");
    engine.shutdown();
}

#[test]
fn test_microservice_choreography() {
    println!("test_microservice_choreography: multi-service coordination via signals");
    let engine = WorkflowEngine::new();

    let coord_key =
        engine.start_workflow(10001, 1, 0, 42, 4, Some(b"choreography:order-123".to_vec()));

    // Start child workflows for each service
    let order_svc =
        engine.start_child_workflow(coord_key, 10101, 2, 42, 1, Some(b"process_order".to_vec()));
    let payment_svc = engine.start_child_workflow(
        coord_key,
        10102,
        3,
        42,
        1,
        Some(b"process_payment".to_vec()),
    );
    let inventory_svc = engine.start_child_workflow(
        coord_key,
        10103,
        4,
        42,
        1,
        Some(b"reserve_inventory".to_vec()),
    );
    let shipping_svc = engine.start_child_workflow(
        coord_key,
        10104,
        5,
        42,
        1,
        Some(b"schedule_shipping".to_vec()),
    );

    // Each service completes and signals the coordinator
    engine.complete_step(order_svc, 0, b"order_processed".to_vec());
    engine.complete_workflow(order_svc, None);
    engine.signal_workflow(coord_key, 50, b"order_done".to_vec());

    engine.complete_step(payment_svc, 0, b"payment_processed".to_vec());
    engine.complete_workflow(payment_svc, None);
    engine.signal_workflow(coord_key, 51, b"payment_done".to_vec());

    engine.complete_step(inventory_svc, 0, b"inventory_reserved".to_vec());
    engine.complete_workflow(inventory_svc, None);
    engine.signal_workflow(coord_key, 52, b"inventory_done".to_vec());

    engine.complete_step(shipping_svc, 0, b"shipping_scheduled".to_vec());
    engine.complete_workflow(shipping_svc, None);
    engine.signal_workflow(coord_key, 53, b"shipping_done".to_vec());

    // Verify all signals received
    assert!(engine.has_signal(coord_key, 50));
    assert!(engine.has_signal(coord_key, 51));
    assert!(engine.has_signal(coord_key, 52));
    assert!(engine.has_signal(coord_key, 53));

    // Coordinator completes all steps
    complete_all_steps(
        &engine,
        coord_key,
        &[
            b"order_done",
            b"payment_done",
            b"inventory_done",
            b"shipping_done",
        ],
    );
    engine.complete_workflow(coord_key, Some(b"choreography_complete".to_vec()));
    assert_eq!(engine.get_status(coord_key), WorkflowStatus::Completed);
    println!(
        "  microservice choreography completed, {} workflows",
        engine.workflow_count()
    );
    engine.shutdown();
}

#[test]
fn test_deployment_pipeline() {
    println!("test_deployment_pipeline: build → test → stage → promote → rollback");
    let engine = WorkflowEngine::new();

    let deploy_key = engine.start_workflow(
        11001,
        1,
        0,
        42,
        5,
        Some(b"deploy:service-api,v2.1.0".to_vec()),
    );

    // Step 0: Build
    engine.complete_step(
        deploy_key,
        0,
        b"build_success:docker-image-sha256:abc123".to_vec(),
    );
    // Step 1: Test
    engine.complete_step(deploy_key, 1, b"tests_passed:142/142".to_vec());
    // Step 2: Stage
    engine.complete_step(deploy_key, 2, b"staged:staging-cluster".to_vec());

    // Smoke test signal from QA
    engine.signal_workflow(deploy_key, 60, b"smoke_test:passed".to_vec());

    // Step 3: Promote to production
    engine.complete_step(deploy_key, 3, b"promoted:prod-cluster".to_vec());

    // Error signal: high error rate detected
    engine.signal_workflow(deploy_key, 61, b"alert:high_error_rate".to_vec());

    // Step 4: Rollback
    engine.complete_step(deploy_key, 4, b"rolled_back:v2.0.9".to_vec());
    engine.complete_workflow(deploy_key, Some(b"deployment_rolled_back".to_vec()));
    assert_eq!(engine.get_status(deploy_key), WorkflowStatus::Completed);
    println!("  deployment pipeline completed with rollback");
    engine.shutdown();
}

#[test]
fn test_subscription_lifecycle() {
    println!("test_subscription_lifecycle: subscribe → renew → upgrade → cancel");
    let engine = WorkflowEngine::new();

    let sub_key = engine.start_workflow(
        12001,
        1,
        0,
        42,
        4,
        Some(b"subscription:user-456,plan=basic".to_vec()),
    );

    // Step 0: Subscribe
    engine.complete_step(sub_key, 0, b"subscribed:basic,$9.99/mo".to_vec());

    // Renewal signal (monthly)
    engine.signal_workflow(sub_key, 70, b"renewal:month_1".to_vec());
    engine.signal_workflow(sub_key, 70, b"renewal:month_2".to_vec());
    engine.signal_workflow(sub_key, 70, b"renewal:month_3".to_vec());

    // Step 1: Process renewals
    engine.complete_step(sub_key, 1, b"renewals_processed:3".to_vec());

    // Upgrade signal
    engine.signal_workflow(sub_key, 71, b"upgrade:plan=premium,$19.99/mo".to_vec());

    // Step 2: Upgrade
    engine.complete_step(sub_key, 2, b"upgraded:premium".to_vec());

    // Cancellation signal
    engine.signal_workflow(sub_key, 72, b"cancel:reason=too_expensive".to_vec());

    // Step 3: Cancel
    engine.complete_step(sub_key, 3, b"cancelled:prorated_refund".to_vec());
    engine.complete_workflow(sub_key, Some(b"subscription_ended".to_vec()));
    assert_eq!(engine.get_status(sub_key), WorkflowStatus::Completed);
    println!("  subscription lifecycle completed");
    engine.shutdown();
}

#[test]
fn test_travel_booking() {
    println!("test_travel_booking: flight + hotel + car rental with saga compensation");
    let engine = WorkflowEngine::new();
    let orchestrator = SagaOrchestrator::new();

    let steps = vec![
        SagaStepDefinition::new("book_flight", 100)
            .with_compensation(200, Some(b"cancel_flight".to_vec())),
        SagaStepDefinition::new("book_hotel", 101)
            .with_compensation(201, Some(b"cancel_hotel".to_vec())),
        SagaStepDefinition::new("book_car_rental", 102)
            .with_compensation(202, Some(b"cancel_car".to_vec())),
    ];
    let saga_id = orchestrator.create_saga(42, steps);
    let travel_key = engine.start_workflow(
        13001,
        1,
        0,
        42,
        3,
        Some(b"travel:NYC,dates=2025-03-01_to_2025-03-05".to_vec()),
    );

    // Book flight — success
    orchestrator.complete_step(saga_id, 0, Some(b"flight_booked:AA-123".to_vec()));
    engine.complete_step(travel_key, 0, b"flight_booked".to_vec());

    // Book hotel — success
    orchestrator.complete_step(saga_id, 1, Some(b"hotel_booked:Hilton-456".to_vec()));
    engine.complete_step(travel_key, 1, b"hotel_booked".to_vec());

    // Book car rental — fails!
    let compensations = orchestrator.fail_step(saga_id, 2);
    assert_eq!(compensations.len(), 2);

    // Execute compensations
    for (comp_type, _) in &compensations {
        let comp_key = engine.start_workflow(*comp_type, *comp_type, 0, 42, 1, None);
        engine.complete_step(comp_key, 0, b"compensated".to_vec());
        engine.complete_workflow(comp_key, None);
    }

    engine.fail_workflow(travel_key);
    assert_eq!(engine.get_status(travel_key), WorkflowStatus::Failed);
    println!("  travel booking saga compensated: flight + hotel cancelled");
    engine.shutdown();
}

#[test]
fn test_loan_origination() {
    println!("test_loan_origination: application → underwriting → approval → funding");
    let engine = WorkflowEngine::new();

    let loan_key = engine.start_workflow(
        14001,
        1,
        0,
        42,
        4,
        Some(b"loan:applicant=Borrower-789,amount=250000".to_vec()),
    );

    // Step 0: Application received
    engine.complete_step(loan_key, 0, b"application_received:APP-001".to_vec());

    // Step 1: Underwriting (multiple signals for documents)
    engine.signal_workflow(loan_key, 80, b"doc:income_verification".to_vec());
    engine.signal_workflow(loan_key, 80, b"doc:credit_report".to_vec());
    engine.signal_workflow(loan_key, 80, b"doc:property_appraisal".to_vec());
    engine.complete_step(loan_key, 1, b"underwriting_complete:score=720".to_vec());

    // Step 2: Approval
    engine.complete_step(loan_key, 2, b"approved:rate=6.5%,term=30yr".to_vec());

    // Step 3: Funding
    engine.complete_step(loan_key, 3, b"funded:wire_transfer_initiated".to_vec());

    engine.complete_workflow(loan_key, Some(b"loan_funded:LOAN-001".to_vec()));
    assert_eq!(engine.get_status(loan_key), WorkflowStatus::Completed);
    println!("  loan origination completed");
    engine.shutdown();
}

#[test]
fn test_incident_response() {
    println!("test_incident_response: detect → page → investigate → resolve → postmortem");
    let engine = WorkflowEngine::new();

    let incident_key = engine.start_workflow(
        15001,
        1,
        0,
        42,
        5,
        Some(b"incident:severity=SEV1,service=payments".to_vec()),
    );

    // Step 0: Detection
    engine.complete_step(incident_key, 0, b"detected:high_latency,p99=5s".to_vec());

    // Step 1: Page on-call
    engine.complete_step(incident_key, 1, b"paged:oncall-engineer-1".to_vec());

    // Step 2: Investigation (signals from monitoring)
    engine.signal_workflow(incident_key, 90, b"metric:cpu_usage=95%".to_vec());
    engine.signal_workflow(incident_key, 90, b"metric:memory_usage=88%".to_vec());
    engine.signal_workflow(incident_key, 91, b"log:oom_killer_triggered".to_vec());
    engine.complete_step(
        incident_key,
        2,
        b"root_cause:memory_leak_in_service-v3.2.1".to_vec(),
    );

    // Step 3: Resolution
    engine.complete_step(incident_key, 3, b"resolved:rolled_back_to_v3.2.0".to_vec());

    // Step 4: Postmortem
    engine.complete_step(
        incident_key,
        4,
        b"postmortem:action_items=5,severity=SEV1".to_vec(),
    );

    engine.complete_workflow(incident_key, Some(b"incident_closed:MTTR=45min".to_vec()));
    assert_eq!(engine.get_status(incident_key), WorkflowStatus::Completed);
    println!("  incident response completed");
    engine.shutdown();
}
