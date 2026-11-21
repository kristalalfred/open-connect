use super::*;
use connect_core::{
    DeclaredTopology, DeliveryCapability, DeliveryMode, DeliveryPathReport, DeliveryPolicy,
    DeliveryReport, DeliveryRole, DeliveryState, EgressStatus, FormatConstraint, HopEndpointClass,
    HopProfile, HopState, HopStatus, LinkCondition, MediaBehavior, NodeCapabilities,
    NodeDescriptor, PayloadKind, RoleSet, SocketStatus, Transport, TransportClass,
};

fn node(id: &str, epoch: u64) -> NodeDescriptor {
    let mut node: NodeDescriptor = serde_json::from_value(serde_json::json!({
        "id":id, "endpoint":"http://127.0.0.1", "status":"ready",
        "topology":{"attachments":[
            {"id":"a", "network":"a", "dial":true,"listeners":{"srt":{"host":"127.0.0.1","port_range":{"start":20000,"end":20100}}}},
            {"id":"b", "network":"b", "dial":true,"listeners":{"srt":{"host":"127.0.0.1","port_range":{"start":21000,"end":21100}}}}
        ]}
    })).unwrap();
    let class = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    let plain = HopProfile {
        id: "srt-forward".into(),
        ingress: class(),
        egress: class(),
        max_egresses: None,
        media_behavior: MediaBehavior::Preserve,
        merge: false,
        accepts: Some(FormatConstraint::default()),
        timed_delivery: None,
    };
    let mut timed = plain.clone();
    timed.id = "srt-timed".into();
    timed.merge = true;
    timed.timed_delivery = Some(DeliveryCapability {
        protocol: connect_core::delivery::TIMED_PROTOCOL.into(),
        roles: vec![
            DeliveryRole::Origin,
            DeliveryRole::Forward,
            DeliveryRole::Receiver,
        ],
        modes: vec![
            DeliveryMode::Fixed,
            DeliveryMode::Auto,
            DeliveryMode::LowLatency,
        ],
        max_latency_ms: 60000,
        scheduling_error_bound_us: 1000,
        clock_domain: "shared-host:test".into(),
        origin_epoch: epoch,
    });
    node.capabilities = NodeCapabilities {
        hop_profiles: vec![plain, timed],
    };
    node
}

fn stream() -> StreamDefinition {
    serde_json::from_value(serde_json::json!({
        "name":"timed", "source":{"srt":{"node":"source"}},
        "destinations":[{"id":"studio","paths":2,"delivery":{"mode":"fixed","latency_ms":500},"srt":{"node":"studio","latency":100}}]
    })).unwrap()
}

fn observed() -> ObservedState {
    ObservedState {
        nodes: vec![node("source", 11), node("studio", 22)],
        hops: vec![],
        endpoints: vec![],
    }
}

#[test]
fn timed_routes_frame_internal_links_and_merge_only_authorized_copies() {
    let observed = observed();
    let outcome = reconcile_with_revisions(
        vec![stream()],
        &observed,
        &LinkKeys::for_tests(),
        &crate::peers::PeerPlanning::default(),
        &BTreeMap::from([("timed".into(), 7)]),
    );
    let hops = &outcome.hops_by_stream["timed"];
    assert_eq!(hops.len(), 2);
    let origin = &hops[0];
    let receiver = &hops[1];
    assert_eq!(origin.delivery.as_ref().unwrap().epoch, 11);
    assert_eq!(
        origin.delivery.as_ref().unwrap().delivery_id,
        receiver.delivery.as_ref().unwrap().delivery_id
    );
    assert!(receiver.merge_ingress.is_some());
    assert!(origin.egresses.iter().all(|egress| matches!(&egress.socket, connect_core::SocketSpec::Srt(socket) if socket.params().payload == PayloadKind::ConnectTimedV1)));
    assert!(
        matches!(&receiver.egresses[0].socket, connect_core::SocketSpec::Srt(socket) if socket.params().payload == PayloadKind::Raw)
    );
    let mut budget_changed = stream();
    budget_changed.destinations[0].delivery = Some(DeliveryPolicy::Fixed {
        latency_ms: 600,
        tolerance_ms: 5,
    });
    let next = reconcile_with_revisions(
        vec![budget_changed],
        &observed,
        &LinkKeys::for_tests(),
        &crate::peers::PeerPlanning::default(),
        &BTreeMap::from([("timed".into(), 7)]),
    );
    assert_eq!(
        origin.revision(),
        next.hops_by_stream["timed"][0].revision()
    );
    assert_ne!(
        receiver.revision(),
        next.hops_by_stream["timed"][1].revision()
    );
}

#[test]
fn unsupported_nodes_clock_domains_and_impossible_budgets_are_not_scheduled() {
    for failure in 0..3 {
        let mut observed = observed();
        let mut definition = stream();
        match failure {
            0 => observed.nodes[0]
                .capabilities
                .hop_profiles
                .retain(|profile| profile.timed_delivery.is_none()),
            1 => {
                observed.nodes[1].capabilities.hop_profiles[1]
                    .timed_delivery
                    .as_mut()
                    .unwrap()
                    .clock_domain = "different".into()
            }
            _ => {
                if let connect_core::StreamTransport::Srt(endpoint) =
                    &mut definition.destinations[0].endpoint
                {
                    endpoint.latency = Some(1000);
                }
            }
        }
        let outcome = reconcile(vec![definition], &observed, &LinkKeys::for_tests());
        assert!(outcome.desired_by_node.values().all(Vec::is_empty));
        assert_eq!(outcome.streams[0].status, PathStatus::Pending);
        assert!(matches!(
            outcome.streams[0].conditions[0].reason,
            StreamConditionReason::TimedDeliveryUnsupported
                | StreamConditionReason::LatencyBudgetInfeasible
        ));
    }
}

#[test]
fn raw_two_path_requests_do_not_select_a_timed_merger() {
    let mut definition = stream();
    definition.destinations[0].delivery = None;
    let outcome = reconcile(vec![definition], &observed(), &LinkKeys::for_tests());
    let receiver = &outcome.hops_by_stream["timed"][1];
    assert!(receiver.merge_ingress.is_none());
    assert!(receiver.delivery.is_none());
    assert_eq!(
        outcome.streams[0].conditions[0].reason,
        StreamConditionReason::SinglePath
    );
}

fn report(hop: &DesiredHop) -> HopStatus {
    let flowing = || SocketStatus {
        peer: None,
        condition: LinkCondition::Flowing,
        resolved: None,
        stats: None,
    };
    HopStatus {
        id: hop.id.clone(),
        node_id: hop.node_id.clone(),
        revision: hop.revision(),
        state: HopState::Provisioned,
        ingress: flowing(),
        merge_ingress: hop.merge_ingress.as_ref().map(|_| flowing()),
        egresses: hop
            .egresses
            .iter()
            .map(|egress| EgressStatus {
                branch_id: egress.branch_id.clone(),
                status: flowing(),
            })
            .collect(),
        delivery: hop
            .delivery
            .as_ref()
            .filter(|timing| timing.role == DeliveryRole::Receiver)
            .map(|_| DeliveryReport {
                state: DeliveryState::Ready,
                selected_latency_ms: Some(500),
                timing_ready: true,
                redundancy_ready: true,
                paths: vec![
                    DeliveryPathReport {
                        qualified: true,
                        ..DeliveryPathReport::default()
                    };
                    2
                ],
                ..DeliveryReport::default()
            }),
    }
}

#[test]
fn timing_evidence_is_revision_bound_and_independent_of_redundancy() {
    let mut observed = observed();
    let initial = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    observed.hops = initial.hops_by_stream["timed"].iter().map(report).collect();
    let ready = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    assert_eq!(ready.streams[0].status, PathStatus::Flowing);
    let receiver = observed
        .hops
        .iter_mut()
        .find(|hop| hop.delivery.is_some())
        .unwrap();
    receiver.delivery.as_mut().unwrap().redundancy_ready = false;
    let degraded = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    assert_eq!(degraded.streams[0].status, PathStatus::Degraded);
    assert!(
        degraded.streams[0]
            .conditions
            .iter()
            .any(
                |condition| condition.condition_type == StreamConditionType::TimingReady
                    && condition.status == StreamConditionStatus::True
            )
    );
    let receiver = observed
        .hops
        .iter_mut()
        .find(|hop| hop.delivery.is_some())
        .unwrap();
    receiver.revision = "stale".into();
    let stale = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    assert!(stale.streams[0].destinations[0].delivery.is_none());
    assert_ne!(stale.streams[0].status, PathStatus::Flowing);
}

#[tokio::test]
async fn source_identity_survives_destination_updates_and_changes_after_recreation() {
    let store = MemStore::new();
    let definition = stream();
    let created = store.create_stream(&definition).await.unwrap();
    let mut changed = definition.clone();
    changed.destinations[0].paths = 1;
    let updated = store
        .update_stream(&changed, created.revision)
        .await
        .unwrap();
    assert_eq!(created.source_revision, updated.source_revision);
    let mut new_source = changed.clone();
    if let connect_core::StreamTransport::Srt(endpoint) = &mut new_source.source {
        endpoint.node = Some("other".into());
    }
    let replaced = store
        .update_stream(&new_source, updated.revision)
        .await
        .unwrap();
    assert_ne!(created.source_revision, replaced.source_revision);
    store
        .delete_stream("timed", replaced.revision)
        .await
        .unwrap();
    let recreated = store.create_stream(&definition).await.unwrap();
    assert_ne!(created.source_revision, recreated.source_revision);
    let restarted_store = MemStore::new();
    let after_restart = restarted_store.create_stream(&definition).await.unwrap();
    assert_ne!(created.source_revision, after_restart.source_revision);
}

#[test]
fn current_origin_clock_fault_overrides_a_ready_receiver() {
    let mut observed = observed();
    let planned = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    observed.hops = planned.hops_by_stream["timed"].iter().map(report).collect();
    observed.hops[0].delivery = Some(DeliveryReport {
        state: DeliveryState::Degraded,
        reason: connect_core::DeliveryReason::ClockUnavailable,
        ..DeliveryReport::default()
    });
    let failed = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    let destination = &failed.streams[0].destinations[0];
    assert_eq!(destination.status, PathStatus::Degraded);
    assert_eq!(
        destination.delivery.as_ref().unwrap().reason,
        connect_core::DeliveryReason::ClockUnavailable
    );
    observed.hops[0].revision = "stale".into();
    let recovered = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    assert_eq!(recovered.streams[0].status, PathStatus::Pending);
    assert!(
        recovered.streams[0].destinations[0]
            .delivery
            .as_ref()
            .unwrap()
            .timing_ready
    );
}

#[test]
fn persistence_tracks_timed_readiness_without_writing_every_counter_sample() {
    let observed = observed();
    let planned = reconcile(vec![stream()], &observed, &LinkKeys::for_tests());
    let node = observed.nodes[1].clone();
    let topology = DeclaredTopology {
        site: None,
        attachments: node.topology.attachments.clone(),
    };
    let registration = NodeRegistration {
        protocol_version: PROTOCOL_VERSION,
        node: node.with_topology(topology),
        observation: None,
        endpoints: vec![],
        hop_status: vec![report(&planned.hops_by_stream["timed"][1])],
    };
    let mut changed = registration.clone();
    changed.hop_status[0]
        .delivery
        .as_mut()
        .unwrap()
        .counters
        .released += 1;
    assert_eq!(reported(&registration), reported(&changed));
    changed.hop_status[0]
        .delivery
        .as_mut()
        .unwrap()
        .timing_ready = false;
    assert_ne!(reported(&registration), reported(&changed));
}

fn low_latency(max_latency_ms: u32) -> StreamDefinition {
    serde_json::from_value(serde_json::json!({
        "name":"talk", "source":{"srt":{"node":"source"}},
        "destinations":[{"id":"studio","delivery":{"mode":"low_latency","max_latency_ms":max_latency_ms},"srt":{"node":"studio"}}]
    })).unwrap()
}

fn latency(socket: &connect_core::SocketSpec) -> Option<u32> {
    match socket {
        connect_core::SocketSpec::Srt(socket) => socket.params().latency,
        _ => None,
    }
}

#[test]
fn low_latency_splits_its_budget_across_two_paths_without_requiring_them_or_a_shared_clock() {
    let mut observed = observed();
    observed.nodes[1].capabilities.hop_profiles[1]
        .timed_delivery
        .as_mut()
        .unwrap()
        .clock_domain = "different".into();
    let outcome = reconcile(vec![low_latency(150)], &observed, &LinkKeys::for_tests());
    let hops = &outcome.hops_by_stream["talk"];
    let origin = &hops[0];
    let receiver = hops
        .iter()
        .find(|hop| hop.role == connect_core::HopRole::Receiver)
        .unwrap();
    let timing = receiver.delivery.as_ref().unwrap();
    assert_eq!(timing.paths, 2);
    assert_eq!(
        timing.policy,
        Some(DeliveryPolicy::LowLatency {
            max_latency_ms: 150,
            max_skew_ms: None,
        })
    );
    assert_eq!(latency(&origin.ingress), Some(20));
    assert_eq!(latency(receiver.merge_ingress.as_ref().unwrap()), Some(100));
    assert_eq!(latency(&receiver.ingress), Some(100));
    assert_eq!(latency(&receiver.egresses[0].socket), Some(20));
    assert!(
        origin
            .egresses
            .iter()
            .all(|egress| latency(&egress.socket) == Some(100))
    );
    assert_eq!(origin.egresses.len(), 2);

    observed.hops = hops.iter().map(report).collect();
    let flowing = reconcile(vec![low_latency(150)], &observed, &LinkKeys::for_tests());
    assert_eq!(flowing.streams[0].status, PathStatus::Flowing);
    let receiver = observed
        .hops
        .iter_mut()
        .find(|hop| hop.delivery.is_some())
        .unwrap();
    receiver.delivery.as_mut().unwrap().redundancy_ready = false;
    receiver.delivery.as_mut().unwrap().paths[1].qualified = false;
    let one_path = reconcile(vec![low_latency(150)], &observed, &LinkKeys::for_tests());
    assert_eq!(one_path.streams[0].status, PathStatus::Flowing);
    let redundancy = one_path.streams[0].destinations[0]
        .conditions
        .iter()
        .find(|condition| condition.condition_type == StreamConditionType::RedundancyReady)
        .unwrap();
    assert_eq!(redundancy.status, StreamConditionStatus::True);
    assert!(redundancy.detail.starts_with("1 of 2 attempted paths"));
}

#[test]
fn an_aligned_destination_reports_its_hold_and_degrades_when_the_skew_outgrows_the_allowance() {
    let mut observed = observed();
    observed.nodes[1].capabilities.hop_profiles[1]
        .timed_delivery
        .as_mut()
        .unwrap()
        .clock_domain = "shared-host:other".into();
    let aligned: StreamDefinition = serde_json::from_value(serde_json::json!({
        "name":"talk", "source":{"srt":{"node":"source"}},
        "destinations":[{"id":"studio","paths":2,"delivery":{"mode":"low_latency","max_latency_ms":150,"max_skew_ms":60},"srt":{"node":"studio"}}]
    }))
    .unwrap();
    let outcome = reconcile(vec![aligned.clone()], &observed, &LinkKeys::for_tests());
    observed.hops = outcome.hops_by_stream["talk"].iter().map(report).collect();
    let receiver = observed
        .hops
        .iter_mut()
        .find(|hop| hop.delivery.is_some())
        .unwrap()
        .delivery
        .as_mut()
        .unwrap();
    receiver.skew_us = Some(42_000);
    receiver.hold_us = Some(42_000);
    let flowing = reconcile(vec![aligned.clone()], &observed, &LinkKeys::for_tests());
    assert_eq!(flowing.streams[0].status, PathStatus::Flowing);
    let condition = |outcome: &crate::ReconcileOutcome, kind| {
        outcome.streams[0].destinations[0]
            .conditions
            .iter()
            .find(|condition| condition.condition_type == kind)
            .unwrap()
            .clone()
    };
    assert_eq!(
        condition(&flowing, StreamConditionType::TimingReady).detail,
        "first-copy release within 150 ms, faster path held 42 ms"
    );
    let receiver = observed
        .hops
        .iter_mut()
        .find(|hop| hop.delivery.is_some())
        .unwrap()
        .delivery
        .as_mut()
        .unwrap();
    receiver.skew_us = Some(95_000);
    receiver.hold_us = Some(60_000);
    receiver.redundancy_ready = false;
    receiver.reason = connect_core::DeliveryReason::SkewExceeded;
    let skewed = reconcile(vec![aligned], &observed, &LinkKeys::for_tests());
    assert_eq!(skewed.streams[0].status, PathStatus::Degraded);
    let redundancy = condition(&skewed, StreamConditionType::RedundancyReady);
    assert_eq!(redundancy.status, StreamConditionStatus::False);
    assert_eq!(
        redundancy.detail,
        "the slower path lags 95 ms, more than max_skew_ms 60"
    );
}

#[test]
fn low_latency_places_one_path_when_a_second_cannot_be_kept_apart() {
    let mut observed = observed();
    observed.nodes[1].topology.attachments.truncate(1);
    let outcome = reconcile(vec![low_latency(150)], &observed, &LinkKeys::for_tests());
    let receiver = &outcome.hops_by_stream["talk"][1];
    assert!(receiver.merge_ingress.is_none());
    assert_eq!(receiver.delivery.as_ref().unwrap().paths, 2);
    assert_eq!(
        outcome.streams[0].conditions[0].reason,
        StreamConditionReason::SinglePath
    );
}

#[test]
fn low_latency_refuses_budgets_without_room_and_receivers_without_the_mode() {
    let outcome = reconcile(vec![low_latency(60)], &observed(), &LinkKeys::for_tests());
    assert_eq!(outcome.streams[0].status, PathStatus::Pending);
    assert_eq!(
        outcome.streams[0].conditions[0].reason,
        StreamConditionReason::LatencyBudgetInfeasible
    );

    let mut observed = observed();
    observed.nodes[1].capabilities.hop_profiles[1]
        .timed_delivery
        .as_mut()
        .unwrap()
        .modes
        .retain(|mode| *mode != DeliveryMode::LowLatency);
    let outcome = reconcile(vec![low_latency(150)], &observed, &LinkKeys::for_tests());
    assert_eq!(outcome.streams[0].status, PathStatus::Pending);
    assert_eq!(
        outcome.streams[0].conditions[0].reason,
        StreamConditionReason::TimedDeliveryUnsupported
    );
}
