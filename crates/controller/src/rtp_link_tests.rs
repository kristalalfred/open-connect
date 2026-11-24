use super::*;
use connect_core::{
    DeliveryCapability, DeliveryMode, DeliveryRole, FecParams, FormatConstraint, HopEndpointClass,
    HopProfile, HopRole, LinkClass, MediaBehavior, NodeCapabilities, NodeDescriptor, PayloadKind,
    RoleSet, RtpParams, RtpSocket, SocketSpec, Transport, TransportClass,
};

fn link(transport: Transport) -> TransportClass {
    TransportClass {
        transport,
        roles: RoleSet::both(),
    }
}

fn native_profiles(epoch: u64, rtp: bool) -> Vec<HopProfile> {
    let srt = || HopEndpointClass::Transport(link(Transport::Srt));
    let egress = || {
        if rtp {
            HopEndpointClass::AnyOf(vec![
                LinkClass::Transport(link(Transport::Srt)),
                LinkClass::Transport(link(Transport::Rtp)),
            ])
        } else {
            srt()
        }
    };
    let mut profiles = vec![
        ("srt-timed-origin", DeliveryRole::Origin, srt(), egress()),
        ("srt-timed-forward", DeliveryRole::Forward, srt(), egress()),
        ("srt-timed-receiver", DeliveryRole::Receiver, srt(), srt()),
    ];
    if rtp {
        let rtp = || HopEndpointClass::Transport(link(Transport::Rtp));
        profiles.push(("rtp-timed-forward", DeliveryRole::Forward, rtp(), egress()));
        profiles.push(("rtp-timed-receiver", DeliveryRole::Receiver, rtp(), srt()));
    }
    profiles
        .into_iter()
        .map(|(id, role, ingress, egress)| HopProfile {
            id: id.into(),
            ingress,
            egress,
            max_egresses: (role == DeliveryRole::Receiver).then_some(1),
            media_behavior: MediaBehavior::Preserve,
            merge: role == DeliveryRole::Receiver,
            accepts: Some(FormatConstraint::default()),
            timed_delivery: Some(DeliveryCapability {
                protocol: connect_core::delivery::TIMED_PROTOCOL.into(),
                roles: vec![role],
                modes: if role == DeliveryRole::Receiver {
                    vec![
                        DeliveryMode::Fixed,
                        DeliveryMode::Auto,
                        DeliveryMode::LowLatency,
                    ]
                } else {
                    vec![]
                },
                max_latency_ms: 60_000,
                scheduling_error_bound_us: 1_000,
                clock_domain: "shared-host:test".into(),
                origin_epoch: epoch,
            }),
        })
        .collect()
}

fn attachment(network: &str, listening: bool, base: u16) -> serde_json::Value {
    let mut attachment = serde_json::json!({"id": network, "network": network, "dial": true});
    if listening {
        attachment["listeners"] = serde_json::json!({
            "srt": {"host": "198.51.100.1", "port_range": {"start": base, "end": base + 99}},
            "rtp": {"host": "198.51.100.1", "port_range": {"start": base + 100, "end": base + 199}},
        });
    }
    attachment
}

fn node(id: &str, epoch: u64, attachments: Vec<serde_json::Value>, rtp: bool) -> NodeDescriptor {
    let mut node: NodeDescriptor = serde_json::from_value(serde_json::json!({
        "id": id, "endpoint": "http://127.0.0.1", "status": "ready",
        "topology": {"attachments": attachments}
    }))
    .unwrap();
    node.capabilities = NodeCapabilities {
        hop_profiles: native_profiles(epoch, rtp),
    };
    node
}

fn lan(base: u16) -> serde_json::Value {
    serde_json::json!({"id": "lan", "network": "lan", "listeners": {
        "srt": {"host": "127.0.0.1", "port_range": {"start": base, "end": base + 99}}
    }})
}

fn dialing(id: &str, epoch: u64, rtp: bool) -> NodeDescriptor {
    node(
        id,
        epoch,
        vec![
            lan(30_000),
            attachment("fiber", false, 0),
            attachment("cellular", false, 0),
        ],
        rtp,
    )
}

fn direct(rtp: bool) -> ObservedState {
    ObservedState {
        nodes: vec![
            dialing("source", 11, rtp),
            node(
                "studio",
                22,
                vec![
                    lan(31_000),
                    attachment("fiber", true, 20_000),
                    attachment("cellular", true, 21_000),
                ],
                rtp,
            ),
        ],
        hops: vec![],
        endpoints: vec![],
    }
}

fn relayed() -> ObservedState {
    ObservedState {
        nodes: vec![
            dialing("source", 11, true),
            node(
                "relay-fiber",
                33,
                vec![attachment("fiber", true, 22_000)],
                true,
            ),
            node(
                "relay-cellular",
                44,
                vec![attachment("cellular", true, 23_000)],
                true,
            ),
            dialing("studio", 22, true),
        ],
        hops: vec![],
        endpoints: vec![],
    }
}

fn stream(delivery: serde_json::Value, studio: serde_json::Value) -> StreamDefinition {
    serde_json::from_value(serde_json::json!({
        "name": "talk", "source": {"srt": {"node": "source"}},
        "destinations": [{"id": "on-air", "delivery": delivery, "srt": studio}]
    }))
    .unwrap()
}

fn low_latency() -> StreamDefinition {
    stream(
        serde_json::json!({"mode": "low_latency", "max_latency_ms": 160, "max_skew_ms": 80}),
        serde_json::json!({"node": "studio"}),
    )
}

fn plan(definition: StreamDefinition, observed: &ObservedState) -> Vec<DesiredHop> {
    let outcome = reconcile(vec![definition], observed, &LinkKeys::for_tests());
    outcome
        .hops_by_stream
        .get("talk")
        .cloned()
        .unwrap_or_else(|| panic!("unplaced: {:?}", outcome.streams[0].conditions))
}

fn receiver(hops: &[DesiredHop]) -> &DesiredHop {
    hops.iter()
        .find(|hop| hop.role == HopRole::Receiver)
        .unwrap()
}

fn rtp(socket: &SocketSpec) -> &RtpParams {
    match socket {
        SocketSpec::Rtp(socket) => socket.params(),
        other => panic!("expected an rtp link, got {other}"),
    }
}

#[test]
fn low_latency_links_between_native_nodes_carry_encrypted_rtp_with_fec_and_the_receiver_waits_for_recovery()
 {
    let hops = plan(low_latency(), &direct(true));
    let receiver = receiver(&hops);
    assert_eq!(receiver.profile_id, "rtp-timed-receiver");
    for socket in [&receiver.ingress, receiver.merge_ingress.as_ref().unwrap()] {
        let params = rtp(socket);
        assert_eq!(params.payload, PayloadKind::ConnectTimedV1);
        assert!(params.passphrase.is_some());
        assert_eq!(params.fec, FecParams::default());
    }
    let origin = &hops[0];
    assert_eq!(origin.profile_id, "srt-timed-origin");
    assert_eq!(origin.egresses.len(), 2);
    assert!(
        origin
            .egresses
            .iter()
            .all(|egress| matches!(egress.socket, SocketSpec::Rtp(RtpSocket::Connect { .. })))
    );
    assert!(matches!(origin.ingress, SocketSpec::Srt(_)));
    assert!(matches!(receiver.egresses[0].socket, SocketSpec::Srt(_)));
    assert_eq!(receiver.delivery.as_ref().unwrap().wait_ms, Some(10 + 20));
}

#[test]
fn a_relayed_path_waits_for_recovery_on_every_link() {
    let hops = plan(low_latency(), &relayed());
    let receiver = receiver(&hops);
    assert_eq!(receiver.profile_id, "rtp-timed-receiver");
    assert_eq!(
        receiver.delivery.as_ref().unwrap().wait_ms,
        Some(10 + 2 * 20)
    );
    let bridges: Vec<&DesiredHop> = hops
        .iter()
        .filter(|hop| hop.role == HopRole::Bridge)
        .collect();
    assert_eq!(bridges.len(), 2);
    for bridge in bridges {
        assert_eq!(bridge.profile_id, "rtp-timed-forward");
        assert!(
            matches!(bridge.ingress, SocketSpec::Rtp(RtpSocket::Listen { port, .. }) if port >= 22_100)
        );
        assert_eq!(
            rtp(&bridge.egresses[0].socket).payload,
            PayloadKind::ConnectTimedV1
        );
    }
}

#[test]
fn fixed_delivery_and_an_explicit_srt_latency_keep_srt_links() {
    let fixed = stream(
        serde_json::json!({"mode": "fixed", "latency_ms": 500}),
        serde_json::json!({"node": "studio", "latency": 100}),
    );
    let explicit = stream(
        serde_json::json!({"mode": "low_latency", "max_latency_ms": 400}),
        serde_json::json!({"node": "studio", "latency": 100}),
    );
    for definition in [fixed, explicit] {
        let hops = plan(definition, &direct(true));
        let receiver = receiver(&hops);
        assert_eq!(receiver.profile_id, "srt-timed-receiver");
        assert!(matches!(receiver.ingress, SocketSpec::Srt(_)));
        assert_eq!(receiver.delivery.as_ref().unwrap().wait_ms, None);
    }
}

#[test]
fn nodes_without_rtp_links_keep_the_srt_budget_split() {
    let hops = plan(low_latency(), &direct(false));
    let receiver = receiver(&hops);
    assert_eq!(receiver.profile_id, "srt-timed-receiver");
    match &receiver.ingress {
        SocketSpec::Srt(socket) => assert_eq!(socket.params().latency, Some(110)),
        other => panic!("expected srt, got {other}"),
    }
    assert_eq!(receiver.delivery.as_ref().unwrap().wait_ms, None);
}

#[test]
fn a_budget_that_cannot_hold_the_recovery_wait_is_refused() {
    let tight = stream(
        serde_json::json!({"mode": "low_latency", "max_latency_ms": 85}),
        serde_json::json!({"node": "studio"}),
    );
    let outcome = reconcile(vec![tight], &relayed(), &LinkKeys::for_tests());
    assert!(!outcome.hops_by_stream.contains_key("talk"));
}
