use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use connect_core::{
    DeliveryReason, DeliveryReport, DeliveryRole, DeliveryState, DesiredHop, EgressStatus,
    HopState, HopStatus, LocalEnd, LocalSocket, PayloadKind, SocketSpec, SocketStatus,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::endpoint::Endpoint;
use crate::merge::{BufferLimits, Merger};
use crate::record::{MAX_PAYLOAD, Record};
use crate::rtp::Rtp;
use crate::transport::Input;

pub const SRT_FORWARD: &str = "srt-forward";
pub const RTP_TO_SRT: &str = "rtp-to-srt";
pub const SRT_TO_RTP: &str = "srt-to-rtp";

#[derive(Clone)]
pub struct Settings {
    pub clock: Clock,
    pub limits: BufferLimits,
    pub bind_ip: IpAddr,
    pub rtp: Arc<Rtp>,
}

pub struct Hop {
    desired: DesiredHop,
    updates: watch::Sender<DesiredHop>,
    pub report: Arc<Mutex<HopStatus>>,
    task: JoinHandle<()>,
}

#[derive(Default)]
pub struct OriginState {
    identity: Option<([u8; 16], u64)>,
    sequence: u64,
    previous_origin_ns: Option<u64>,
}

fn initial_report(desired: &DesiredHop) -> HopStatus {
    HopStatus {
        id: desired.id.clone(),
        node_id: desired.node_id.clone(),
        revision: desired.revision(),
        state: HopState::Pending,
        ingress: SocketStatus {
            peer: None,
            condition: connect_core::LinkCondition::Connecting,
            resolved: None,
            stats: None,
        },
        merge_ingress: None,
        egresses: vec![],
        delivery: None,
    }
}

impl Hop {
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub fn start(
        desired: DesiredHop,
        settings: Settings,
        origin: Arc<Mutex<OriginState>>,
    ) -> anyhow::Result<Self> {
        validate(&desired, &settings)?;
        let report = Arc::new(Mutex::new(initial_report(&desired)));
        let (updates, receive) = watch::channel(desired.clone());
        let shared = report.clone();
        let task = tokio::spawn(async move { run(receive, shared, settings, origin).await });
        Ok(Self {
            desired,
            updates,
            report,
            task,
        })
    }

    pub fn update(&mut self, desired: DesiredHop, settings: &Settings) -> anyhow::Result<()> {
        validate(&desired, settings)?;
        if self.desired != desired {
            self.desired = desired.clone();
            self.updates.send_replace(desired);
        }
        anyhow::ensure!(
            !self.task.is_finished(),
            "media actor exited and requires a new origin epoch"
        );
        Ok(())
    }
}

impl Drop for Hop {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn validate(desired: &DesiredHop, settings: &Settings) -> anyhow::Result<()> {
    let clock = &settings.clock;
    anyhow::ensure!(
        !desired.egresses.is_empty(),
        "native hop requires an output"
    );
    let raw = |socket: &SocketSpec| matches!(socket, SocketSpec::Srt(socket) if socket.params().payload == PayloadKind::Raw);
    if let Some(timing) = &desired.delivery {
        anyhow::ensure!(
            desired
                .sockets()
                .all(|socket| matches!(socket, SocketSpec::Srt(_))),
            "native timed hops only carry SRT"
        );
        let (profile, role) = match timing.role {
            DeliveryRole::Origin => ("srt-timed-origin", connect_core::HopRole::Sender),
            DeliveryRole::Forward => ("srt-timed-forward", connect_core::HopRole::Bridge),
            DeliveryRole::Receiver => ("srt-timed-receiver", connect_core::HopRole::Receiver),
        };
        anyhow::ensure!(
            desired.profile_id == profile && desired.role == role && timing.epoch != 0,
            "invalid native timed profile, role or epoch"
        );
        let framed = |socket: &SocketSpec| matches!(socket, SocketSpec::Srt(socket) if socket.params().payload == PayloadKind::ConnectTimedV1 && socket.params().passphrase.is_some());
        match timing.role {
            DeliveryRole::Origin => anyhow::ensure!(
                raw(&desired.ingress) && desired.merge_ingress.is_none(),
                "origin requires one raw ingress"
            ),
            DeliveryRole::Forward => anyhow::ensure!(
                framed(&desired.ingress)
                    && desired.merge_ingress.is_none()
                    && desired.egresses.iter().all(|egress| framed(&egress.socket)),
                "forwarder requires encrypted timed sockets"
            ),
            DeliveryRole::Receiver => anyhow::ensure!(
                framed(&desired.ingress)
                    && desired.merge_ingress.as_ref().is_none_or(framed)
                    && desired.egresses.len() == 1
                    && raw(&desired.egresses[0].socket)
                    && usize::from(timing.paths) > usize::from(desired.merge_ingress.is_some()),
                "receiver requires encrypted timed inputs and one raw output"
            ),
        }
        anyhow::ensure!(
            desired
                .egresses
                .iter()
                .all(|egress| raw(&egress.socket) || framed(&egress.socket)),
            "timed outputs require encryption"
        );
        anyhow::ensure!(
            timing.role == DeliveryRole::Forward || timing.clock_domain == clock.domain(),
            "timed hop clock domain does not match the node"
        );
        anyhow::ensure!(
            (1..=2).contains(&timing.paths),
            "invalid requested timed path count"
        );
        anyhow::ensure!(
            timing.role != DeliveryRole::Receiver
                || timing
                    .policy
                    .as_ref()
                    .is_some_and(|policy| policy.validate().is_empty()),
            "timed receiver requires a valid delivery policy"
        );
    } else {
        let input = |socket: &SocketSpec| matches!(socket, SocketSpec::Local(LocalSocket { end: LocalEnd::Input, id }) if settings.rtp.has_input(id));
        let output = |socket: &SocketSpec| matches!(socket, SocketSpec::Local(LocalSocket { end: LocalEnd::Output, id }) if settings.rtp.has_output(id));
        let egresses_raw = desired.egresses.iter().all(|egress| raw(&egress.socket));
        match desired.profile_id.as_str() {
            SRT_FORWARD => anyhow::ensure!(
                raw(&desired.ingress) && desired.merge_ingress.is_none() && egresses_raw,
                "SRT forwarding takes one raw SRT input and raw SRT outputs"
            ),
            RTP_TO_SRT => anyhow::ensure!(
                input(&desired.ingress) && desired.merge_ingress.is_none() && egresses_raw,
                "an RTP sender takes one configured RTP input and raw SRT outputs"
            ),
            SRT_TO_RTP => anyhow::ensure!(
                raw(&desired.ingress)
                    && desired.merge_ingress.as_ref().is_none_or(raw)
                    && desired.egresses.len() == 1
                    && output(&desired.egresses[0].socket),
                "an RTP receiver takes raw SRT inputs and one configured RTP output"
            ),
            other => anyhow::bail!("unknown native profile {other}"),
        }
    }
    Ok(())
}

struct Actor {
    settings: Settings,
    desired: DesiredHop,
    inputs: BTreeMap<usize, Endpoint>,
    outputs: BTreeMap<String, Endpoint>,
    merger: Option<Merger>,
    origin: Arc<Mutex<OriginState>>,
    origin_report: DeliveryReport,
}

impl Actor {
    fn configure(&mut self, desired: DesiredHop, incoming: &mpsc::Sender<Input>) {
        let limits = self.settings.limits;
        let old_identity = self
            .desired
            .delivery
            .as_ref()
            .map(|timing| (timing.delivery_id, timing.epoch));
        let new_identity = desired
            .delivery
            .as_ref()
            .map(|timing| (timing.delivery_id, timing.epoch));
        if old_identity != new_identity {
            self.merger = None;
            self.origin_report = DeliveryReport::default();
        }
        let count = 1 + usize::from(desired.merge_ingress.is_some());
        if let Some(timing) = desired
            .delivery
            .as_ref()
            .filter(|timing| timing.role == DeliveryRole::Receiver)
        {
            let policy = timing.policy.clone().unwrap();
            if let Some(merger) = &mut self.merger {
                merger.reconfigure(policy, count, usize::from(timing.paths));
            } else {
                self.merger = Some(Merger::new(
                    policy,
                    count,
                    usize::from(timing.paths),
                    limits,
                ));
            }
        } else {
            self.merger = None;
        }
        self.inputs.retain(|path, _| *path < count);
        for (path, spec) in std::iter::once(&desired.ingress)
            .chain(desired.merge_ingress.iter())
            .enumerate()
        {
            if self
                .inputs
                .get(&path)
                .is_none_or(|input| !input.matches(spec))
            {
                self.inputs.remove(&path);
                self.inputs.insert(
                    path,
                    Endpoint::input(spec, path, incoming.clone(), &self.settings),
                );
                if let Some(merger) = &mut self.merger {
                    merger.reset_path(path);
                }
            }
        }
        self.outputs.retain(|id, _| {
            desired
                .egresses
                .iter()
                .any(|egress| &egress.branch_id == id)
        });
        for egress in &desired.egresses {
            if self
                .outputs
                .get(&egress.branch_id)
                .is_none_or(|output| !output.matches(&egress.socket))
            {
                self.outputs.remove(&egress.branch_id);
                self.outputs.insert(
                    egress.branch_id.clone(),
                    Endpoint::output(&egress.socket, &self.settings),
                );
            }
        }
        self.desired = desired;
    }

    fn accept(&mut self, input: Input) {
        let clock = &self.settings.clock;
        let Some(timing) = &self.desired.delivery else {
            let now = clock.monotonic_ns();
            for output in self.outputs.values() {
                let _ = output.try_send(input.bytes.clone(), now);
            }
            return;
        };
        let timing = timing.clone();
        match timing.role {
            DeliveryRole::Origin => {
                let mut origin = self.origin.lock().unwrap();
                if origin.identity != Some((timing.delivery_id, timing.epoch)) {
                    *origin = OriginState {
                        identity: Some((timing.delivery_id, timing.epoch)),
                        ..OriginState::default()
                    };
                }
                let stamp = match input.stamp {
                    Ok(stamp)
                        if origin
                            .previous_origin_ns
                            .is_none_or(|previous| stamp.tai_ns >= previous) =>
                    {
                        stamp
                    }
                    failed => {
                        let reason = failed.err().unwrap_or(DeliveryReason::ClockDiscontinuity);
                        self.origin_report.state = DeliveryState::Degraded;
                        self.origin_report.reason = reason;
                        for output in self
                            .outputs
                            .values()
                            .filter(|output| output.payload() == PayloadKind::Raw)
                        {
                            let _ = output.try_send(input.bytes.clone(), clock.monotonic_ns());
                        }
                        return;
                    }
                };
                origin.previous_origin_ns = Some(stamp.tai_ns);
                for output in self
                    .outputs
                    .values()
                    .filter(|output| output.payload() == PayloadKind::Raw)
                {
                    let _ = output.try_send(input.bytes.clone(), clock.monotonic_ns());
                }
                for chunk in input.bytes.chunks(MAX_PAYLOAD) {
                    let Some(next) = origin.sequence.checked_add(1) else {
                        self.origin_report.state = DeliveryState::Failed;
                        return;
                    };
                    let record = Record {
                        delivery_id: timing.delivery_id,
                        epoch: timing.epoch,
                        sequence: origin.sequence,
                        origin_tai_ns: stamp.tai_ns,
                        origin_uncertainty_ns: stamp.uncertainty_ns,
                        payload: Bytes::copy_from_slice(chunk),
                    };
                    origin.sequence = next;
                    let encoded = record.encode().expect("origin chunk fits record");
                    for output in self
                        .outputs
                        .values()
                        .filter(|output| output.payload() == PayloadKind::ConnectTimedV1)
                    {
                        let _ = output.try_send(encoded.clone(), clock.monotonic_ns());
                    }
                }
                self.origin_report.state = DeliveryState::Ready;
                self.origin_report.reason = DeliveryReason::Timely;
                self.origin_report.clock_uncertainty_us =
                    Some(stamp.uncertainty_ns.div_ceil(1_000));
            }
            DeliveryRole::Forward => {
                if Record::decode(input.bytes.clone(), timing.delivery_id, timing.epoch).is_err() {
                    self.origin_report.counters.invalid_records += 1;
                    return;
                }
                for output in self.outputs.values() {
                    let _ = output.try_send(input.bytes.clone(), clock.monotonic_ns());
                }
            }
            DeliveryRole::Receiver => {
                let merger = self.merger.as_mut().unwrap();
                match (
                    Record::decode(input.bytes, timing.delivery_id, timing.epoch),
                    input.stamp,
                ) {
                    (Ok(record), Ok(stamp)) => merger.ingest(record, input.path, stamp),
                    (_, Err(reason)) => merger.suspend(reason, clock.monotonic_ns()),
                    (Err(_), _) => merger.invalid_record(),
                }
            }
        }
    }

    async fn release(&mut self) {
        let clock = &self.settings.clock;
        if self
            .desired
            .delivery
            .as_ref()
            .is_some_and(|timing| timing.role == DeliveryRole::Origin)
            && let Err(reason) = clock.sample()
        {
            self.origin_report.state = DeliveryState::Degraded;
            self.origin_report.reason = reason;
            self.origin_report.clock_uncertainty_us = None;
        }
        let Some(merger) = &mut self.merger else {
            return;
        };
        let sample = match clock.sample() {
            Ok(sample) => sample,
            Err(reason) => {
                merger.suspend(reason, clock.monotonic_ns());
                return;
            }
        };
        merger.calibrate(sample);
        let output_latency_ns = self
            .outputs
            .values()
            .map(Endpoint::latency)
            .min()
            .unwrap_or_default()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        let mut submitted = 0;
        loop {
            let current = match clock.sample() {
                Ok(current) => current,
                Err(reason) => {
                    merger.suspend(reason, clock.monotonic_ns());
                    break;
                }
            };
            let Some(due) = merger.take_due(current, output_latency_ns) else {
                break;
            };
            let mut result = Ok(());
            for output in self.outputs.values() {
                if let Err(reason) = output.try_send_at(
                    due.record.payload.clone(),
                    clock.instant(due.target_monotonic_ns),
                    clock.monotonic_ns(),
                ) {
                    result = Err(reason);
                }
            }
            match (result, clock.sample()) {
                (Ok(()), Ok(after)) => merger.released(&due.record, after),
                (Err(reason), _) | (_, Err(reason)) => {
                    merger.output_failed(reason, clock.monotonic_ns())
                }
            }
            submitted += 1;
            if submitted % 32 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    fn report(&mut self, now_ns: u64) -> HopStatus {
        let drops = self
            .inputs
            .values()
            .chain(self.outputs.values())
            .map(Endpoint::take_drops)
            .fold(0_u64, u64::saturating_add);
        if let Some(merger) = &mut self.merger {
            merger.transport_dropped(drops);
        } else {
            self.origin_report.counters.buffer_drops = self
                .origin_report
                .counters
                .buffer_drops
                .saturating_add(drops);
        }
        let states: Vec<_> = self
            .inputs
            .values()
            .chain(self.outputs.values())
            .map(Endpoint::state)
            .collect();
        let state = if states.contains(&HopState::Failed) {
            HopState::Failed
        } else if states.contains(&HopState::Pending) {
            HopState::Pending
        } else {
            HopState::Provisioned
        };
        HopStatus {
            id: self.desired.id.clone(),
            node_id: self.desired.node_id.clone(),
            revision: self.desired.revision(),
            state,
            ingress: self.inputs[&0].status(now_ns),
            merge_ingress: self.inputs.get(&1).map(|input| input.status(now_ns)),
            egresses: self
                .outputs
                .iter()
                .map(|(id, output)| EgressStatus {
                    branch_id: id.clone(),
                    status: output.status(now_ns),
                })
                .collect(),
            delivery: self.desired.delivery.as_ref().map(|_| {
                self.merger.as_ref().map_or_else(
                    || self.origin_report.clone(),
                    |merger| merger.report(now_ns),
                )
            }),
        }
    }
}

async fn run(
    mut updates: watch::Receiver<DesiredHop>,
    report: Arc<Mutex<HopStatus>>,
    settings: Settings,
    origin: Arc<Mutex<OriginState>>,
) {
    let clock = settings.clock.clone();
    let desired = updates.borrow_and_update().clone();
    let mut actor = Actor {
        settings,
        desired: desired.clone(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        merger: None,
        origin,
        origin_report: DeliveryReport::default(),
    };
    let (incoming, mut receive) = mpsc::channel(256);
    actor.configure(desired, &incoming);
    let mut publish = tokio::time::interval(Duration::from_millis(100));
    publish.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let now_ns = clock.monotonic_ns();
        let wait_ns = actor
            .merger
            .as_ref()
            .and_then(Merger::next_deadline_ns)
            .map_or(100_000_000, |deadline| deadline.saturating_sub(now_ns))
            .min(100_000_000);
        tokio::select! {
            biased;
            changed = updates.changed() => {
                if changed.is_err() { return; }
                let desired = updates.borrow_and_update().clone();
                actor.configure(desired, &incoming);
            }
            _ = tokio::time::sleep(Duration::from_nanos(wait_ns)) => actor.release().await,
            _ = publish.tick() => { actor.release().await; *report.lock().unwrap() = actor.report(clock.monotonic_ns()); }
            Some(input) = receive.recv() => { actor.accept(input); actor.release().await; tokio::task::yield_now().await; }
        }
    }
}

#[cfg(test)]
mod tests {
    use connect_core::{
        DeliveryPolicy, DesiredEgress, HopRole, Passphrase, SrtParams, SrtSocket as SrtSpec,
        TimedHop,
    };
    use futures::{SinkExt, StreamExt};
    use srt_tokio::SrtSocket;

    use super::*;
    use crate::clock::ClockConfig;
    use crate::rtp::{RtpConfig, RtpInput, RtpOutput};

    static NETWORK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn srt_carries_bursty_transport_sized_messages_after_idle() {
        let _network = NETWORK.lock().await;
        let port = ports()[0];
        let listen = SrtSocket::builder()
            .latency(Duration::from_millis(200))
            .listen_on(std::net::SocketAddr::from(([127, 0, 0, 1], port)));
        let call = SrtSocket::builder()
            .latency(Duration::from_millis(200))
            .set(|options| {
                options.sender.buffer_size = srt_tokio::options::ByteCount(8 * 1024 * 1024)
            })
            .call(std::net::SocketAddr::from(([127, 0, 0, 1], port)), None);
        let (receiver, sender) = tokio::join!(listen, call);
        let mut receiver = receiver.unwrap();
        let mut sender = sender.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let send = async {
            for sequence in 0..500_u64 {
                let mut payload = vec![0_u8; 1316];
                payload[..8].copy_from_slice(&sequence.to_be_bytes());
                sender
                    .send((std::time::Instant::now(), payload.into()))
                    .await
                    .unwrap();
                if sequence % 100 == 99 {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            }
        };
        let receive = async {
            for sequence in 0..500_u64 {
                let (_, bytes) = tokio::time::timeout(Duration::from_secs(3), receiver.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(&bytes[..8], &sequence.to_be_bytes());
            }
        };
        tokio::join!(send, receive);
    }

    fn settings(clock: &Clock) -> Settings {
        with_rtp(clock, RtpConfig::default())
    }

    fn with_rtp(clock: &Clock, rtp: RtpConfig) -> Settings {
        Settings {
            clock: clock.clone(),
            limits: BufferLimits::default(),
            bind_ip: "127.0.0.1".parse().unwrap(),
            rtp: Arc::new(Rtp::new(&rtp)),
        }
    }

    fn ports() -> [u16; 4] {
        let sockets: Vec<_> = (0..4)
            .map(|_| std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
            .collect();
        std::array::from_fn(|index| sockets[index].local_addr().unwrap().port())
    }

    fn params(timed: bool, latency: u32) -> SrtParams {
        SrtParams {
            payload: if timed {
                PayloadKind::ConnectTimedV1
            } else {
                PayloadKind::Raw
            },
            latency: Some(latency),
            passphrase: timed.then(|| Passphrase::new("timed-native-test-key")),
            pbkeylen: timed.then_some(32),
        }
    }

    fn pair(policy: DeliveryPolicy, ports: [u16; 4]) -> (DesiredHop, DesiredHop) {
        let timing = TimedHop {
            delivery_id: [9; 16],
            epoch: 11,
            clock_domain: "shared-host:test".into(),
            role: DeliveryRole::Origin,
            policy: None,
            paths: 1,
        };
        let source = DesiredHop {
            id: "sender".into(),
            node_id: "source".into(),
            profile_id: "srt-timed-origin".into(),
            role: HopRole::Sender,
            ingress: SocketSpec::Srt(SrtSpec::Listen {
                port: ports[0],
                params: params(false, 10),
            }),
            merge_ingress: None,
            egresses: [("studio", ports[1], 10), ("studio.2", ports[2], 100)]
                .into_iter()
                .map(|(id, port, latency)| DesiredEgress {
                    branch_id: id.into(),
                    socket: SocketSpec::Srt(SrtSpec::Connect {
                        host: "127.0.0.1".into(),
                        port,
                        params: params(true, latency),
                    }),
                })
                .collect(),
            tracks: None,
            delivery: Some(timing.clone()),
        };
        let receiver = DesiredHop {
            id: "receiver".into(),
            node_id: "studio".into(),
            profile_id: "srt-timed-receiver".into(),
            role: HopRole::Receiver,
            ingress: SocketSpec::Srt(SrtSpec::Listen {
                port: ports[1],
                params: params(true, 10),
            }),
            merge_ingress: Some(SocketSpec::Srt(SrtSpec::Listen {
                port: ports[2],
                params: params(true, 100),
            })),
            egresses: vec![DesiredEgress {
                branch_id: "studio".into(),
                socket: SocketSpec::Srt(SrtSpec::Listen {
                    port: ports[3],
                    params: params(false, 10),
                }),
            }],
            tracks: None,
            delivery: Some(TimedHop {
                role: DeliveryRole::Receiver,
                policy: Some(policy),
                paths: 2,
                ..timing
            }),
        };
        (source, receiver)
    }

    async fn peers(ports: [u16; 4]) -> (SrtSocket, SrtSocket) {
        let producer = SrtSocket::builder()
            .latency(Duration::from_millis(10))
            .call(format!("127.0.0.1:{}", ports[0]), None);
        let consumer = SrtSocket::builder()
            .latency(Duration::from_millis(10))
            .call(format!("127.0.0.1:{}", ports[3]), None);
        let (producer, consumer) = tokio::join!(producer, consumer);
        (producer.unwrap(), consumer.unwrap())
    }

    #[tokio::test]
    async fn encrypted_redundant_media_keeps_its_bytes_and_schedule_after_one_path_is_removed() {
        let _network = NETWORK.lock().await;
        let ports = ports();
        let clock = Clock::new(ClockConfig::SharedHost {
            domain: "test".into(),
            tai_offset_secs: 37,
        });
        let (source, receiver) = pair(
            DeliveryPolicy::Fixed {
                latency_ms: 200,
                tolerance_ms: 20,
            },
            ports,
        );
        let mut origin = Hop::start(source.clone(), settings(&clock), Arc::default()).unwrap();
        let receiver = Hop::start(receiver, settings(&clock), Arc::default()).unwrap();
        let (mut producer, mut consumer) = peers(ports).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let sending = async {
            for sequence in 0..200_u64 {
                if sequence == 100 {
                    let mut changed = source.clone();
                    changed.egresses.remove(0);
                    origin.update(changed, &settings(&clock)).unwrap();
                }
                producer
                    .send((
                        std::time::Instant::now(),
                        Bytes::copy_from_slice(&sequence.to_be_bytes()),
                    ))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let receiving = async {
            for expected in 0..200_u64 {
                let (_, payload) = tokio::time::timeout(Duration::from_secs(4), consumer.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(payload.as_ref(), &expected.to_be_bytes());
            }
        };
        tokio::join!(sending, receiving);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let report = receiver.report.lock().unwrap().delivery.clone().unwrap();
        assert_eq!(report.counters.released, 200);
        assert_eq!(report.counters.unrecovered, 0);
        assert!(report.counters.duplicates > 0);
        assert!(report.minimum_release_age_us.unwrap() >= 180_000);
        assert!(report.maximum_release_age_us.unwrap() <= 220_000);
    }

    #[tokio::test]
    async fn automatic_real_srt_calibration_uses_the_slower_path_and_keeps_the_selected_delay() {
        let _network = NETWORK.lock().await;
        let ports = ports();
        let clock = Clock::new(ClockConfig::SharedHost {
            domain: "test".into(),
            tai_offset_secs: 37,
        });
        let policy = DeliveryPolicy::Auto {
            min_latency_ms: 40,
            max_latency_ms: 300,
            tolerance_ms: 20,
            calibration_ms: 1000,
            max_late_ppm: 10000,
            margin_ms: 20,
        };
        let (source, receiver) = pair(policy, ports);
        let _origin = Hop::start(source, settings(&clock), Arc::default()).unwrap();
        let receiver = Hop::start(receiver, settings(&clock), Arc::default()).unwrap();
        let (mut producer, mut consumer) = peers(ports).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let sending = async {
            for sequence in 0..700_u64 {
                producer
                    .send((
                        std::time::Instant::now(),
                        Bytes::copy_from_slice(&sequence.to_be_bytes()),
                    ))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        };
        let receiving = async {
            let mut previous = None;
            for _ in 0..100 {
                let (_, payload) = tokio::time::timeout(Duration::from_secs(5), consumer.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let sequence = u64::from_be_bytes(payload[..].try_into().unwrap());
                assert!(previous.is_none_or(|previous| sequence == previous + 1));
                previous = Some(sequence);
            }
        };
        tokio::join!(sending, receiving);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let selected = receiver
            .report
            .lock()
            .unwrap()
            .delivery
            .as_ref()
            .unwrap()
            .selected_latency_ms
            .unwrap();
        assert!((120..=300).contains(&selected), "selected {selected} ms");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            receiver
                .report
                .lock()
                .unwrap()
                .delivery
                .as_ref()
                .unwrap()
                .selected_latency_ms,
            Some(selected)
        );
    }

    #[tokio::test]
    async fn rtp_crosses_two_srt_paths_once_and_survives_losing_the_fast_one() {
        let _network = NETWORK.lock().await;
        let ports = ports();
        let [input_port, output_port, ..] = super::tests::ports();
        let clock = Clock::new(ClockConfig::SharedHost {
            domain: "test".into(),
            tai_offset_secs: 37,
        });
        let settings = with_rtp(
            &clock,
            RtpConfig {
                inputs: vec![RtpInput {
                    id: "mic".into(),
                    label: "Mic".into(),
                    tracks: vec![connect_core::Track::Audio],
                    address: std::net::Ipv4Addr::LOCALHOST,
                    port: input_port,
                    source: None,
                    interface: None,
                }],
                outputs: vec![RtpOutput {
                    id: "bus".into(),
                    label: "Bus".into(),
                    address: std::net::Ipv4Addr::LOCALHOST,
                    port: output_port,
                    interface: None,
                    ttl: None,
                }],
            },
        );
        let keyed = |latency| SrtParams {
            payload: PayloadKind::Raw,
            latency: Some(latency),
            passphrase: Some(Passphrase::new("rtp-native-test-key")),
            pbkeylen: Some(32),
        };
        let sender = DesiredHop {
            id: "sender".into(),
            node_id: "venue".into(),
            profile_id: RTP_TO_SRT.into(),
            role: HopRole::Sender,
            ingress: SocketSpec::input("mic"),
            merge_ingress: None,
            egresses: [("studio", ports[1], 10), ("studio.2", ports[2], 100)]
                .into_iter()
                .map(|(id, port, latency)| DesiredEgress {
                    branch_id: id.into(),
                    socket: SocketSpec::Srt(SrtSpec::Connect {
                        host: "127.0.0.1".into(),
                        port,
                        params: keyed(latency),
                    }),
                })
                .collect(),
            tracks: Some(vec![connect_core::Track::Audio]),
            delivery: None,
        };
        let receiver = DesiredHop {
            id: "receiver".into(),
            node_id: "studio".into(),
            profile_id: SRT_TO_RTP.into(),
            role: HopRole::Receiver,
            ingress: SocketSpec::Srt(SrtSpec::Listen {
                port: ports[1],
                params: keyed(10),
            }),
            merge_ingress: Some(SocketSpec::Srt(SrtSpec::Listen {
                port: ports[2],
                params: keyed(100),
            })),
            egresses: vec![DesiredEgress {
                branch_id: "studio".into(),
                socket: SocketSpec::output("bus"),
            }],
            tracks: Some(vec![connect_core::Track::Audio]),
            delivery: None,
        };
        let mut origin = Hop::start(sender.clone(), settings.clone(), Arc::default()).unwrap();
        let receiver = Hop::start(receiver, settings.clone(), Arc::default()).unwrap();
        let consumer = tokio::net::UdpSocket::bind(("127.0.0.1", output_port))
            .await
            .unwrap();
        let producer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        let first_sequence = 65_400_u16;
        let count = 400_u16;
        let sending = async {
            for index in 0..count {
                if index == count / 2 {
                    let mut fast_path_lost = sender.clone();
                    fast_path_lost.egresses.remove(0);
                    origin.update(fast_path_lost, &settings).unwrap();
                }
                let sequence = first_sequence.wrapping_add(index);
                let mut packet = vec![0x80, 97];
                packet.extend_from_slice(&sequence.to_be_bytes());
                packet.extend_from_slice(&(u32::from(index) * 48).to_be_bytes());
                packet.extend_from_slice(&0x5eed_u32.to_be_bytes());
                packet.extend_from_slice(&[index as u8; 288]);
                producer
                    .send_to(&packet, ("127.0.0.1", input_port))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        };
        let receiving = async {
            let mut received = Vec::new();
            let mut buffer = [0_u8; 2048];
            while received.len() < usize::from(count) {
                let length =
                    tokio::time::timeout(Duration::from_secs(3), consumer.recv(&mut buffer))
                        .await
                        .expect("every packet arrives")
                        .unwrap();
                assert_eq!(length, 300);
                assert!(buffer[12..length].iter().all(|byte| *byte == buffer[12]));
                received.push(u16::from_be_bytes([buffer[2], buffer[3]]));
            }
            received
        };
        let ((), received) = tokio::join!(sending, receiving);
        let mut extra = [0_u8; 2048];
        assert!(
            tokio::time::timeout(Duration::from_millis(400), consumer.recv(&mut extra))
                .await
                .is_err(),
            "a duplicate reached the output"
        );
        let expected: std::collections::BTreeSet<u16> = (0..count)
            .map(|index| first_sequence.wrapping_add(index))
            .collect();
        assert_eq!(
            received
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        let report = receiver.report.lock().unwrap().clone();
        assert_eq!(report.state, HopState::Provisioned);
        assert_eq!(
            report.egresses[0].status.condition,
            connect_core::LinkCondition::Flowing
        );
    }
}
