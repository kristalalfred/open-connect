use std::collections::{BTreeMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::Deserialize;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use weave_core::{
    HopState, LinkCondition, LinkStats, NodeInput, NodeOutput, ResolvedAddr, SocketStatus, Track,
};

use crate::clock::Clock;
use crate::transport::Input;

const STALL: Duration = Duration::from_secs(3);
const RETRY: Duration = Duration::from_secs(1);
const MAX_DATAGRAM: usize = 65_536;
const RECEIVE_BUFFER: usize = 4 * 1024 * 1024;
const SHARED_PACKETS: usize = 4_096;
pub const DEDUPLICATION_WINDOW: usize = 65_536;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RtpConfig {
    pub inputs: Vec<RtpInput>,
    pub outputs: Vec<RtpOutput>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RtpInput {
    pub id: String,
    pub label: String,
    pub tracks: Vec<Track>,
    pub address: Ipv4Addr,
    pub port: u16,
    #[serde(default)]
    pub source: Option<Ipv4Addr>,
    #[serde(default)]
    pub interface: Option<Ipv4Addr>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RtpOutput {
    pub id: String,
    pub label: String,
    pub address: Ipv4Addr,
    pub port: u16,
    #[serde(default)]
    pub interface: Option<Ipv4Addr>,
    #[serde(default)]
    pub ttl: Option<u32>,
}

impl RtpConfig {
    #[must_use]
    pub fn declared_inputs(&self) -> Vec<NodeInput> {
        self.inputs
            .iter()
            .map(|input| NodeInput {
                id: input.id.clone(),
                label: input.label.clone(),
                tracks: input.tracks.clone(),
            })
            .collect()
    }

    #[must_use]
    pub fn declared_outputs(&self) -> Vec<NodeOutput> {
        self.outputs
            .iter()
            .map(|output| NodeOutput {
                id: output.id.clone(),
                label: output.label.clone(),
            })
            .collect()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let issues = weave_core::validation::validate_local_io(
            &self.declared_inputs(),
            &self.declared_outputs(),
            "rtp.",
        );
        if let Some(issue) = issues.first() {
            anyhow::bail!("{}: {}", issue.field, issue.message);
        }
        for (index, input) in self.inputs.iter().enumerate() {
            anyhow::ensure!(input.port != 0, "rtp.inputs[{index}].port must not be 0");
            anyhow::ensure!(
                input.interface.is_none() || input.address.is_multicast(),
                "rtp.inputs[{index}].interface needs a multicast address"
            );
        }
        for (index, output) in self.outputs.iter().enumerate() {
            anyhow::ensure!(
                output.port != 0 && !output.address.is_unspecified(),
                "rtp.outputs[{index}] needs a destination address and port"
            );
            anyhow::ensure!(
                (output.interface.is_none() && output.ttl.is_none())
                    || output.address.is_multicast(),
                "rtp.outputs[{index}].interface and ttl need a multicast address"
            );
            anyhow::ensure!(
                output.ttl.is_none_or(|ttl| (1..=255).contains(&ttl)),
                "rtp.outputs[{index}].ttl must be between 1 and 255"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PacketKey {
    pub ssrc: u32,
    pub sequence: u16,
    pub timestamp: u32,
}

impl PacketKey {
    #[must_use]
    pub fn parse(packet: &[u8]) -> Option<Self> {
        let header = packet.get(..12)?;
        let csrc = usize::from(header[0] & 0x0f);
        if header[0] >> 6 != 2 || packet.len() < 12 + 4 * csrc {
            return None;
        }
        Some(Self {
            sequence: u16::from_be_bytes([header[2], header[3]]),
            timestamp: u32::from_be_bytes(header[4..8].try_into().ok()?),
            ssrc: u32::from_be_bytes(header[8..12].try_into().ok()?),
        })
    }
}

pub struct Seen {
    keys: HashSet<PacketKey>,
    order: VecDeque<PacketKey>,
    capacity: usize,
}

impl Seen {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            keys: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn first(&mut self, key: PacketKey) -> bool {
        if !self.keys.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.keys.remove(&oldest);
        }
        true
    }
}

#[derive(Default)]
struct Meter {
    open: bool,
    failed: bool,
    bytes: u64,
    last_progress: Option<Instant>,
    sample_at: Option<Instant>,
    sample_bytes: u64,
    rate_mbps: f64,
    drops: u64,
}

impl Meter {
    fn progress(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes as u64);
        self.last_progress = Some(Instant::now());
    }

    fn status(&mut self, address: Ipv4Addr, port: u16) -> SocketStatus {
        let now = Instant::now();
        let elapsed = self
            .sample_at
            .map_or(Duration::MAX, |sample| now.duration_since(sample));
        if elapsed >= Duration::from_millis(200) {
            if self.sample_at.is_some() {
                self.rate_mbps = self.bytes.saturating_sub(self.sample_bytes) as f64 * 8.0
                    / elapsed.as_secs_f64()
                    / 1_000_000.0;
            }
            self.sample_at = Some(now);
            self.sample_bytes = self.bytes;
        }
        let condition = match (self.open, self.last_progress) {
            (false, _) => LinkCondition::Connecting,
            (true, Some(last)) if now.duration_since(last) > STALL => LinkCondition::Stalled,
            (true, Some(_)) => LinkCondition::Flowing,
            (true, None) => LinkCondition::Idle,
        };
        SocketStatus {
            peer: None,
            condition,
            resolved: Some(ResolvedAddr {
                host: address.to_string(),
                port,
            }),
            stats: Some(LinkStats {
                connections: usize::from(self.open),
                rate_mbps: self.rate_mbps,
                ..LinkStats::default()
            }),
        }
    }

    fn state(&self) -> HopState {
        if self.failed {
            HopState::Failed
        } else if self.open {
            HopState::Provisioned
        } else {
            HopState::Pending
        }
    }
}

pub struct Rtp {
    inputs: BTreeMap<String, RtpInput>,
    outputs: BTreeMap<String, RtpOutput>,
    receivers: Mutex<BTreeMap<String, Weak<Receiver>>>,
}

impl Rtp {
    #[must_use]
    pub fn new(config: &RtpConfig) -> Self {
        Self {
            inputs: config
                .inputs
                .iter()
                .map(|input| (input.id.clone(), input.clone()))
                .collect(),
            outputs: config
                .outputs
                .iter()
                .map(|output| (output.id.clone(), output.clone()))
                .collect(),
            receivers: Mutex::default(),
        }
    }

    #[must_use]
    pub fn has_input(&self, id: &str) -> bool {
        self.inputs.contains_key(id)
    }

    #[must_use]
    pub fn has_output(&self, id: &str) -> bool {
        self.outputs.contains_key(id)
    }

    fn receiver(&self, id: &str) -> Arc<Receiver> {
        let mut receivers = self.receivers.lock().unwrap();
        receivers.retain(|_, receiver| receiver.strong_count() > 0);
        if let Some(receiver) = receivers.get(id).and_then(Weak::upgrade) {
            return receiver;
        }
        let receiver = Arc::new(Receiver::start(self.inputs[id].clone()));
        receivers.insert(id.to_string(), Arc::downgrade(&receiver));
        receiver
    }

    #[must_use]
    pub fn tap(&self, id: &str, path: usize, sender: mpsc::Sender<Input>, clock: Clock) -> Tap {
        Tap::start(self.receiver(id), path, sender, clock)
    }

    #[must_use]
    pub fn output(&self, id: &str) -> Output {
        Output::new(id.to_string(), self.outputs[id].clone())
    }
}

struct Receiver {
    config: RtpInput,
    packets: broadcast::Sender<Bytes>,
    meter: Arc<Mutex<Meter>>,
    task: JoinHandle<()>,
}

fn bind_input(config: &RtpInput) -> anyhow::Result<tokio::net::UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    let multicast = config.address.is_multicast();
    if multicast {
        socket.set_reuse_address(true)?;
    }
    socket.set_recv_buffer_size(RECEIVE_BUFFER)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::V4(SocketAddrV4::new(config.address, config.port)).into())?;
    if multicast {
        let interface = config.interface.unwrap_or(Ipv4Addr::UNSPECIFIED);
        match config.source {
            Some(source) => socket.join_ssm_v4(&source, &config.address, &interface)?,
            None => socket.join_multicast_v4(&config.address, &interface)?,
        }
    }
    Ok(tokio::net::UdpSocket::from_std(socket.into())?)
}

impl Receiver {
    fn start(config: RtpInput) -> Self {
        let (packets, _) = broadcast::channel(SHARED_PACKETS);
        let meter = Arc::new(Mutex::new(Meter::default()));
        let task = tokio::spawn(receive(config.clone(), packets.clone(), meter.clone()));
        Self {
            config,
            packets,
            meter,
            task,
        }
    }
}

async fn receive(config: RtpInput, packets: broadcast::Sender<Bytes>, meter: Arc<Mutex<Meter>>) {
    let mut buffer = vec![0_u8; MAX_DATAGRAM];
    loop {
        let socket = match bind_input(&config) {
            Ok(socket) => socket,
            Err(error) => {
                tracing::warn!(input = %config.id, error = %error, "RTP input cannot bind");
                meter.lock().unwrap().failed = true;
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        {
            let mut meter = meter.lock().unwrap();
            meter.failed = false;
            meter.open = true;
        }
        loop {
            match socket.recv_from(&mut buffer).await {
                Ok((length, from)) => {
                    let packet = &buffer[..length];
                    let unexpected = config
                        .source
                        .is_some_and(|source| from.ip() != std::net::IpAddr::V4(source));
                    if unexpected || PacketKey::parse(packet).is_none() {
                        meter.lock().unwrap().drops += 1;
                        continue;
                    }
                    meter.lock().unwrap().progress(length);
                    let _ = packets.send(Bytes::copy_from_slice(packet));
                }
                Err(error) => {
                    tracing::warn!(input = %config.id, error = %error, "RTP input receive failed");
                    break;
                }
            }
        }
        meter.lock().unwrap().open = false;
        tokio::time::sleep(RETRY).await;
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct Tap {
    pub id: String,
    receiver: Arc<Receiver>,
    drops: Arc<Mutex<u64>>,
    task: JoinHandle<()>,
}

impl Tap {
    fn start(
        receiver: Arc<Receiver>,
        path: usize,
        sender: mpsc::Sender<Input>,
        clock: Clock,
    ) -> Self {
        let drops = Arc::new(Mutex::new(0_u64));
        let lost = drops.clone();
        let mut packets = receiver.packets.subscribe();
        let task = tokio::spawn(async move {
            loop {
                match packets.recv().await {
                    Ok(bytes) => {
                        let stamp = clock.sample();
                        if sender.try_send(Input { path, bytes, stamp }).is_err() {
                            *lost.lock().unwrap() += 1;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        *lost.lock().unwrap() += skipped;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Self {
            id: receiver.config.id.clone(),
            receiver,
            drops,
            task,
        }
    }

    #[must_use]
    pub fn status(&self) -> SocketStatus {
        self.receiver
            .meter
            .lock()
            .unwrap()
            .status(self.receiver.config.address, self.receiver.config.port)
    }

    #[must_use]
    pub fn take_drops(&self) -> u64 {
        std::mem::take(&mut *self.drops.lock().unwrap())
            + std::mem::take(&mut self.receiver.meter.lock().unwrap().drops)
    }

    #[must_use]
    pub fn state(&self) -> HopState {
        self.receiver.meter.lock().unwrap().state()
    }
}

impl Drop for Tap {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Sending {
    socket: Option<UdpSocket>,
    attempted: Option<Instant>,
    seen: Seen,
}

pub struct Output {
    pub id: String,
    config: RtpOutput,
    sending: Mutex<Sending>,
    meter: Mutex<Meter>,
}

fn open_output(config: &RtpOutput) -> anyhow::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    let local = config.interface.unwrap_or(Ipv4Addr::UNSPECIFIED);
    if config.address.is_multicast() {
        if let Some(interface) = config.interface {
            socket.set_multicast_if_v4(&interface)?;
        }
        if let Some(ttl) = config.ttl {
            socket.set_multicast_ttl_v4(ttl)?;
        }
    }
    socket.bind(&SocketAddr::V4(SocketAddrV4::new(local, 0)).into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

impl Output {
    fn new(id: String, config: RtpOutput) -> Self {
        let output = Self {
            id,
            config,
            sending: Mutex::new(Sending {
                socket: None,
                attempted: None,
                seen: Seen::new(DEDUPLICATION_WINDOW),
            }),
            meter: Mutex::default(),
        };
        output.reopen(&mut output.sending.lock().unwrap());
        output
    }

    fn reopen(&self, sending: &mut Sending) {
        if sending.socket.is_some()
            || sending
                .attempted
                .is_some_and(|attempted| attempted.elapsed() < RETRY)
        {
            return;
        }
        sending.attempted = Some(Instant::now());
        let mut meter = self.meter.lock().unwrap();
        match open_output(&self.config) {
            Ok(socket) => {
                sending.socket = Some(socket);
                meter.open = true;
                meter.failed = false;
            }
            Err(error) => {
                tracing::warn!(output = %self.id, error = %error, "RTP output cannot open");
                meter.failed = true;
            }
        }
    }

    pub fn send(&self, bytes: &[u8]) {
        let mut sending = self.sending.lock().unwrap();
        let Some(key) = PacketKey::parse(bytes) else {
            self.meter.lock().unwrap().drops += 1;
            return;
        };
        if !sending.seen.first(key) {
            return;
        }
        self.reopen(&mut sending);
        let sent = sending.socket.as_ref().map(|socket| {
            socket.send_to(
                bytes,
                SocketAddrV4::new(self.config.address, self.config.port),
            )
        });
        let mut meter = self.meter.lock().unwrap();
        match sent {
            Some(Ok(_)) => meter.progress(bytes.len()),
            Some(Err(error)) => {
                tracing::debug!(output = %self.id, error = %error, "RTP output send failed");
                meter.drops += 1;
            }
            None => meter.drops += 1,
        }
    }

    #[must_use]
    pub fn status(&self) -> SocketStatus {
        self.meter
            .lock()
            .unwrap()
            .status(self.config.address, self.config.port)
    }

    #[must_use]
    pub fn take_drops(&self) -> u64 {
        std::mem::take(&mut self.meter.lock().unwrap().drops)
    }

    #[must_use]
    pub fn state(&self) -> HopState {
        self.meter.lock().unwrap().state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(ssrc: u32, sequence: u16, timestamp: u32) -> Vec<u8> {
        let mut packet = vec![0x80, 97];
        packet.extend_from_slice(&sequence.to_be_bytes());
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&ssrc.to_be_bytes());
        packet.extend_from_slice(&[0; 288]);
        packet
    }

    #[test]
    fn a_packet_key_comes_from_a_version_two_header() {
        assert_eq!(
            PacketKey::parse(&packet(7, 65_535, 48_000)),
            Some(PacketKey {
                ssrc: 7,
                sequence: 65_535,
                timestamp: 48_000
            })
        );
        assert_eq!(PacketKey::parse(&[0x80; 11]), None);
        let mut version_one = packet(7, 1, 1);
        version_one[0] = 0x40;
        assert_eq!(PacketKey::parse(&version_one), None);
        let mut missing_csrc = packet(7, 1, 1);
        missing_csrc[0] = 0x8f;
        missing_csrc.truncate(40);
        assert_eq!(PacketKey::parse(&missing_csrc), None);
    }

    #[test]
    fn only_the_first_copy_of_a_packet_passes_until_it_leaves_the_window() {
        let mut seen = Seen::new(3);
        let key = |sequence| PacketKey {
            ssrc: 1,
            sequence,
            timestamp: u32::from(sequence) * 48,
        };
        assert!(seen.first(key(1)));
        assert!(!seen.first(key(1)));
        assert!(seen.first(key(2)));
        assert!(seen.first(key(3)));
        assert!(!seen.first(key(2)));
        assert!(seen.first(key(4)));
        assert!(seen.first(key(1)));
    }

    #[test]
    fn a_restarted_sender_reusing_sequence_numbers_is_not_a_duplicate() {
        let mut seen = Seen::new(DEDUPLICATION_WINDOW);
        let before = PacketKey {
            ssrc: 1,
            sequence: 10,
            timestamp: 480,
        };
        assert!(seen.first(before));
        assert!(seen.first(PacketKey {
            timestamp: 96_000,
            ..before
        }));
    }

    #[test]
    fn configuration_needs_unique_ids_and_multicast_for_interfaces() {
        let config: RtpConfig = serde_norway::from_str(
            "inputs:\n  - { id: mic, label: Mic, tracks: [audio], address: 239.69.0.1, port: 5004, interface: 10.0.0.2 }\noutputs:\n  - { id: bus, label: Bus, address: 239.69.0.2, port: 5004, ttl: 16 }\n",
        )
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.declared_inputs()[0].tracks, vec![Track::Audio]);

        let mut unicast = config.clone();
        unicast.inputs[0].address = Ipv4Addr::new(10, 0, 0, 2);
        assert!(format!("{:#}", unicast.validate().unwrap_err()).contains("interface"));

        let mut duplicate = config.clone();
        duplicate.outputs.push(duplicate.outputs[0].clone());
        assert!(duplicate.validate().is_err());

        let mut unaddressed = config;
        unaddressed.outputs[0].address = Ipv4Addr::UNSPECIFIED;
        assert!(unaddressed.validate().is_err());
    }

    #[tokio::test]
    async fn two_hops_on_one_input_share_its_socket() {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let rtp = Rtp::new(&RtpConfig {
            inputs: vec![RtpInput {
                id: "mic".into(),
                label: "Mic".into(),
                tracks: vec![Track::Audio],
                address: Ipv4Addr::LOCALHOST,
                port,
                source: None,
                interface: None,
            }],
            outputs: vec![],
        });
        let clock = Clock::new(crate::clock::ClockConfig::SharedHost {
            domain: "test".into(),
            tai_offset_secs: 37,
        });
        let (first_sender, mut first) = mpsc::channel(8);
        let (second_sender, mut second) = mpsc::channel(8);
        let first_tap = rtp.tap("mic", 0, first_sender, clock.clone());
        let second_tap = rtp.tap("mic", 0, second_sender, clock);
        assert!(Arc::ptr_eq(&first_tap.receiver, &second_tap.receiver));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let producer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        producer
            .send_to(&packet(1, 1, 48), ("127.0.0.1", port))
            .await
            .unwrap();
        producer
            .send_to(b"not rtp", ("127.0.0.1", port))
            .await
            .unwrap();
        for receiver in [&mut first, &mut second] {
            let input = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(input.bytes.as_ref(), packet(1, 1, 48).as_slice());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(first_tap.take_drops(), 1);
        drop((first_tap, second_tap));
        assert!(rtp.receivers.lock().unwrap()["mic"].upgrade().is_none());
    }
}
