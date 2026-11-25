use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use connect_core::{
    DeliveryReason, HopState, LinkCondition, LinkStats, Passphrase, ResolvedAddr, RtpSocket,
    SocketRole, SocketStatus,
};
use hkdf::Hkdf;
use sha2::Sha256;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::fec::{Decoder, Encoder, Parity};
use crate::transport::Input;

const VERSION: u8 = 0x80;
const DATA: u8 = 96;
const PARITY: u8 = 97;
const KEEPALIVE: u8 = 98;
const HEADER: usize = 16;
const PARITY_HEADER: usize = 12;
const TAG: usize = 16;
const MAX_DATAGRAM: usize = 65_536;
const SOCKET_BUFFER: usize = 4 * 1024 * 1024;
const KEEPALIVE_EVERY: Duration = Duration::from_millis(100);
const PEER_SILENT: Duration = Duration::from_secs(1);
const RETRY: Duration = Duration::from_secs(1);
const SEEN: usize = 8_192;
const SALT: &[u8] = b"open-connect rtp link v1";

struct Keys {
    send: Option<ChaCha20Poly1305>,
    receive: Option<ChaCha20Poly1305>,
}

impl Keys {
    fn new(passphrase: Option<&Passphrase>, role: SocketRole) -> Self {
        let Some(passphrase) = passphrase else {
            return Self {
                send: None,
                receive: None,
            };
        };
        let hkdf = Hkdf::<Sha256>::new(Some(SALT), passphrase.expose().as_bytes());
        let key = |info: &[u8]| {
            let mut key = [0_u8; 32];
            hkdf.expand(info, &mut key)
                .expect("32 bytes is a valid HKDF-SHA256 output length");
            ChaCha20Poly1305::new(Key::from_slice(&key))
        };
        let toward_listener = key(b"caller to listener");
        let toward_caller = key(b"listener to caller");
        match role {
            SocketRole::Connect => Self {
                send: Some(toward_listener),
                receive: Some(toward_caller),
            },
            SocketRole::Listen => Self {
                send: Some(toward_caller),
                receive: Some(toward_listener),
            },
        }
    }
}

fn nonce(ssrc: u32, kind: u8, counter: u64) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[..4].copy_from_slice(&ssrc.to_be_bytes());
    nonce[4] = kind;
    nonce[6..].copy_from_slice(&counter.to_be_bytes()[2..]);
    nonce
}

fn seal(
    key: Option<&ChaCha20Poly1305>,
    ssrc: u32,
    kind: u8,
    counter: u64,
    timestamp: u32,
    plaintext: &[u8],
) -> Option<Vec<u8>> {
    let mut packet = Vec::with_capacity(HEADER + plaintext.len() + TAG);
    packet.push(VERSION);
    packet.push(kind);
    packet.extend_from_slice(&(counter as u16).to_be_bytes());
    packet.extend_from_slice(&timestamp.to_be_bytes());
    packet.extend_from_slice(&ssrc.to_be_bytes());
    packet.extend_from_slice(&((counter >> 16) as u32).to_be_bytes());
    match key {
        Some(key) => {
            let sealed = key
                .encrypt(
                    Nonce::from_slice(&nonce(ssrc, kind, counter)),
                    Payload {
                        msg: plaintext,
                        aad: &packet,
                    },
                )
                .ok()?;
            packet.extend_from_slice(&sealed);
        }
        None => packet.extend_from_slice(plaintext),
    }
    Some(packet)
}

struct Opened {
    kind: u8,
    ssrc: u32,
    counter: u64,
    body: Vec<u8>,
}

fn open(key: Option<&ChaCha20Poly1305>, packet: &[u8]) -> Option<Opened> {
    let header = packet.get(..HEADER)?;
    if header[0] != VERSION {
        return None;
    }
    let kind = header[1];
    let low = u64::from(u16::from_be_bytes([header[2], header[3]]));
    let ssrc = u32::from_be_bytes(header[8..12].try_into().ok()?);
    let high = u64::from(u32::from_be_bytes(header[12..16].try_into().ok()?));
    let counter = (high << 16) | low;
    let body = &packet[HEADER..];
    let body = match key {
        Some(key) => key
            .decrypt(
                Nonce::from_slice(&nonce(ssrc, kind, counter)),
                Payload {
                    msg: body,
                    aad: header,
                },
            )
            .ok()?,
        None => body.to_vec(),
    };
    Some(Opened {
        kind,
        ssrc,
        counter,
        body,
    })
}

fn parity_body(parity: &Parity) -> Vec<u8> {
    let mut body = Vec::with_capacity(PARITY_HEADER + parity.shard.len());
    body.extend_from_slice(&parity.base.to_be_bytes());
    body.extend_from_slice(&[parity.originals, parity.recovery, parity.index, 0]);
    body.extend_from_slice(&parity.shard);
    body
}

fn parse_parity(body: &[u8]) -> Option<Parity> {
    Some(Parity {
        base: u64::from_be_bytes(body.get(..8)?.try_into().ok()?),
        originals: *body.get(8)?,
        recovery: *body.get(9)?,
        index: *body.get(10)?,
        shard: body.get(PARITY_HEADER..)?.to_vec(),
    })
}

struct Peer {
    address: SocketAddr,
    ssrc: u32,
    heard: Instant,
    counter: u64,
}

struct State {
    socket: Option<Arc<UdpSocket>>,
    target: Option<SocketAddr>,
    peer: Option<Peer>,
    counters: [u64; 3],
    encoder: Encoder,
    decoder: Decoder,
    seen: std::collections::BTreeSet<u64>,
    highest: Option<u64>,
    bound: bool,
    bytes: u64,
    drops: u64,
    missing: u64,
    last_progress_ns: Option<u64>,
    sample_ns: u64,
    sample_bytes: u64,
    rate_mbps: f64,
}

struct Shared {
    keys: Keys,
    ssrc: u32,
    started: Instant,
    state: Mutex<State>,
    group_opened: Notify,
}

impl Shared {
    fn send(&self, state: &mut State, kind: u8, body: &[u8]) -> Result<usize, DeliveryReason> {
        let socket = state
            .socket
            .clone()
            .ok_or(DeliveryReason::OutputUnavailable)?;
        let address = state
            .peer
            .as_ref()
            .map(|peer| peer.address)
            .or(state.target)
            .ok_or(DeliveryReason::OutputUnavailable)?;
        let slot = match kind {
            DATA => 0,
            PARITY => 1,
            _ => 2,
        };
        let counter = state.counters[slot];
        state.counters[slot] += 1;
        let timestamp = self.started.elapsed().as_millis() as u32;
        let packet = seal(
            self.keys.send.as_ref(),
            self.ssrc,
            kind,
            counter,
            timestamp,
            body,
        )
        .ok_or(DeliveryReason::OutputUnavailable)?;
        match socket.try_send_to(&packet, address) {
            Ok(sent) => Ok(sent),
            Err(_) => {
                state.drops += 1;
                Err(DeliveryReason::OutputBackpressure)
            }
        }
    }

    fn adopt(&self, state: &mut State, opened: &Opened, from: SocketAddr, role: SocketRole) {
        let now = Instant::now();
        match &mut state.peer {
            Some(peer) if peer.ssrc == opened.ssrc => {
                if opened.counter >= peer.counter || peer.address == from {
                    peer.heard = now;
                    peer.counter = peer.counter.max(opened.counter);
                    if role == SocketRole::Listen {
                        peer.address = from;
                    }
                }
            }
            Some(peer) if now.duration_since(peer.heard) < PEER_SILENT => {}
            _ => {
                state.peer = Some(Peer {
                    address: if role == SocketRole::Listen {
                        from
                    } else {
                        state.target.unwrap_or(from)
                    },
                    ssrc: opened.ssrc,
                    heard: now,
                    counter: opened.counter,
                });
                state.seen.clear();
                state.highest = None;
                state.decoder = Decoder::default();
            }
        }
    }

    fn connected(state: &State) -> bool {
        state
            .peer
            .as_ref()
            .is_some_and(|peer| peer.heard.elapsed() < PEER_SILENT)
    }
}

fn bind(ip: IpAddr, port: u16) -> anyhow::Result<UdpSocket> {
    let address = SocketAddr::new(ip, port);
    let socket = Socket::new(
        Domain::for_address(address),
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    socket.set_recv_buffer_size(SOCKET_BUFFER)?;
    socket.set_send_buffer_size(SOCKET_BUFFER)?;
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    Ok(UdpSocket::from_std(socket.into())?)
}

fn random_ssrc() -> u32 {
    let mut bytes = [0_u8; 4];
    getrandom::fill(&mut bytes).expect("the operating system provides randomness");
    u32::from_be_bytes(bytes)
}

pub struct RtpLink {
    pub spec: RtpSocket,
    shared: Arc<Shared>,
    invalid: bool,
    task: JoinHandle<()>,
}

struct Receiving {
    path: usize,
    sender: mpsc::Sender<Input>,
    clock: Clock,
    maximum: usize,
}

impl RtpLink {
    #[must_use]
    pub fn input(
        spec: RtpSocket,
        path: usize,
        sender: mpsc::Sender<Input>,
        clock: Clock,
        bind_ip: IpAddr,
    ) -> Self {
        let maximum = if spec.params().payload == connect_core::PayloadKind::ConnectTimedV1 {
            crate::record::MAX_MESSAGE
        } else {
            MAX_DATAGRAM
        };
        Self::start(
            spec,
            bind_ip,
            Some(Receiving {
                path,
                sender,
                clock,
                maximum,
            }),
        )
    }

    #[must_use]
    pub fn output(spec: RtpSocket, bind_ip: IpAddr) -> Self {
        Self::start(spec, bind_ip, None)
    }

    fn start(spec: RtpSocket, bind_ip: IpAddr, receiving: Option<Receiving>) -> Self {
        let params = spec.params();
        let invalid = params.fec.validate().is_err()
            || params.passphrase.as_ref().is_some_and(|phrase| {
                !(Passphrase::MIN_LEN..=Passphrase::MAX_LEN).contains(&phrase.expose().len())
            });
        let shared = Arc::new(Shared {
            keys: Keys::new(params.passphrase.as_ref(), spec.role()),
            ssrc: random_ssrc(),
            started: Instant::now(),
            group_opened: Notify::new(),
            state: Mutex::new(State {
                socket: None,
                target: None,
                peer: None,
                counters: [0; 3],
                encoder: Encoder::new(params.fec),
                decoder: Decoder::default(),
                seen: std::collections::BTreeSet::new(),
                highest: None,
                bound: false,
                bytes: 0,
                drops: 0,
                missing: 0,
                last_progress_ns: None,
                sample_ns: 0,
                sample_bytes: 0,
                rate_mbps: 0.0,
            }),
        });
        let task = if invalid {
            tokio::spawn(async {})
        } else {
            tokio::spawn(run(spec.clone(), bind_ip, shared.clone(), receiving))
        };
        Self {
            spec,
            shared,
            invalid,
            task,
        }
    }

    pub fn try_send(&self, bytes: &Bytes, now_ns: u64) -> Result<(), DeliveryReason> {
        let mut state = self.shared.state.lock().unwrap();
        let sequence = state.counters[0];
        let sent = self.shared.send(&mut state, DATA, bytes);
        if state.counters[0] == sequence {
            return sent.map(|_| ());
        }
        let idle = state.encoder.due().is_none();
        for parity in state.encoder.push(sequence, bytes, Instant::now()) {
            let _ = self.shared.send(&mut state, PARITY, &parity_body(&parity));
        }
        if idle && state.encoder.due().is_some() {
            self.shared.group_opened.notify_one();
        }
        sent?;
        state.bytes = state.bytes.saturating_add(bytes.len() as u64);
        state.last_progress_ns = Some(now_ns);
        Ok(())
    }

    #[must_use]
    pub fn status(&self, now_ns: u64) -> SocketStatus {
        let mut state = self.shared.state.lock().unwrap();
        let elapsed = now_ns.saturating_sub(state.sample_ns);
        if elapsed >= 200_000_000 {
            state.rate_mbps =
                state.bytes.saturating_sub(state.sample_bytes) as f64 * 8_000.0 / elapsed as f64;
            state.sample_ns = now_ns;
            state.sample_bytes = state.bytes;
        }
        let connected = Shared::connected(&state);
        let condition = match (connected, state.last_progress_ns) {
            (_, Some(last)) if now_ns.saturating_sub(last) > 3_000_000_000 => {
                LinkCondition::Stalled
            }
            (true, Some(_)) => LinkCondition::Flowing,
            (true, None) => LinkCondition::Connected,
            (false, _) => LinkCondition::Connecting,
        };
        SocketStatus {
            peer: None,
            condition,
            resolved: Some(ResolvedAddr {
                host: match &self.spec {
                    RtpSocket::Listen { .. } => "0.0.0.0".into(),
                    RtpSocket::Connect { host, .. } => host.clone(),
                },
                port: self.spec.port(),
            }),
            stats: Some(LinkStats {
                connections: usize::from(connected),
                rate_mbps: state.rate_mbps,
                packets_received_lost: i64::try_from(state.missing).unwrap_or(i64::MAX),
                packets_recovered: i64::try_from(state.decoder.recovered).unwrap_or(i64::MAX),
                ..LinkStats::default()
            }),
        }
    }

    #[must_use]
    pub fn take_drops(&self) -> u64 {
        std::mem::take(&mut self.shared.state.lock().unwrap().drops)
    }

    #[must_use]
    pub fn state(&self) -> HopState {
        if self.invalid {
            HopState::Failed
        } else if self.shared.state.lock().unwrap().bound {
            HopState::Provisioned
        } else {
            HopState::Pending
        }
    }
}

impl Drop for RtpLink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn resolve(spec: &RtpSocket) -> Option<SocketAddr> {
    match spec {
        RtpSocket::Listen { .. } => None,
        RtpSocket::Connect { host, port, .. } => tokio::net::lookup_host((host.as_str(), *port))
            .await
            .ok()?
            .next(),
    }
}

async fn run(spec: RtpSocket, bind_ip: IpAddr, shared: Arc<Shared>, receiving: Option<Receiving>) {
    let socket = loop {
        let port = match &spec {
            RtpSocket::Listen { port, .. } => *port,
            RtpSocket::Connect { .. } => 0,
        };
        let target = match &spec {
            RtpSocket::Listen { .. } => None,
            RtpSocket::Connect { .. } => match resolve(&spec).await {
                Some(target) => Some(target),
                None => {
                    tokio::time::sleep(RETRY).await;
                    continue;
                }
            },
        };
        let local = match target {
            Some(SocketAddr::V6(_)) if bind_ip.is_unspecified() => {
                IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
            }
            _ => bind_ip,
        };
        match bind(local, port) {
            Ok(socket) => {
                let socket = Arc::new(socket);
                let mut state = shared.state.lock().unwrap();
                state.socket = Some(socket.clone());
                state.target = target;
                state.bound = true;
                break socket;
            }
            Err(error) => {
                tracing::debug!(port, error = %error, "RTP link bind failed");
                tokio::time::sleep(RETRY).await;
            }
        }
    };
    let role = spec.role();
    let mut keepalive = tokio::time::interval(KEEPALIVE_EVERY);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut buffer = vec![0_u8; MAX_DATAGRAM];
    loop {
        let due = shared.state.lock().unwrap().encoder.due();
        let flush = async {
            match due {
                Some(due) => tokio::time::sleep_until(due.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                if let Ok((length, from)) = received {
                    receive(&shared, &buffer[..length], from, role, receiving.as_ref());
                }
            }
            () = shared.group_opened.notified() => {}
            _ = keepalive.tick() => {
                let mut state = shared.state.lock().unwrap();
                let _ = shared.send(&mut state, KEEPALIVE, &[]);
            }
            () = flush => {
                let mut state = shared.state.lock().unwrap();
                for parity in state.encoder.flush() {
                    let _ = shared.send(&mut state, PARITY, &parity_body(&parity));
                }
            }
        }
    }
}

fn receive(
    shared: &Shared,
    packet: &[u8],
    from: SocketAddr,
    role: SocketRole,
    receiving: Option<&Receiving>,
) {
    let Some(opened) = open(shared.keys.receive.as_ref(), packet) else {
        return;
    };
    let mut state = shared.state.lock().unwrap();
    shared.adopt(&mut state, &opened, from, role);
    if state
        .peer
        .as_ref()
        .is_none_or(|peer| peer.ssrc != opened.ssrc)
    {
        return;
    }
    let Some(receiving) = receiving else {
        return;
    };
    let now = Instant::now();
    let delivered = match opened.kind {
        DATA => {
            let sequence = opened.counter;
            let mut delivered = Vec::new();
            if fresh(&mut state, sequence) {
                delivered.push(opened.body.clone());
            }
            for (sequence, payload) in state.decoder.data(sequence, &opened.body, now) {
                if fresh(&mut state, sequence) {
                    delivered.push(payload);
                }
            }
            delivered
        }
        PARITY => match parse_parity(&opened.body) {
            Some(parity) => state
                .decoder
                .parity(parity, now)
                .into_iter()
                .filter_map(|(sequence, payload)| fresh(&mut state, sequence).then_some(payload))
                .collect(),
            None => Vec::new(),
        },
        _ => Vec::new(),
    };
    if delivered.is_empty() {
        return;
    }
    let now_ns = receiving.clock.monotonic_ns();
    for payload in delivered {
        state.bytes = state.bytes.saturating_add(payload.len() as u64);
        state.last_progress_ns = Some(now_ns);
        let input = Input {
            path: receiving.path,
            bytes: Bytes::from(payload),
            stamp: receiving.clock.sample(),
        };
        if input.bytes.len() > receiving.maximum || receiving.sender.try_send(input).is_err() {
            state.drops += 1;
        }
    }
}

fn fresh(state: &mut State, sequence: u64) -> bool {
    if !state.seen.insert(sequence) {
        return false;
    }
    while state.seen.len() > SEEN {
        state.seen.pop_first();
    }
    match state.highest {
        Some(highest) if sequence > highest => {
            state.missing += sequence - highest - 1;
            state.highest = Some(sequence);
        }
        Some(_) => state.missing = state.missing.saturating_sub(1),
        None => state.highest = Some(sequence),
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phrase() -> Passphrase {
        Passphrase::new("a-link-passphrase-for-tests")
    }

    #[test]
    fn a_sealed_packet_opens_only_with_the_other_ends_key_and_an_untouched_header() {
        let caller = Keys::new(Some(&phrase()), SocketRole::Connect);
        let listener = Keys::new(Some(&phrase()), SocketRole::Listen);
        let packet = seal(caller.send.as_ref(), 7, DATA, 70_000, 5, b"record").unwrap();
        let opened = open(listener.receive.as_ref(), &packet).unwrap();
        assert_eq!(
            (
                opened.kind,
                opened.ssrc,
                opened.counter,
                opened.body.as_slice()
            ),
            (DATA, 7, 70_000, b"record".as_slice())
        );
        assert!(open(caller.receive.as_ref(), &packet).is_none());
        let mut tampered = packet.clone();
        tampered[3] ^= 1;
        assert!(open(listener.receive.as_ref(), &tampered).is_none());
        let other = Keys::new(
            Some(&Passphrase::new("another-passphrase")),
            SocketRole::Listen,
        );
        assert!(open(other.receive.as_ref(), &packet).is_none());
    }

    #[test]
    fn the_counter_survives_sixteen_bit_wraparound() {
        let packet = seal(None, 1, KEEPALIVE, (3 << 16) + 9, 0, &[]).unwrap();
        assert_eq!(open(None, &packet).unwrap().counter, (3 << 16) + 9);
    }

    #[test]
    fn parity_round_trips_through_its_body() {
        let parity = Parity {
            base: 1 << 40,
            originals: 10,
            recovery: 2,
            index: 1,
            shard: vec![1, 2, 3, 4],
        };
        assert_eq!(parse_parity(&parity_body(&parity)), Some(parity));
    }

    fn free_port() -> u16 {
        std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn params() -> connect_core::RtpParams {
        connect_core::RtpParams {
            payload: connect_core::PayloadKind::Raw,
            passphrase: Some(phrase()),
            fec: connect_core::FecParams::default(),
        }
    }

    fn clock() -> Clock {
        Clock::new(crate::clock::ClockConfig::SharedHost {
            domain: "test".into(),
            tai_offset_secs: 37,
        })
    }

    async fn carry(output: RtpLink, mut received: mpsc::Receiver<Input>) -> Vec<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut sent = 0_u8;
        let mut got = Vec::new();
        while got.len() < 20 && Instant::now() < deadline {
            if sent < 20 && output.try_send(&Bytes::from(vec![sent; 100]), 1).is_ok() {
                sent += 1;
            }
            if let Ok(Some(input)) =
                tokio::time::timeout(Duration::from_millis(20), received.recv()).await
            {
                got.push(input.bytes.to_vec());
            }
        }
        got
    }

    #[tokio::test]
    async fn a_caller_sends_to_a_listener_over_an_encrypted_link() {
        let port = free_port();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let (sender, receiver) = mpsc::channel(64);
        let _input = RtpLink::input(
            RtpSocket::Listen {
                port,
                params: params(),
            },
            0,
            sender,
            clock(),
            ip,
        );
        let output = RtpLink::output(
            RtpSocket::Connect {
                host: "127.0.0.1".into(),
                port,
                params: params(),
            },
            ip,
        );
        let got = carry(output, receiver).await;
        assert_eq!(got, (0..20).map(|n| vec![n; 100]).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_listener_sends_to_a_caller_once_the_callers_keepalive_arrives() {
        let port = free_port();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let output = RtpLink::output(
            RtpSocket::Listen {
                port,
                params: params(),
            },
            ip,
        );
        let (sender, receiver) = mpsc::channel(64);
        let _input = RtpLink::input(
            RtpSocket::Connect {
                host: "127.0.0.1".into(),
                port,
                params: params(),
            },
            0,
            sender,
            clock(),
            ip,
        );
        let got = carry(output, receiver).await;
        assert_eq!(got.len(), 20);
        assert!(
            got.iter()
                .enumerate()
                .all(|(n, bytes)| bytes == &vec![n as u8; 100])
        );
    }

    #[tokio::test]
    async fn a_group_opened_while_the_link_is_idle_sends_its_parity_when_its_window_closes() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let keys = Keys::new(Some(&phrase()), SocketRole::Listen);
        let output = RtpLink::output(
            RtpSocket::Connect {
                host: "127.0.0.1".into(),
                port: peer.local_addr().unwrap().port(),
                params: params(),
            },
            "127.0.0.1".parse().unwrap(),
        );
        let mut warm = Encoder::new(params().fec);
        warm.push(0, b"record", Instant::now());
        assert!(!warm.flush().is_empty());
        let mut buffer = vec![0_u8; MAX_DATAGRAM];
        let mut next = async || {
            let (length, _) = peer.recv_from(&mut buffer).await.unwrap();
            open(keys.receive.as_ref(), &buffer[..length]).unwrap().kind
        };
        while next().await != KEEPALIVE {}
        let sent = Instant::now();
        output.try_send(&Bytes::from_static(b"record"), 1).unwrap();
        assert_eq!(next().await, DATA);
        let window = Duration::from_millis(u64::from(params().fec.window_ms));
        let parity = tokio::time::timeout(window * 3, async { while next().await != PARITY {} });
        assert!(parity.await.is_ok());
        assert!(sent.elapsed() >= window);
    }

    #[test]
    fn gaps_count_as_missing_until_the_sequence_arrives() {
        let mut state = State {
            socket: None,
            target: None,
            peer: None,
            counters: [0; 3],
            encoder: Encoder::new(connect_core::FecParams::default()),
            decoder: Decoder::default(),
            seen: std::collections::BTreeSet::new(),
            highest: None,
            bound: false,
            bytes: 0,
            drops: 0,
            missing: 0,
            last_progress_ns: None,
            sample_ns: 0,
            sample_bytes: 0,
            rate_mbps: 0.0,
        };
        assert!(fresh(&mut state, 10));
        assert!(fresh(&mut state, 14));
        assert_eq!(state.missing, 3);
        assert!(fresh(&mut state, 12));
        assert!(!fresh(&mut state, 12));
        assert_eq!(state.missing, 2);
    }
}
