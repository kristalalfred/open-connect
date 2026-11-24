use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use connect_core::FecParams;

const RETAIN: Duration = Duration::from_secs(2);
const MAX_RETAINED: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parity {
    pub base: u64,
    pub originals: u8,
    pub recovery: u8,
    pub index: u8,
    pub shard: Vec<u8>,
}

fn shard(payload: &[u8], length: usize) -> Vec<u8> {
    let mut shard = Vec::with_capacity(length);
    shard.extend_from_slice(
        &u16::try_from(payload.len())
            .unwrap_or(u16::MAX)
            .to_be_bytes(),
    );
    shard.extend_from_slice(payload);
    shard.resize(length, 0);
    shard
}

fn shard_length(longest: usize) -> usize {
    (longest + 2).next_multiple_of(2)
}

fn unshard(shard: &[u8]) -> Option<Vec<u8>> {
    let length = usize::from(u16::from_be_bytes([*shard.first()?, *shard.get(1)?]));
    shard.get(2..2 + length).map(<[u8]>::to_vec)
}

pub struct Encoder {
    params: FecParams,
    base: u64,
    group: Vec<Vec<u8>>,
    opened: Option<Instant>,
}

impl Encoder {
    #[must_use]
    pub fn new(params: FecParams) -> Self {
        Self {
            params,
            base: 0,
            group: Vec::new(),
            opened: None,
        }
    }

    pub fn push(&mut self, sequence: u64, payload: &[u8], now: Instant) -> Vec<Parity> {
        if self.params.recovery == 0 {
            return Vec::new();
        }
        if self.group.is_empty() {
            self.base = sequence;
            self.opened = Some(now);
        }
        self.group.push(payload.to_vec());
        if self.group.len() >= usize::from(self.params.originals) {
            self.flush()
        } else {
            Vec::new()
        }
    }

    #[must_use]
    pub fn due(&self) -> Option<Instant> {
        self.opened
            .map(|opened| opened + Duration::from_millis(u64::from(self.params.window_ms)))
    }

    pub fn flush(&mut self) -> Vec<Parity> {
        self.opened = None;
        let group = std::mem::take(&mut self.group);
        let Some(longest) = group.iter().map(Vec::len).max() else {
            return Vec::new();
        };
        let length = shard_length(longest);
        let originals = group.len();
        let recovery = usize::from(self.params.recovery);
        let Ok(shards) = reed_solomon_simd::encode(
            originals,
            recovery,
            group.iter().map(|payload| shard(payload, length)),
        ) else {
            return Vec::new();
        };
        shards
            .into_iter()
            .enumerate()
            .map(|(index, shard)| Parity {
                base: self.base,
                originals: u8::try_from(originals).unwrap_or(u8::MAX),
                recovery: self.params.recovery,
                index: u8::try_from(index).unwrap_or(u8::MAX),
                shard,
            })
            .collect()
    }
}

struct Group {
    originals: u8,
    recovery: u8,
    shards: BTreeMap<u8, Vec<u8>>,
    seen: Instant,
    done: bool,
}

#[derive(Default)]
pub struct Decoder {
    received: BTreeMap<u64, (Vec<u8>, Instant)>,
    groups: BTreeMap<u64, Group>,
    pub recovered: u64,
}

impl Decoder {
    pub fn data(&mut self, sequence: u64, payload: &[u8], now: Instant) -> Vec<(u64, Vec<u8>)> {
        self.expire(now);
        self.received.insert(sequence, (payload.to_vec(), now));
        let pending: Vec<u64> = self
            .groups
            .range(..=sequence)
            .rev()
            .take_while(|(base, group)| sequence < **base + u64::from(group.originals))
            .filter(|(_, group)| !group.done)
            .map(|(base, _)| *base)
            .collect();
        pending
            .into_iter()
            .flat_map(|base| self.recover(base))
            .collect()
    }

    pub fn parity(&mut self, parity: Parity, now: Instant) -> Vec<(u64, Vec<u8>)> {
        self.expire(now);
        if parity.originals == 0 || parity.index >= parity.recovery {
            return Vec::new();
        }
        let group = self.groups.entry(parity.base).or_insert_with(|| Group {
            originals: parity.originals,
            recovery: parity.recovery,
            shards: BTreeMap::new(),
            seen: now,
            done: false,
        });
        if group.originals != parity.originals || group.recovery != parity.recovery {
            return Vec::new();
        }
        group.shards.insert(parity.index, parity.shard);
        self.recover(parity.base)
    }

    fn recover(&mut self, base: u64) -> Vec<(u64, Vec<u8>)> {
        let Some(group) = self.groups.get_mut(&base) else {
            return Vec::new();
        };
        if group.done {
            return Vec::new();
        }
        let originals = usize::from(group.originals);
        let present: Vec<(usize, &Vec<u8>)> = (0..originals)
            .filter_map(|index| {
                self.received
                    .get(&(base + index as u64))
                    .map(|(payload, _)| (index, payload))
            })
            .collect();
        if present.len() == originals {
            group.done = true;
            return Vec::new();
        }
        if present.len() + group.shards.len() < originals {
            return Vec::new();
        }
        let Some(length) = group.shards.values().next().map(Vec::len) else {
            return Vec::new();
        };
        if present
            .iter()
            .any(|(_, payload)| shard_length(payload.len()) > length)
        {
            group.done = true;
            return Vec::new();
        }
        let restored = reed_solomon_simd::decode(
            originals,
            usize::from(group.recovery),
            present
                .iter()
                .map(|(index, payload)| (*index, shard(payload, length))),
            group
                .shards
                .iter()
                .map(|(index, shard)| (usize::from(*index), shard.clone())),
        );
        group.done = true;
        let Ok(restored) = restored else {
            return Vec::new();
        };
        let seen = group.seen;
        let recovered: Vec<(u64, Vec<u8>)> = restored
            .into_iter()
            .filter_map(|(index, shard)| Some((base + index as u64, unshard(&shard)?)))
            .collect();
        for (sequence, payload) in &recovered {
            self.received.insert(*sequence, (payload.clone(), seen));
        }
        self.recovered += recovered.len() as u64;
        recovered
    }

    fn expire(&mut self, now: Instant) {
        while self
            .received
            .first_key_value()
            .is_some_and(|(_, (_, at))| now.duration_since(*at) > RETAIN)
            || self.received.len() > MAX_RETAINED
        {
            self.received.pop_first();
        }
        while self
            .groups
            .first_key_value()
            .is_some_and(|(_, group)| now.duration_since(group.seen) > RETAIN)
            || self.groups.len() > MAX_RETAINED
        {
            self.groups.pop_first();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(originals: u8, recovery: u8) -> FecParams {
        FecParams {
            originals,
            recovery,
            window_ms: 20,
        }
    }

    fn payload(sequence: u64) -> Vec<u8> {
        let length = 40 + (sequence as usize * 7) % 30;
        (0..length)
            .map(|byte| (byte as u64 ^ sequence) as u8)
            .collect()
    }

    fn send(encoder: &mut Encoder, sequences: std::ops::Range<u64>, now: Instant) -> Vec<Parity> {
        sequences
            .flat_map(|sequence| encoder.push(sequence, &payload(sequence), now))
            .collect()
    }

    #[test]
    fn a_full_group_emits_its_parity_and_recovers_as_many_losses_as_it_has_parity() {
        let now = Instant::now();
        let mut encoder = Encoder::new(params(10, 2));
        let parity = send(&mut encoder, 100..110, now);
        assert_eq!(parity.len(), 2);
        assert!(parity.iter().all(|p| p.base == 100 && p.originals == 10));
        let mut decoder = Decoder::default();
        for sequence in (100..110).filter(|s| *s != 103 && *s != 108) {
            assert!(decoder.data(sequence, &payload(sequence), now).is_empty());
        }
        let mut recovered: Vec<_> = parity
            .into_iter()
            .flat_map(|p| decoder.parity(p, now))
            .collect();
        recovered.sort();
        assert_eq!(recovered, vec![(103, payload(103)), (108, payload(108))]);
        assert_eq!(decoder.recovered, 2);
    }

    #[test]
    fn more_losses_than_parity_recover_nothing() {
        let now = Instant::now();
        let mut encoder = Encoder::new(params(4, 1));
        let parity = send(&mut encoder, 0..4, now);
        let mut decoder = Decoder::default();
        decoder.data(0, &payload(0), now);
        decoder.data(3, &payload(3), now);
        assert!(
            parity
                .into_iter()
                .flat_map(|p| decoder.parity(p, now))
                .next()
                .is_none()
        );
    }

    #[test]
    fn a_window_flush_protects_a_partial_group() {
        let now = Instant::now();
        let mut encoder = Encoder::new(params(10, 1));
        assert!(send(&mut encoder, 7..10, now).is_empty());
        assert_eq!(encoder.due(), Some(now + Duration::from_millis(20)));
        let parity = encoder.flush();
        assert_eq!(parity.len(), 1);
        assert_eq!(parity[0].originals, 3);
        assert_eq!(encoder.due(), None);
        let mut decoder = Decoder::default();
        decoder.data(7, &payload(7), now);
        decoder.data(9, &payload(9), now);
        assert_eq!(
            decoder.parity(parity[0].clone(), now),
            vec![(8, payload(8))]
        );
    }

    #[test]
    fn parity_that_arrives_before_the_data_recovers_once_enough_data_arrives() {
        let now = Instant::now();
        let mut encoder = Encoder::new(params(3, 1));
        let parity = send(&mut encoder, 50..53, now);
        let mut decoder = Decoder::default();
        assert!(decoder.parity(parity[0].clone(), now).is_empty());
        assert!(decoder.data(50, &payload(50), now).is_empty());
        assert_eq!(decoder.data(52, &payload(52), now), vec![(51, payload(51))]);
        assert!(decoder.data(51, &payload(51), now).is_empty());
    }

    #[test]
    fn without_recovery_no_parity_is_sent() {
        let now = Instant::now();
        let mut encoder = Encoder::new(params(4, 0));
        assert!(send(&mut encoder, 0..8, now).is_empty());
        assert!(encoder.flush().is_empty());
    }
}
