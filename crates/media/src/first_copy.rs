use std::collections::{BTreeMap, VecDeque};

use connect_core::{DeliveryPathReport, DeliveryReason, DeliveryReport, DeliveryState};

use crate::merge::{BufferLimits, ClockSample};
use crate::record::{HEADER_LENGTH, Record};

const NS_PER_MS: u64 = 1_000_000;
const HEALTH_WINDOW_NS: u64 = 5_000_000_000;
const SKIPPED_RANGES: usize = 64;
pub const UNKNOWN_UNCERTAINTY_NS: u32 = u32::MAX;

struct Arrival {
    record: Record,
    arrived_ns: u64,
}

pub struct FirstCopy {
    wait_ns: u64,
    input_paths: usize,
    requested_paths: usize,
    limits: BufferLimits,
    pending: BTreeMap<u64, Arrival>,
    pending_bytes: usize,
    watermark: Option<u64>,
    skipped: VecDeque<(u64, u64)>,
    last_received: [Option<u64>; 2],
    last_release_ns: Option<u64>,
    last_fault_ns: Option<u64>,
    report: DeliveryReport,
}

impl FirstCopy {
    #[must_use]
    pub fn new(
        wait_ms: u32,
        input_paths: usize,
        requested_paths: usize,
        limits: BufferLimits,
    ) -> Self {
        assert!((1..=2).contains(&input_paths));
        Self {
            wait_ns: u64::from(wait_ms) * NS_PER_MS,
            input_paths,
            requested_paths,
            limits,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            watermark: None,
            skipped: VecDeque::new(),
            last_received: [None; 2],
            last_release_ns: None,
            last_fault_ns: None,
            report: DeliveryReport {
                paths: vec![DeliveryPathReport::default(); input_paths],
                ..DeliveryReport::default()
            },
        }
    }

    pub fn reconfigure(&mut self, input_paths: usize, requested_paths: usize) {
        assert!((1..=2).contains(&input_paths));
        self.input_paths = input_paths;
        self.requested_paths = requested_paths;
        self.report
            .paths
            .resize(input_paths, DeliveryPathReport::default());
    }

    pub fn reset_path(&mut self, path: usize) {
        if path < self.last_received.len() {
            self.last_received[path] = None;
        }
    }

    pub fn invalid_record(&mut self) {
        self.report.counters.invalid_records += 1;
    }

    pub fn transport_dropped(&mut self, count: u64) {
        self.report.counters.buffer_drops = self.report.counters.buffer_drops.saturating_add(count);
    }

    fn fault(&mut self, reason: DeliveryReason, now_ns: u64) {
        self.last_fault_ns = Some(now_ns);
        self.report.reason = reason;
    }

    fn was_skipped(&self, sequence: u64) -> bool {
        self.skipped
            .iter()
            .any(|(first, last)| (*first..=*last).contains(&sequence))
    }

    pub fn ingest(&mut self, record: Record, path: usize, now_ns: u64, clock: Option<ClockSample>) {
        if path >= self.input_paths || self.report.state == DeliveryState::Failed {
            self.report.counters.invalid_records += 1;
            return;
        }
        self.last_received[path] = Some(now_ns);
        let report = &mut self.report.paths[path];
        report.received += 1;
        if let Some(clock) =
            clock.filter(|_| record.origin_uncertainty_ns != UNKNOWN_UNCERTAINTY_NS)
        {
            report.latest_age_us = Some(clock.tai_ns.saturating_sub(record.origin_tai_ns) / 1_000);
            self.report.clock_uncertainty_us = Some(clock.uncertainty_ns.div_ceil(1_000));
        }
        if self
            .watermark
            .is_some_and(|watermark| record.sequence <= watermark)
        {
            if self.was_skipped(record.sequence) {
                self.report.paths[path].late += 1;
                self.report.counters.late_arrivals += 1;
            } else {
                self.report.counters.duplicates += 1;
            }
            return;
        }
        if let Some(existing) = self.pending.get(&record.sequence) {
            if existing.record == record {
                self.report.counters.duplicates += 1;
            } else {
                self.pending.clear();
                self.pending_bytes = 0;
                self.report.state = DeliveryState::Failed;
                self.report.reason = DeliveryReason::RecordConflict;
                self.report.timing_ready = false;
            }
            return;
        }
        let expected = self.watermark.map(|watermark| watermark + 1).or_else(|| {
            self.pending
                .first_key_value()
                .map(|(sequence, _)| *sequence)
        });
        if expected
            .is_some_and(|expected| record.sequence.abs_diff(expected) > self.limits.records as u64)
        {
            self.report.counters.invalid_records += 1;
            return;
        }
        let size = HEADER_LENGTH + record.payload.len();
        if self.pending.len() >= self.limits.records
            || self.pending_bytes.saturating_add(size) > self.limits.bytes
        {
            self.report.counters.buffer_drops += 1;
            self.fault(DeliveryReason::BufferExhausted, now_ns);
            return;
        }
        self.pending_bytes += size;
        self.pending.insert(
            record.sequence,
            Arrival {
                record,
                arrived_ns: now_ns,
            },
        );
    }

    fn expected(&self, first: u64) -> u64 {
        self.watermark.map_or(first, |watermark| watermark + 1)
    }

    #[must_use]
    pub fn next_deadline_ns(&self) -> Option<u64> {
        let (sequence, arrival) = self.pending.first_key_value()?;
        Some(if *sequence == self.expected(*sequence) {
            arrival.arrived_ns
        } else {
            arrival.arrived_ns.saturating_add(self.wait_ns)
        })
    }

    pub fn take_ready(&mut self, now_ns: u64) -> Option<Record> {
        if self.report.state == DeliveryState::Failed {
            return None;
        }
        let (&sequence, arrival) = self.pending.first_key_value()?;
        let expected = self.expected(sequence);
        if sequence != expected {
            if now_ns.saturating_sub(arrival.arrived_ns) < self.wait_ns {
                return None;
            }
            self.report.counters.unrecovered = self
                .report
                .counters
                .unrecovered
                .saturating_add(sequence - expected);
            self.skipped.push_back((expected, sequence - 1));
            if self.skipped.len() > SKIPPED_RANGES {
                self.skipped.pop_front();
            }
            self.fault(DeliveryReason::DeadlineMissed, now_ns);
        }
        let arrival = self.pending.pop_first()?.1;
        self.pending_bytes -= HEADER_LENGTH + arrival.record.payload.len();
        self.watermark = Some(sequence);
        Some(arrival.record)
    }

    pub fn released(&mut self, record: &Record, now_ns: u64, clock: Option<ClockSample>) {
        self.report.counters.released += 1;
        self.last_release_ns = Some(now_ns);
        if let Some(clock) =
            clock.filter(|_| record.origin_uncertainty_ns != UNKNOWN_UNCERTAINTY_NS)
        {
            let age_us = clock.tai_ns.saturating_sub(record.origin_tai_ns) / 1_000;
            self.report.minimum_release_age_us = Some(
                self.report
                    .minimum_release_age_us
                    .map_or(age_us, |previous| previous.min(age_us)),
            );
            self.report.maximum_release_age_us = Some(
                self.report
                    .maximum_release_age_us
                    .map_or(age_us, |previous| previous.max(age_us)),
            );
        }
    }

    pub fn output_failed(&mut self, reason: DeliveryReason, now_ns: u64) {
        self.report.counters.unrecovered += 1;
        self.fault(reason, now_ns);
    }

    #[must_use]
    pub fn report(&self, now_ns: u64) -> DeliveryReport {
        let mut report = self.report.clone();
        report.buffer_bytes = self.pending_bytes as u64;
        report.buffer_records = self.pending.len() as u64;
        let recent = |value: Option<u64>| {
            value.is_some_and(|then| now_ns.saturating_sub(then) <= HEALTH_WINDOW_NS)
        };
        for (path, last) in report.paths.iter_mut().zip(self.last_received) {
            path.qualified = recent(last);
        }
        report.redundancy_ready = self.input_paths == self.requested_paths
            && report.paths.iter().all(|path| path.qualified);
        if report.state == DeliveryState::Failed {
            report.timing_ready = false;
            return report;
        }
        (report.state, report.timing_ready) = if !recent(self.last_release_ns) {
            report.reason = DeliveryReason::AwaitingInput;
            (DeliveryState::AwaitingInput, false)
        } else if recent(self.last_fault_ns) {
            (DeliveryState::Degraded, false)
        } else {
            report.reason = DeliveryReason::Timely;
            (DeliveryState::Ready, true)
        };
        report
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    const MS: u64 = NS_PER_MS;

    fn record(sequence: u64) -> Record {
        Record {
            delivery_id: [7; 16],
            epoch: 1,
            sequence,
            origin_tai_ns: 1_000 * MS + sequence * MS,
            origin_uncertainty_ns: 1_000,
            payload: Bytes::from(vec![sequence as u8; 16]),
        }
    }

    fn receiver(paths: usize) -> FirstCopy {
        FirstCopy::new(10, paths, 2, BufferLimits::default())
    }

    fn drain(receiver: &mut FirstCopy, now_ns: u64) -> Vec<u64> {
        let mut released = Vec::new();
        while let Some(record) = receiver.take_ready(now_ns) {
            receiver.released(&record, now_ns, None);
            released.push(record.sequence);
        }
        released
    }

    #[test]
    fn the_first_copy_of_each_record_is_released_at_once_and_the_other_is_a_duplicate() {
        let mut receiver = receiver(2);
        receiver.ingest(record(0), 0, 0, None);
        receiver.ingest(record(1), 1, MS, None);
        assert_eq!(drain(&mut receiver, MS), vec![0, 1]);
        receiver.ingest(record(1), 0, 2 * MS, None);
        receiver.ingest(record(0), 1, 2 * MS, None);
        assert_eq!(drain(&mut receiver, 2 * MS), Vec::<u64>::new());
        let report = receiver.report(2 * MS);
        assert_eq!(report.counters.released, 2);
        assert_eq!(report.counters.duplicates, 2);
        assert_eq!(report.counters.unrecovered, 0);
        assert!(report.timing_ready && report.redundancy_ready);
    }

    #[test]
    fn a_gap_waits_for_the_other_path_and_is_filled_when_its_copy_arrives_in_time() {
        let mut receiver = receiver(2);
        receiver.ingest(record(0), 0, 0, None);
        receiver.ingest(record(2), 0, MS, None);
        assert_eq!(drain(&mut receiver, MS), vec![0]);
        assert_eq!(receiver.next_deadline_ns(), Some(11 * MS));
        receiver.ingest(record(1), 1, 4 * MS, None);
        assert_eq!(drain(&mut receiver, 4 * MS), vec![1, 2]);
        assert_eq!(receiver.report(4 * MS).counters.unrecovered, 0);
    }

    #[test]
    fn a_gap_no_path_fills_is_skipped_after_the_wait_and_its_late_copy_is_counted_late() {
        let mut receiver = receiver(2);
        receiver.ingest(record(0), 0, 0, None);
        receiver.ingest(record(3), 0, MS, None);
        assert_eq!(drain(&mut receiver, 10 * MS), vec![0]);
        assert_eq!(drain(&mut receiver, 11 * MS), vec![3]);
        receiver.ingest(record(2), 1, 12 * MS, None);
        let report = receiver.report(12 * MS);
        assert_eq!(report.counters.unrecovered, 2);
        assert_eq!(report.counters.late_arrivals, 1);
        assert_eq!(report.paths[1].late, 1);
        assert_eq!(report.state, DeliveryState::Degraded);
        assert_eq!(report.reason, DeliveryReason::DeadlineMissed);
        assert_eq!(
            receiver.report(11 * MS + HEALTH_WINDOW_NS + 1).state,
            DeliveryState::AwaitingInput
        );
    }

    #[test]
    fn conflicting_copies_of_one_record_fail_the_receiver() {
        let mut receiver = receiver(2);
        receiver.ingest(record(1), 0, 0, None);
        let mut other = record(1);
        other.payload = Bytes::from_static(b"different");
        receiver.ingest(other, 1, 0, None);
        let report = receiver.report(0);
        assert_eq!(report.state, DeliveryState::Failed);
        assert_eq!(report.reason, DeliveryReason::RecordConflict);
        assert!(receiver.take_ready(0).is_none());
    }

    #[test]
    fn one_path_releases_without_redundancy_and_ages_need_a_stamped_origin() {
        let mut receiver = FirstCopy::new(10, 1, 2, BufferLimits::default());
        let clock = ClockSample {
            tai_ns: 1_040 * MS,
            monotonic_ns: 0,
            uncertainty_ns: 2_000,
        };
        receiver.ingest(record(0), 0, 0, Some(clock));
        let mut unstamped = record(1);
        unstamped.origin_uncertainty_ns = UNKNOWN_UNCERTAINTY_NS;
        receiver.ingest(unstamped, 0, MS, Some(clock));
        assert_eq!(drain(&mut receiver, MS), vec![0, 1]);
        let report = receiver.report(MS);
        assert!(report.timing_ready && !report.redundancy_ready);
        assert_eq!(report.paths[0].latest_age_us, Some(40_000));
        assert_eq!(report.clock_uncertainty_us, Some(2));
    }
}
