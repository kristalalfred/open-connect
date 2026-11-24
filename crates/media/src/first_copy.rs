use std::collections::{BTreeMap, VecDeque};

use connect_core::{DeliveryPathReport, DeliveryReason, DeliveryReport, DeliveryState};

use crate::merge::{BufferLimits, ClockSample};
use crate::record::{HEADER_LENGTH, Record};

const NS_PER_MS: u64 = 1_000_000;
const HEALTH_WINDOW_NS: u64 = 5_000_000_000;
const SKIPPED_RANGES: usize = 64;
const SKEW_WINDOW_NS: u64 = 1_000_000_000;
const SKEW_UPDATE_NS: u64 = 100_000_000;
const SKEW_MIN_SAMPLES: usize = 16;
const SKEW_SAMPLES: usize = 4_096;
const SKEW_IDLE_NS: u64 = 500_000_000;
pub const UNKNOWN_UNCERTAINTY_NS: u32 = u32::MAX;

struct Arrival {
    record: Record,
    due_ns: u64,
}

struct Alignment {
    max_skew_ns: u64,
    first_copies: BTreeMap<u64, (usize, u64)>,
    samples: VecDeque<(u64, i64)>,
    lag_ns: Option<i64>,
    updated_ns: Option<u64>,
    last_seen_ns: [Option<u64>; 2],
    resumed_ns: [Option<u64>; 2],
}

impl Alignment {
    fn new(max_skew_ms: u32) -> Self {
        Self {
            max_skew_ns: u64::from(max_skew_ms) * NS_PER_MS,
            first_copies: BTreeMap::new(),
            samples: VecDeque::new(),
            lag_ns: None,
            updated_ns: None,
            last_seen_ns: [None; 2],
            resumed_ns: [None; 2],
        }
    }

    fn observe(&mut self, sequence: u64, path: usize, now_ns: u64, capacity: usize) {
        if self.last_seen_ns[path].is_some_and(|last| now_ns.saturating_sub(last) >= SKEW_IDLE_NS) {
            self.resumed_ns[path] = Some(now_ns);
        }
        self.last_seen_ns[path] = Some(now_ns);
        let settling = self
            .resumed_ns
            .iter()
            .flatten()
            .any(|resumed| now_ns.saturating_sub(*resumed) < SKEW_WINDOW_NS);
        let horizon_ns = self.max_skew_ns + SKEW_WINDOW_NS;
        while let Some((_, (_, first_ns))) = self.first_copies.first_key_value() {
            if now_ns.saturating_sub(*first_ns) <= horizon_ns && self.first_copies.len() < capacity
            {
                break;
            }
            self.first_copies.pop_first();
        }
        match self.first_copies.get(&sequence) {
            Some((first_path, _)) if *first_path == path => {}
            Some(_) if settling => {
                self.first_copies.remove(&sequence);
            }
            Some(&(_, first_ns)) => {
                self.first_copies.remove(&sequence);
                let gap = i64::try_from(now_ns.saturating_sub(first_ns)).unwrap_or(i64::MAX);
                self.samples
                    .push_back((now_ns, if path == 1 { gap } else { -gap }));
                if self.samples.len() > SKEW_SAMPLES {
                    self.samples.pop_front();
                }
            }
            None => {
                self.first_copies.insert(sequence, (path, now_ns));
            }
        }
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now_ns.saturating_sub(*at) > SKEW_WINDOW_NS)
        {
            self.samples.pop_front();
        }
        if self.samples.len() >= SKEW_MIN_SAMPLES
            && self
                .updated_ns
                .is_none_or(|updated| now_ns.saturating_sub(updated) >= SKEW_UPDATE_NS)
        {
            let mut lags: Vec<i64> = self.samples.iter().map(|(_, lag)| *lag).collect();
            let middle = lags.len() / 2;
            self.lag_ns = Some(*lags.select_nth_unstable(middle).1);
            self.updated_ns = Some(now_ns);
        }
    }

    fn skew_ns(&self) -> Option<u64> {
        self.lag_ns.map(i64::unsigned_abs)
    }

    fn hold_ns(&self) -> u64 {
        self.skew_ns().unwrap_or(0).min(self.max_skew_ns)
    }

    fn offset_ns(&self, path: usize, now_ns: u64) -> u64 {
        let settling = self.resumed_ns[path]
            .is_some_and(|resumed| now_ns.saturating_sub(resumed) < SKEW_WINDOW_NS);
        let other_live = self.last_seen_ns[1 - path]
            .is_some_and(|seen| now_ns.saturating_sub(seen) < SKEW_IDLE_NS);
        match self.lag_ns {
            Some(_) if settling && other_live => self.hold_ns(),
            Some(lag) if (lag > 0 && path == 0) || (lag < 0 && path == 1) => self.hold_ns(),
            _ => 0,
        }
    }

    fn exceeded(&self) -> bool {
        self.skew_ns().is_some_and(|skew| skew > self.max_skew_ns)
    }
}

pub struct FirstCopy {
    wait_ns: u64,
    input_paths: usize,
    requested_paths: usize,
    limits: BufferLimits,
    alignment: Option<Alignment>,
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
        max_skew_ms: Option<u32>,
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
            alignment: max_skew_ms.map(Alignment::new),
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

    pub fn reconfigure(
        &mut self,
        wait_ms: u32,
        max_skew_ms: Option<u32>,
        input_paths: usize,
        requested_paths: usize,
    ) {
        assert!((1..=2).contains(&input_paths));
        self.wait_ns = u64::from(wait_ms) * NS_PER_MS;
        self.input_paths = input_paths;
        self.requested_paths = requested_paths;
        match (&mut self.alignment, max_skew_ms) {
            (Some(alignment), Some(max_skew_ms)) => {
                alignment.max_skew_ns = u64::from(max_skew_ms) * NS_PER_MS;
            }
            (alignment, max_skew_ms) => *alignment = max_skew_ms.map(Alignment::new),
        }
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
        if let Some(alignment) = &mut self.alignment {
            alignment.observe(record.sequence, path, now_ns, self.limits.records);
        }
        let due_ns = now_ns.saturating_add(
            self.alignment
                .as_ref()
                .map_or(0, |alignment| alignment.offset_ns(path, now_ns)),
        );
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
        if let Some(existing) = self.pending.get_mut(&record.sequence) {
            if existing.record == record {
                existing.due_ns = existing.due_ns.min(due_ns);
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
        self.pending
            .insert(record.sequence, Arrival { record, due_ns });
    }

    fn expected(&self, first: u64) -> u64 {
        self.watermark.map_or(first, |watermark| watermark + 1)
    }

    #[must_use]
    pub fn next_deadline_ns(&self) -> Option<u64> {
        let (sequence, arrival) = self.pending.first_key_value()?;
        Some(if *sequence == self.expected(*sequence) {
            arrival.due_ns
        } else {
            arrival.due_ns.saturating_add(self.wait_ns)
        })
    }

    pub fn take_ready(&mut self, now_ns: u64) -> Option<Record> {
        if self.report.state == DeliveryState::Failed {
            return None;
        }
        let (&sequence, arrival) = self.pending.first_key_value()?;
        let expected = self.expected(sequence);
        if sequence == expected && now_ns < arrival.due_ns {
            return None;
        }
        if sequence != expected {
            if now_ns < arrival.due_ns.saturating_add(self.wait_ns) {
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
            self.report.latest_release_age_us = Some(age_us);
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
        let skew_exceeded = self.alignment.as_ref().is_some_and(|alignment| {
            report.skew_us = alignment.skew_ns().map(|skew| skew / 1_000);
            report.hold_us = Some(alignment.hold_ns() / 1_000);
            alignment.exceeded()
        });
        if skew_exceeded {
            report.redundancy_ready = false;
        }
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
            report.reason = if skew_exceeded {
                DeliveryReason::SkewExceeded
            } else {
                DeliveryReason::Timely
            };
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
        FirstCopy::new(10, None, paths, 2, BufferLimits::default())
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

    struct Links {
        lag_ms: [u64; 2],
        down: [std::ops::Range<u64>; 2],
        backlog: u64,
        rush: Option<(usize, std::ops::Range<u64>, u64)>,
    }

    fn run_aligned(receiver: &mut FirstCopy, links: &Links, records: u64) -> BTreeMap<u64, u64> {
        let mut released = BTreeMap::new();
        let horizon = records + links.lag_ms.iter().max().unwrap() + 20;
        for now in 0..horizon {
            for path in 0..2 {
                let Some(sequence) = now.checked_sub(links.lag_ms[path]) else {
                    continue;
                };
                let down = &links.down[path];
                if sequence == down.end && !down.is_empty() {
                    for old in down.end.saturating_sub(links.backlog)..down.end {
                        receiver.ingest(record(old), path, now * MS, None);
                    }
                }
                let rushed = |sequence: u64| {
                    links.rush.as_ref().is_some_and(|(rushed, range, _)| {
                        *rushed == path && range.contains(&sequence)
                    })
                };
                if sequence < records && !down.contains(&sequence) && !rushed(sequence) {
                    receiver.ingest(record(sequence), path, now * MS, None);
                }
                if let Some((_, _, early_ms)) = &links.rush
                    && let Some(sequence) = (now + early_ms).checked_sub(links.lag_ms[path])
                    && rushed(sequence)
                {
                    receiver.ingest(record(sequence), path, now * MS, None);
                }
            }
            for sequence in drain(receiver, now * MS) {
                released.insert(sequence, now);
            }
        }
        released
    }

    #[test]
    fn aligned_release_holds_the_faster_path_so_losing_and_regaining_it_moves_no_record() {
        let mut receiver = FirstCopy::new(10, Some(80), 2, 2, BufferLimits::default());
        let links = Links {
            lag_ms: [5, 45],
            down: [400..600, 0..0],
            backlog: 0,
            rush: None,
        };
        let released = run_aligned(&mut receiver, &links, 1_000);
        for sequence in 200..1_000 {
            assert_eq!(
                released.get(&sequence),
                Some(&(sequence + 45)),
                "record {sequence}"
            );
        }
        let report = receiver.report(1_000 * MS);
        assert_eq!(report.counters.unrecovered, 0);
        assert_eq!(report.counters.late_arrivals, 0);
        assert_eq!(report.skew_us, Some(40_000));
        assert_eq!(report.hold_us, Some(40_000));
        assert_eq!(report.reason, DeliveryReason::Timely);
    }

    #[test]
    fn a_returning_slower_path_that_briefly_runs_fast_releases_nothing_early() {
        let mut receiver = FirstCopy::new(10, Some(80), 2, 2, BufferLimits::default());
        let links = Links {
            lag_ms: [5, 45],
            down: [0..0, 400..1_400],
            backlog: 0,
            rush: Some((1, 1_400..1_410, 25)),
        };
        let released = run_aligned(&mut receiver, &links, 2_000);
        for sequence in 200..2_000 {
            assert_eq!(
                released.get(&sequence),
                Some(&(sequence + 45)),
                "record {sequence}"
            );
        }
        assert_eq!(receiver.report(2_000 * MS).counters.unrecovered, 0);
    }

    #[test]
    fn aligned_release_holds_whichever_path_is_faster_and_survives_losing_the_slower_one() {
        let mut receiver = FirstCopy::new(10, Some(80), 2, 2, BufferLimits::default());
        let links = Links {
            lag_ms: [30, 0],
            down: [400..600, 0..0],
            backlog: 0,
            rush: None,
        };
        let released = run_aligned(&mut receiver, &links, 1_000);
        for sequence in 200..1_000 {
            assert_eq!(
                released.get(&sequence),
                Some(&(sequence + 30)),
                "record {sequence}"
            );
        }
        assert_eq!(receiver.report(1_000 * MS).counters.unrecovered, 0);
        let mut receiver = FirstCopy::new(10, Some(80), 2, 2, BufferLimits::default());
        let links = Links {
            lag_ms: [30, 0],
            down: [0..0, 400..600],
            backlog: 0,
            rush: None,
        };
        let released = run_aligned(&mut receiver, &links, 1_000);
        for sequence in 200..1_000 {
            assert_eq!(
                released.get(&sequence),
                Some(&(sequence + 30)),
                "record {sequence}"
            );
        }
        assert_eq!(receiver.report(1_000 * MS).counters.unrecovered, 0);
    }

    #[test]
    fn a_backlog_a_returning_path_delivers_does_not_move_the_hold() {
        let mut receiver = FirstCopy::new(10, Some(80), 2, 2, BufferLimits::default());
        let links = Links {
            lag_ms: [5, 45],
            down: [400..1_000, 0..0],
            backlog: 600,
            rush: None,
        };
        let released = run_aligned(&mut receiver, &links, 2_000);
        for sequence in 200..2_000 {
            assert_eq!(
                released.get(&sequence),
                Some(&(sequence + 45)),
                "record {sequence}"
            );
        }
        let report = receiver.report(2_000 * MS);
        assert_eq!(report.counters.unrecovered, 0);
        assert_eq!(report.hold_us, Some(40_000));
    }

    #[test]
    fn a_skew_beyond_the_allowance_holds_only_the_allowance_and_reports_redundancy_lost() {
        let mut receiver = FirstCopy::new(10, Some(80), 2, 2, BufferLimits::default());
        let links = Links {
            lag_ms: [0, 120],
            down: [0..0, 0..0],
            backlog: 0,
            rush: None,
        };
        let released = run_aligned(&mut receiver, &links, 1_000);
        assert_eq!(released.get(&500), Some(&580));
        let report = receiver.report(1_000 * MS);
        assert_eq!(report.skew_us, Some(120_000));
        assert_eq!(report.hold_us, Some(80_000));
        assert!(report.timing_ready && !report.redundancy_ready);
        assert_eq!(report.reason, DeliveryReason::SkewExceeded);
    }

    #[test]
    fn without_an_allowance_the_faster_path_is_released_at_once() {
        let mut receiver = receiver(2);
        let links = Links {
            lag_ms: [5, 45],
            down: [0..0, 0..0],
            backlog: 0,
            rush: None,
        };
        let released = run_aligned(&mut receiver, &links, 300);
        assert_eq!(released.get(&200), Some(&205));
        let report = receiver.report(300 * MS);
        assert_eq!((report.skew_us, report.hold_us), (None, None));
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
        let mut receiver = FirstCopy::new(10, None, 1, 2, BufferLimits::default());
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
        assert_eq!(report.latest_release_age_us, None);
        assert_eq!(report.clock_uncertainty_us, Some(2));
    }
}
