# Timed delivery and redundant paths

The native `connect-node` runtime implements fixed timed release, automatic
calibration and low-latency first-copy release over encrypted SRT links. Strom and browser nodes do not implement
these roles. Media stays in the nodes, outside the controller.

## Contract and manifest

A destination can request a delay between the managed origin sampling time
immediately after reading external ingress and the receiver submitting the
unwrapped payload to its final SRT socket's bounded send queue. Capture,
encoding, transport buffering before that ingress read, final transport,
decoding and display are outside this boundary.

```yaml
name: venue-to-studio
source:
  srt: { node: venue }
destinations:
  - id: studio
    paths: 2
    delivery:
      mode: fixed
      latency_ms: 500
      tolerance_ms: 5
    srt: { node: studio, latency: 100 }
```

For target `B` and tolerance `T`, successful release aims for `B`, with an
allowed age interval `[B - T, B + T]`. This requires bounded clock error and
at least one timely copy. It is a conditional timing contract on a general
purpose operating system. Network loss and scheduler pauses can violate it;
status and counters expose those failures. A 500 ms setting does not promise
500 ms from camera to display.

`delivery` requires managed SRT source and destination nodes. Remote endpoints,
inputs, outputs, WHIP, WHEP and RIST destinations reject it. Other destinations of the
same SRT source may remain untimed. Format declarations describe the original
payload; framing does not infer or convert media. Each destination schedules
independently from the same source timestamps.

| Field | Meaning |
|---|---|
| `delivery.mode` | Required: `fixed`, `auto` or [`low_latency`](#low-latency). |
| `latency_ms` | Fixed target, 1 to 60,000 ms. Required in fixed mode. |
| `tolerance_ms` | Defaults to 5 ms. Positive and less than the target or minimum. |
| `max_skew_ms` | Low latency only. Enables [path alignment](#path-alignment). |
| `paths` | Existing desired count, 1 or 2. Defaults to 1. |
| `srt.latency` | Separate per-connection SRT buffering, not the total delivery target. |

Unknown policy fields and fields from the other mode are rejected. Omitted
and explicit defaults are equal for no-op writes, generations and ETags.
Stream sets use the same policy validation and equality.

## Automatic calibration

```yaml
delivery:
  mode: auto
  min_latency_ms: 100
  max_latency_ms: 500
  tolerance_ms: 5
  calibration_ms: 5000
  max_late_ppm: 1000
  margin_ms: 20
```

Auto observes real traffic before releasing output. It chooses a delay once
and keeps it until the policy or source session changes. It does not adjust a
live schedule continuously. A policy change flushes queued records and starts
calibration again; this can interrupt output.

The minimum must exceed tolerance and not exceed the maximum; the maximum is
at most 60,000 ms. Calibration defaults to 5,000 ms, with a range of 1,000 to
60,000 ms. The late allowance defaults to 1,000 parts per million (0.1%) and
cannot exceed 100,000. Margin defaults to 20 ms and must be below the maximum.

Every provisioned input path is measured separately, including copies that
lose the merge race. Calibration waits at least its configured interval and
for enough observations: `clamp(ceil(1,000,000 / max_late_ppm), 32, 65,536)`, or 1,000
when the allowance is zero. Low-rate feeds can therefore take longer. At most
65,536 record observations are retained. Records newer than the maximum delay
are excluded from the decision so a missing slow copy has time to arrive.
Known sequence gaps and missing copies count against each path's allowance.

For each input, choose the observed age quantile that satisfies the allowance,
including clock and scheduling uncertainty. Add margin, take the slowest
path's requirement, round up to milliseconds and clamp to the minimum. If this
exceeds the ceiling, or too many copies are missing, report
`latency_budget_infeasible` and release nothing. Reapply a changed policy to
retry. With one provisioned path and two requested, calibration may select a
delay for that path, but redundancy remains false. Calibration evidence does
not guarantee future network conditions or identify the globally optimal delay.

## Low latency

```yaml
delivery:
  mode: low_latency
  max_latency_ms: 150
```

`low_latency` releases each record as soon as its first copy arrives, in
sequence order, instead of holding it to a scheduled delay. It needs no clock
domain. `max_latency_ms` is a ceiling on SRT buffering that open-connect
configures, from the source's ingress listener to the receiver's consumer
listener. Propagation delay, encoding and decoding are outside it.

The controller sets the consumer listener to 20 ms, and the ingress listener to
20 ms unless the source names `srt.latency`. It reserves 10 ms for the receiver
to wait on a missing record, and splits the rest evenly across each path's
internal links. A path whose links would get less than 20 ms each is refused
with `latency_budget_infeasible`. An explicit destination `srt.latency` sets
every internal link instead and must fit the ceiling.

A `low_latency` destination without `via` always tries a second path, which
does not require `paths: 2`. With one path it stays `flowing`, and
`redundancy_ready` names how many attempted paths carry media. `paths: 2` makes
redundancy required, as for the other modes.

When the next record in sequence is missing, the receiver waits up to 10 ms for
the other path's copy, then skips it and counts it as unrecovered. A copy that
arrives after its record was skipped counts as late. Release ages are reported
only while the origin and receiver share a clock domain.

### Path alignment

Without alignment, output follows the faster path. When the faster path fails,
output pauses for the difference in path delay and stays that much later. When
it returns, the receiver skips forward by the same amount.

```yaml
delivery:
  mode: low_latency
  max_latency_ms: 150
  max_skew_ms: 80
```

`max_skew_ms`, 1 to 10,000, enables alignment. For every record that arrives on
both paths, the receiver measures how much later the second copy arrives on its
own monotonic clock, so alignment needs no clock domain. It takes the median of
the last second of these measurements, at least 16, and holds the faster path's
copies by that skew, at most `max_skew_ms`. A record is released when its
earliest copy, plus that path's hold, is due. Output then follows the slower
path, and losing or regaining either path moves releases only by path jitter.
The cost is the slower path's delay on every record.

Until both paths have delivered enough records, nothing is held. When the hold
first applies, output pauses once by the measured skew. While a path is down,
the last measured skew stays in force. When a path resumes after 500 ms without
records, measurements are ignored for one second so that its backlog does not
count as path delay. During that second, while the other path delivers, the
resumed path's copies are held like the faster path's, so a reconnecting path
that briefly runs fast releases nothing early. When the skew exceeds `max_skew_ms`, the
receiver holds `max_skew_ms`, a failover pauses by the remainder, and
`redundancy_ready` is false with reason `skew_exceeded`. `max_skew_ms` is
separate from `max_latency_ms`, which bounds configured SRT buffering only.

## Identity and framing

SRT sequence numbers belong to individual connections. Timed routes use an
application envelope so independently terminated relays preserve one source
sequence space. The origin stamps once, splits ingress into payloads of at
most 1,024 bytes, and duplicates envelopes. Relays forward envelopes unchanged.
Receivers unwrap them. External payload bytes and order are preserved when
records arrive; message boundaries may change. No remuxing or transcoding occurs.

Each SRT message contains this 64-byte header followed by 1 to 1,024 bytes.
Unsigned integers use network byte order.

| Field | Bytes | Value |
|---|---:|---|
| `magic` | 4 | ASCII `WVTD` |
| `version` | 2 | `1` |
| `flags` | 2 | `0` |
| `delivery_id` | 16 | Source incarnation identifier |
| `epoch` | 8 | Authorized origin process epoch |
| `sequence` | 8 | Increasing from zero, without wrap |
| `origin_tai_ns` | 8 | Nanoseconds since 1970-01-01 00:00:00 TAI |
| `origin_uncertainty_ns` | 4 | Source timestamp error bound |
| `payload_length` | 4 | Following payload length |
| `reserved` | 8 | `0` |

The controller persists a `source_revision` when creating a stream or changing
its source. Destination changes preserve it. The in-memory store uses a random namespace
to avoid reusing source identities after controller state loss. The identifier is the first
16 bytes of SHA-256 over the fixed namespace, source revision, stream name and
serialized source. The native node generates a random nonzero 64-bit boot
epoch and advertises it in registration. Desired state authorizes both values
at each hop. A source restart needs controller authorization of its new epoch;
receivers never adopt an epoch from media alone. Sequence state survives hop
updates and temporary removal during the same node process. Source restart,
source replacement and stream recreation are discontinuities.

Validate version, reserved fields, length and authorized identity before
buffering. Conflicting copies of a buffered sequence stop that receiver with
`record_conflict`. Older records below the release watermark are discarded.
These checks do not authenticate an otherwise untrusted media node.

## Clock configuration

Origins and receivers need the same clock domain and a current error bound.
Forwarders need no synchronized clock. open-connect does not synchronize clocks.

A local demo may use:

```yaml
clock:
  provider: shared_host
  domain: demo
  tai_offset_secs: 37
```

Its advertised domain is `shared-host:demo`. This provider is only for nodes
sharing the same operating-system clock. It declares a 50 microsecond base
sampling bound and adds measured sampling duration. Using the same name on
separate hosts does not establish synchronization.

For separate hosts, configure a trusted external clock-status provider:

```yaml
clock:
  provider: external
  domain: production-tai
  status_file: /run/connect/clock.json
```

The external synchronization service must atomically replace that JSON file:

```json
{"domain":"production-tai","tai_offset_secs":37,"uncertainty_us":100,"observed_at_unix_ms":1792414800000,"valid_until_unix_ms":1792414801000,"max_drift_ppb":100}
```

Timestamps are Unix milliseconds. The node reloads the file every 250 ms and
rejects missing, malformed, mismatched, future or expired evidence. Error adds
sampling duration and worst-case drift from observation through a 60-second
scheduling horizon. Operators must supply a bound their clock service can
support, including the correct UTC-to-TAI offset. Merely running NTP supplies
no such evidence. Separate-host synchronization is not verified by local tests.

For every release, source uncertainty plus receiver uncertainty plus the
node's declared scheduling bound must fit tolerance. Deadlines use a local
monotonic clock projected from the original TAI target. Detected clock steps
or unavailable receiver bounds suspend release and flush pending media. Source
clock failure stops timed stamping while untimed branches can continue.

## Buffers, paths and admission

The receiver keeps a bounded ordered buffer, accepts the first valid copy,
and discards matching duplicates. A missing earlier record cannot delay a
later one past its original deadline. Each released payload carries its
scheduled release time as its SRT source time, so the final connection's
negotiated SRT latency absorbs late submission. A record submitted outside
tolerance counts as a scheduling violation. A record still pending after its
scheduled time plus that latency, or plus tolerance when larger, has expired
and is dropped. The runtime never expands a fixed or calibrated delay, replays
a backlog or manufactures replacement media. A gap can leave the original feed
undecodable.

Native defaults are 32,768 pending records, 32 MiB and a 1,000 microsecond
scheduling bound. Configure these under `media.max_buffer_records`,
`media.max_buffer_bytes` and `media.scheduling_error_bound_us`. Records and
bytes are bounded independently. `media.listen_ip` defaults to `0.0.0.0` and
controls native SRT binding. Each SRT sender has an 8 MiB packet buffer and each hop has a 256-message
ingress queue. Each transport has bounded queues; a slow branch
does not block the other branch. Output absence, backpressure and exhausted
buffers drop affected records and appear in status.

The controller requires explicit `connect_timed_v1` origin, forwarding and
receiver capabilities. Receivers share the origin's declared clock domain.
Internal SRT sockets carry an explicit payload kind and controller-derived
encryption keys. Ordinary Strom and browser profiles cannot satisfy a timed
route. Bare `paths: 2` does not enable timed framing or a raw native merger.

The planner rejects a path if its configured internal SRT buffering sum reaches
`maximum_ms - tolerance_ms`. Passing that check only establishes configured
headroom. Actual negotiated latency, network conditions and scheduling can
still miss deadlines. Auto does not tune individual SRT buffers. Set their
latency explicitly when requesting a small total budget.

Two paths retain existing attachment and relay exclusion rules. One eligible
path remains usable with reduced redundancy. Declarations do not prove carrier
independence. Selection between active copies is local and needs no controller
tick. Hop updates preserve unchanged sockets, the receiver watermark and its
schedule. Changes that replace sockets or policies can interrupt media.

## Status and verification

Southbound protocol version is 15. Native profiles advertise separate origin,
forward and receiver roles; the timed receiver advertises merge support and
the `modes` it releases.
Strom rejects timed desired hops; browser profiles advertise none. Rebuild
nodes and control-plane components together when upgrading.

Destination `delivery` reports the selected delay, state, reason, clock
uncertainty, buffer usage, the latest and extreme release ages, per-path ages
and counters.
An aligned `low_latency` receiver also reports the measured `skew_us` and the
applied `hold_us`.
Counters include releases, duplicates, late copies, known unrecovered records,
scheduling violations, buffer drops and invalid records. Only reports matching
the current desired revision count. Current origin faults also invalidate the
destination. Timing and redundancy are separate conditions and aggregate at
stream level. A timed stream cannot report `flowing` without current timing
evidence; missing redundancy makes it `degraded` even when output is timely.

Readiness uses a five-second monotonic observation window. A violation keeps
timing false until fresh compliant releases occur outside that window. Every
requested input needs recent timely records for redundancy. Idle sources
retain counters without inventing losses. Unknown clock values remain null.
Release-age observations use the configured clock evidence; they are not an
independent measurement of synchronization accuracy.

`just timed-demo` runs an isolated local four-node demo with two relayed SRT
paths, generated H264/AAC media, a decoder and fixed/auto controls. See the
[bench commands](../bench/README.md). Deterministic scheduler tests cover
merge and deadline behavior; real encrypted socket tests cover payload equality,
path loss and automatic calibration. Neither proves separate-host timing,
physical path independence or performance under arbitrary load.
