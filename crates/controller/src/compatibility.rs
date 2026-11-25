use connect_core::{
    FormatConstraint, FormatPolicy, HopRole, MediaBehavior, MediaFormat, NodeDescriptor, Path,
    StreamCondition, StreamConditionReason as Reason, StreamConditionStatus as Status,
    StreamConditionType, StreamDefinition,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compatibility {
    pub destinations: Vec<Verdict>,
    strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub id: String,
    pub condition: StreamCondition,
    pub declared: bool,
}

impl Compatibility {
    pub fn aggregate(&self) -> StreamCondition {
        aggregate(self.destinations.iter().map(|verdict| &verdict.condition))
    }

    pub fn for_stream(&self, stream: &StreamDefinition) -> StreamCondition {
        aggregate(self.within(stream).map(|verdict| &verdict.condition))
    }

    pub fn reported(&self) -> Option<StreamCondition> {
        self.report(self.destinations.iter())
    }

    pub fn reported_for_stream(&self, stream: &StreamDefinition) -> Option<StreamCondition> {
        self.report(self.within(stream))
    }

    fn within<'a>(&'a self, stream: &'a StreamDefinition) -> impl Iterator<Item = &'a Verdict> {
        self.destinations.iter().filter(|verdict| {
            stream
                .destinations
                .iter()
                .any(|destination| destination.id == verdict.id)
        })
    }

    fn report<'a>(&self, verdicts: impl Iterator<Item = &'a Verdict>) -> Option<StreamCondition> {
        let mut verdicts = verdicts
            .filter(|verdict| self.strict || verdict.declared)
            .peekable();
        verdicts.peek()?;
        Some(aggregate(verdicts.map(|verdict| &verdict.condition)))
    }
}

fn restricts(constraint: Option<&FormatConstraint>) -> bool {
    constraint.is_some_and(|constraint| *constraint != FormatConstraint::default())
}

fn compatible() -> StreamCondition {
    super::stream_condition(
        StreamConditionType::FormatCompatible,
        Status::True,
        Reason::FormatCompatible,
        "declared formats are compatible",
    )
}

fn rank(status: Status) -> u8 {
    match status {
        Status::True => 0,
        Status::Unknown => 1,
        Status::False => 2,
    }
}

fn aggregate<'a>(conditions: impl Iterator<Item = &'a StreamCondition>) -> StreamCondition {
    conditions.fold(compatible(), |result, next| {
        if rank(next.status) > rank(result.status) {
            next.clone()
        } else {
            result
        }
    })
}

fn record(result: &mut StreamCondition, status: Status, reason: Reason, detail: String) {
    if rank(status) > rank(result.status) {
        *result = super::stream_condition(
            StreamConditionType::FormatCompatible,
            status,
            reason,
            detail,
        );
    }
}

fn compare(
    result: &mut StreamCondition,
    format: Option<&MediaFormat>,
    accepts: Option<&FormatConstraint>,
    missing: Reason,
    boundary: &str,
) {
    match (format, accepts) {
        (_, None) => record(
            result,
            Status::Unknown,
            missing,
            format!("{boundary}: accepts is not declared"),
        ),
        (Some(format), Some(accepts)) => {
            let mismatches = accepts.mismatches(format);
            if !mismatches.is_empty() {
                record(
                    result,
                    Status::False,
                    Reason::FormatMismatch,
                    format!(
                        "{boundary}: {}",
                        mismatches
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join("; ")
                    ),
                );
            }
        }
        (None, Some(_)) => {}
    }
}

pub fn evaluate(
    stream: &StreamDefinition,
    path: Option<&Path>,
    nodes: &[NodeDescriptor],
) -> Compatibility {
    let mut destinations: Vec<_> = stream.destinations.iter().collect();
    destinations.sort_by(|left, right| left.id.cmp(&right.id));
    Compatibility {
        strict: stream.format_policy == FormatPolicy::Strict,
        destinations: destinations
            .into_iter()
            .map(|destination| {
                let mut result = compatible();
                let mut declared =
                    stream.source.format().is_some() || restricts(destination.endpoint.accepts());
                if stream.source.format().is_none() {
                    record(
                        &mut result,
                        Status::Unknown,
                        Reason::SourceFormatUnknown,
                        format!(
                            "destination {}: source format is not declared",
                            destination.id
                        ),
                    );
                }
                let Some(path) = path else {
                    record(
                        &mut result,
                        Status::Unknown,
                        Reason::FormatRouteUnknown,
                        format!(
                            "destination {}: no route is available to evaluate",
                            destination.id
                        ),
                    );
                    return Verdict {
                        id: destination.id.clone(),
                        condition: result,
                        declared,
                    };
                };
                let secondary = format!("{}.2", destination.id);
                let branches = std::iter::once(destination.id.as_str()).chain(
                    path.hops
                        .first()
                        .filter(|sender| {
                            sender
                                .egresses
                                .iter()
                                .any(|egress| egress.branch_id == secondary)
                        })
                        .map(|_| secondary.as_str()),
                );
                for branch in branches {
                    let mut format = stream.source.format();
                    let hops = path
                        .hops
                        .iter()
                        .filter(|hop| {
                            hop.role != HopRole::Receiver
                                && hop.egresses.iter().any(|egress| egress.branch_id == branch)
                        })
                        .chain(path.hops.iter().filter(|hop| {
                            hop.role == HopRole::Receiver
                                && hop
                                    .egresses
                                    .iter()
                                    .any(|egress| egress.branch_id == destination.id)
                        }));
                    let mut found = false;
                    for hop in hops {
                        found = true;
                        let boundary = format!(
                            "destination {} path {branch}, node {} profile {}",
                            destination.id, hop.node_id, hop.profile_id
                        );
                        let profile =
                            nodes
                                .iter()
                                .find(|node| node.id == hop.node_id)
                                .and_then(|node| {
                                    node.capabilities
                                        .hop_profiles
                                        .iter()
                                        .find(|profile| profile.id == hop.profile_id)
                                });
                        let Some(profile) = profile else {
                            record(
                                &mut result,
                                Status::Unknown,
                                Reason::FormatRouteUnknown,
                                format!("{boundary}: profile is unavailable"),
                            );
                            format = None;
                            continue;
                        };
                        declared |= restricts(profile.accepts.as_ref());
                        compare(
                            &mut result,
                            format,
                            profile.accepts.as_ref(),
                            Reason::HopAcceptsUnknown,
                            &boundary,
                        );
                        if profile.media_behavior != MediaBehavior::Preserve {
                            record(
                                &mut result,
                                Status::Unknown,
                                Reason::MediaBehaviorUnknown,
                                format!("{boundary}: media_behavior is unknown"),
                            );
                            format = None;
                        }
                    }
                    if !found {
                        record(
                            &mut result,
                            Status::Unknown,
                            Reason::FormatRouteUnknown,
                            format!(
                                "destination {}: no route is available to evaluate",
                                destination.id
                            ),
                        );
                        format = None;
                    }
                    compare(
                        &mut result,
                        format,
                        destination.endpoint.accepts(),
                        Reason::DestinationAcceptsUnknown,
                        &format!("destination {} path {branch}", destination.id),
                    );
                }
                Verdict {
                    id: destination.id.clone(),
                    condition: result,
                    declared,
                }
            })
            .collect(),
    }
}
