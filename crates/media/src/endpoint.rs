use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use weave_core::{DeliveryReason, HopState, PayloadKind, SocketSpec, SocketStatus};

use crate::runtime::Settings;
use crate::transport::{Input, SocketTask};

pub enum Endpoint {
    Srt(SocketTask),
}

impl Endpoint {
    #[must_use]
    pub fn input(
        spec: &SocketSpec,
        path: usize,
        sender: mpsc::Sender<Input>,
        settings: &Settings,
    ) -> Self {
        match spec {
            SocketSpec::Srt(spec) => Self::Srt(SocketTask::input(
                spec.clone(),
                path,
                sender,
                settings.clock.clone(),
                settings.bind_ip,
            )),
            _ => unreachable!("validated native input"),
        }
    }

    #[must_use]
    pub fn output(spec: &SocketSpec, settings: &Settings) -> Self {
        match spec {
            SocketSpec::Srt(spec) => Self::Srt(SocketTask::output(spec.clone(), settings.bind_ip)),
            _ => unreachable!("validated native output"),
        }
    }

    #[must_use]
    pub fn matches(&self, spec: &SocketSpec) -> bool {
        match (self, spec) {
            (Self::Srt(task), SocketSpec::Srt(spec)) => task.spec == *spec,
            _ => false,
        }
    }

    #[must_use]
    pub fn payload(&self) -> PayloadKind {
        match self {
            Self::Srt(task) => task.spec.params().payload,
        }
    }

    pub fn try_send(&self, bytes: Bytes, now_ns: u64) -> Result<(), DeliveryReason> {
        match self {
            Self::Srt(task) => task.try_send(bytes, now_ns),
        }
    }

    pub fn try_send_at(
        &self,
        bytes: Bytes,
        source_time: Instant,
        now_ns: u64,
    ) -> Result<(), DeliveryReason> {
        match self {
            Self::Srt(task) => task.try_send_at(bytes, source_time, now_ns),
        }
    }

    #[must_use]
    pub fn status(&self, now_ns: u64) -> SocketStatus {
        match self {
            Self::Srt(task) => task.status(now_ns),
        }
    }

    #[must_use]
    pub fn take_drops(&self) -> u64 {
        match self {
            Self::Srt(task) => task.take_drops(),
        }
    }

    #[must_use]
    pub fn state(&self) -> HopState {
        match self {
            Self::Srt(task) => task.state(),
        }
    }

    #[must_use]
    pub fn latency(&self) -> Duration {
        match self {
            Self::Srt(task) => task.latency(),
        }
    }
}
