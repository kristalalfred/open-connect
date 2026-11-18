use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use weave_core::{
    DeliveryReason, HopState, LocalEnd, LocalSocket, PayloadKind, SocketSpec, SocketStatus,
};

use crate::rtp::{Output, Tap};
use crate::runtime::Settings;
use crate::transport::{Input, SocketTask};

pub enum Endpoint {
    Srt(SocketTask),
    RtpInput(Tap),
    RtpOutput(Output),
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
            SocketSpec::Local(LocalSocket {
                end: LocalEnd::Input,
                id,
            }) => Self::RtpInput(settings.rtp.tap(id, path, sender, settings.clock.clone())),
            _ => unreachable!("validated native input"),
        }
    }

    #[must_use]
    pub fn output(spec: &SocketSpec, settings: &Settings) -> Self {
        match spec {
            SocketSpec::Srt(spec) => Self::Srt(SocketTask::output(spec.clone(), settings.bind_ip)),
            SocketSpec::Local(LocalSocket {
                end: LocalEnd::Output,
                id,
            }) => Self::RtpOutput(settings.rtp.output(id)),
            _ => unreachable!("validated native output"),
        }
    }

    #[must_use]
    pub fn matches(&self, spec: &SocketSpec) -> bool {
        match (self, spec) {
            (Self::Srt(task), SocketSpec::Srt(spec)) => task.spec == *spec,
            (
                Self::RtpInput(Tap { id: current, .. }),
                SocketSpec::Local(LocalSocket {
                    end: LocalEnd::Input,
                    id,
                }),
            )
            | (
                Self::RtpOutput(Output { id: current, .. }),
                SocketSpec::Local(LocalSocket {
                    end: LocalEnd::Output,
                    id,
                }),
            ) => current == id,
            _ => false,
        }
    }

    #[must_use]
    pub fn payload(&self) -> PayloadKind {
        match self {
            Self::Srt(task) => task.spec.params().payload,
            Self::RtpInput(_) | Self::RtpOutput(_) => PayloadKind::Raw,
        }
    }

    pub fn try_send(&self, bytes: Bytes, now_ns: u64) -> Result<(), DeliveryReason> {
        match self {
            Self::Srt(task) => task.try_send(bytes, now_ns),
            Self::RtpOutput(output) => {
                output.send(&bytes);
                Ok(())
            }
            Self::RtpInput(_) => Err(DeliveryReason::OutputUnavailable),
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
            Self::RtpInput(_) | Self::RtpOutput(_) => Err(DeliveryReason::OutputUnavailable),
        }
    }

    #[must_use]
    pub fn status(&self, now_ns: u64) -> SocketStatus {
        match self {
            Self::Srt(task) => task.status(now_ns),
            Self::RtpInput(tap) => tap.status(),
            Self::RtpOutput(output) => output.status(),
        }
    }

    #[must_use]
    pub fn take_drops(&self) -> u64 {
        match self {
            Self::Srt(task) => task.take_drops(),
            Self::RtpInput(tap) => tap.take_drops(),
            Self::RtpOutput(output) => output.take_drops(),
        }
    }

    #[must_use]
    pub fn state(&self) -> HopState {
        match self {
            Self::Srt(task) => task.state(),
            Self::RtpInput(tap) => tap.state(),
            Self::RtpOutput(output) => output.state(),
        }
    }

    #[must_use]
    pub fn latency(&self) -> Duration {
        match self {
            Self::Srt(task) => task.latency(),
            Self::RtpInput(_) | Self::RtpOutput(_) => Duration::ZERO,
        }
    }
}
