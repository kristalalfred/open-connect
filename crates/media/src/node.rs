use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use connect_core::auth::Token;
use connect_core::{
    DeliveryCapability, DeliveryRole, DesiredHop, FormatConstraint, HopEndpointClass, HopProfile,
    HopStatus, LocalEnd, MediaBehavior, NodeCapabilities, NodeConfig, NodeDescriptor,
    NodeHeartbeat, NodeRegistration, NodeStatus, PROTOCOL_VERSION, RoleSet, Transport,
    TransportClass,
};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use serde_json::json;

use crate::clock::{Clock, ClockConfig};
use crate::merge::BufferLimits;
use crate::rtp::{Rtp, RtpConfig};
use crate::runtime::{Hop, OriginState, RTP_TO_SRT, SRT_FORWARD, SRT_TO_RTP, Settings};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    pub clock: ClockConfig,
    #[serde(default)]
    pub media: MediaConfig,
    #[serde(default)]
    pub rtp: RtpConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MediaConfig {
    pub listen_ip: IpAddr,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
    pub scheduling_error_bound_us: u32,
}

impl Default for MediaConfig {
    fn default() -> Self {
        let limits = BufferLimits::default();
        Self {
            listen_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            max_buffer_records: limits.records,
            max_buffer_bytes: limits.bytes,
            scheduling_error_bound_us: limits.scheduling_error_us,
        }
    }
}

impl MediaConfig {
    fn limits(&self) -> BufferLimits {
        BufferLimits {
            records: self.max_buffer_records,
            bytes: self.max_buffer_bytes,
            scheduling_error_us: self.scheduling_error_bound_us,
        }
    }
}

#[must_use]
pub fn profiles(clock_domain: &str, epoch: u64, scheduling_error_bound_us: u32) -> Vec<HopProfile> {
    let class = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    let untimed = |id: &str, ingress, egress, max_egresses, merge| HopProfile {
        id: id.into(),
        ingress,
        egress,
        max_egresses,
        media_behavior: MediaBehavior::Preserve,
        merge,
        accepts: Some(FormatConstraint::default()),
        timed_delivery: None,
    };
    let mut profiles = vec![
        untimed(SRT_FORWARD, class(), class(), None, false),
        untimed(
            RTP_TO_SRT,
            HopEndpointClass::Local(LocalEnd::Input),
            class(),
            None,
            false,
        ),
        untimed(
            SRT_TO_RTP,
            class(),
            HopEndpointClass::Local(LocalEnd::Output),
            Some(1),
            true,
        ),
    ];
    for (id, role, merge) in [
        ("srt-timed-origin", DeliveryRole::Origin, false),
        ("srt-timed-forward", DeliveryRole::Forward, false),
        ("srt-timed-receiver", DeliveryRole::Receiver, true),
    ] {
        profiles.push(HopProfile {
            id: id.into(),
            ingress: class(),
            egress: class(),
            max_egresses: (role == DeliveryRole::Receiver).then_some(1),
            media_behavior: MediaBehavior::Preserve,
            merge,
            accepts: Some(FormatConstraint::default()),
            timed_delivery: Some(DeliveryCapability {
                protocol: connect_core::delivery::TIMED_PROTOCOL.into(),
                roles: vec![role],
                max_latency_ms: 60_000,
                scheduling_error_bound_us,
                clock_domain: clock_domain.into(),
                origin_epoch: epoch,
            }),
        });
    }
    profiles
}

struct Control {
    client: Client,
    url: String,
    token: Option<Token>,
    registered: bool,
}

impl Control {
    fn authorize(&self, builder: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => builder.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => builder,
        }
    }

    async fn synchronize(
        &mut self,
        registration: NodeRegistration,
        hops: &mut BTreeMap<String, Hop>,
        origins: &mut BTreeMap<String, Arc<Mutex<OriginState>>>,
        settings: &Settings,
    ) -> Result<()> {
        if !self.registered {
            self.authorize(self.client.post(format!("{}/nodes/register", self.url)))
                .json(&registration)
                .send()
                .await?
                .error_for_status()?;
            self.registered = true;
        }
        let response = self
            .authorize(self.client.get(format!(
                "{}/nodes/{}/desired",
                self.url, registration.node.id
            )))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.registered = false;
            return Ok(());
        }
        let desired: Vec<DesiredHop> = response.error_for_status()?.json().await?;
        anyhow::ensure!(
            desired
                .iter()
                .all(|hop| hop.node_id == registration.node.id),
            "controller returned another node's desired hops"
        );
        hops.retain(|id, _| desired.iter().any(|hop| &hop.id == id));
        for desired in desired {
            if let Some(timing) = desired
                .delivery
                .as_ref()
                .filter(|timing| timing.role == DeliveryRole::Origin)
            {
                let authorized = registration
                    .node
                    .capabilities
                    .hop_profiles
                    .iter()
                    .filter_map(|profile| profile.timed_delivery.as_ref())
                    .any(|capability| {
                        capability.roles.contains(&DeliveryRole::Origin)
                            && capability.origin_epoch == timing.epoch
                    });
                anyhow::ensure!(
                    authorized,
                    "waiting for the controller to authorize the current origin epoch"
                );
            }
            if let Some(hop) = hops.get_mut(&desired.id) {
                hop.update(desired, settings)?;
            } else {
                let origin = origins.entry(desired.id.clone()).or_default().clone();
                hops.insert(
                    desired.id.clone(),
                    Hop::start(desired, settings.clone(), origin)?,
                );
            }
        }
        let heartbeat = NodeHeartbeat {
            inputs: registration.node.inputs,
            outputs: registration.node.outputs,
            node_id: registration.node.id,
            status: NodeStatus::Ready,
            observation: None,
            endpoints: vec![],
            hop_status: reports(hops),
        };
        let response = self
            .authorize(self.client.post(format!(
                "{}/nodes/{}/heartbeat",
                self.url, heartbeat.node_id
            )))
            .json(&heartbeat)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.registered = false;
            return Ok(());
        }
        response.error_for_status()?;
        Ok(())
    }
}

fn reports(hops: &BTreeMap<String, Hop>) -> Vec<HopStatus> {
    hops.values()
        .map(|hop| hop.report.lock().unwrap().clone())
        .collect()
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        self.node
            .validate()
            .context("validating native node configuration")?;
        anyhow::ensure!(
            self.node.topology.site.is_none(),
            "topology.site needs a node that observes its own uplink, and the native node does not; declare an attachment with a network and listener host instead"
        );
        self.rtp.validate()?;
        anyhow::ensure!(
            (1..=65_536).contains(&self.media.max_buffer_records),
            "buffer record limit must be between 1 and 65536"
        );
        anyhow::ensure!(
            (1_088..=256 * 1024 * 1024).contains(&self.media.max_buffer_bytes),
            "buffer byte limit must be between 1088 and 268435456"
        );
        Ok(())
    }
}

pub async fn run(config: Config) -> Result<()> {
    config.validate()?;
    let clock = Clock::new(config.clock.clone());
    anyhow::ensure!(!clock.domain().is_empty(), "clock domain must not be empty");
    clock.refresh().await;
    let mut epoch_bytes = [0_u8; 8];
    getrandom::fill(&mut epoch_bytes)
        .map_err(|error| anyhow::anyhow!("generating origin runtime epoch: {error}"))?;
    let epoch = u64::from_be_bytes(epoch_bytes).max(1);
    let node = NodeDescriptor {
        inputs: config.rtp.declared_inputs(),
        outputs: config.rtp.declared_outputs(),
        id: config.node.id.clone(),
        endpoint: config.node.public_endpoint(),
        status: NodeStatus::Ready,
        capabilities: NodeCapabilities {
            hop_profiles: profiles(
                &clock.domain(),
                epoch,
                config.media.scheduling_error_bound_us,
            ),
        },
        topology: config.node.topology.clone(),
    };
    let mut control = Control {
        client: Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()?,
        url: config.node.southbound_url.trim_end_matches('/').to_string(),
        token: config.node.resolve_southbound_token()?,
        registered: false,
    };
    let published: Arc<Mutex<Vec<HopStatus>>> = Arc::default();
    let health_reports = published.clone();
    let app = Router::new()
        .route(
            "/health",
            get(|| async { Json(json!({"status":"ok", "version":env!("CARGO_PKG_VERSION")})) }),
        )
        .route(
            "/status",
            get(move || {
                let reports = health_reports.clone();
                async move { Json(reports.lock().unwrap().clone()) }
            }),
        );
    let listener = tokio::net::TcpListener::bind(&config.node.listen)
        .await
        .context("binding native node health API")?;
    let health = tokio::spawn(async move { axum::serve(listener, app).await });
    let refreshed = clock.clone();
    let refresh = tokio::spawn(async move {
        loop {
            refreshed.refresh().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
    let settings = Settings {
        clock: clock.clone(),
        limits: config.media.limits(),
        bind_ip: config.media.listen_ip,
        rtp: Arc::new(Rtp::new(&config.rtp)),
    };
    let mut hops = BTreeMap::new();
    let mut origins = BTreeMap::new();
    tracing::info!(node = %node.id, epoch, clock_domain = %clock.domain(), "native SRT media node starting");
    let operation = async {
        loop {
            anyhow::ensure!(
                hops.values().all(|hop: &Hop| !hop.is_finished()),
                "media actor exited; restart the node to authorize a new origin epoch"
            );
            let registration = NodeRegistration {
                protocol_version: PROTOCOL_VERSION,
                node: node.clone(),
                observation: None,
                endpoints: vec![],
                hop_status: reports(&hops),
            };
            if let Err(error) = control
                .synchronize(registration, &mut hops, &mut origins, &settings)
                .await
            {
                if error
                    .downcast_ref::<reqwest::Error>()
                    .and_then(reqwest::Error::status)
                    .is_some_and(|status| status.is_client_error())
                {
                    return Err(error).context("control surface rejected the native node");
                }
                tracing::warn!(node = %node.id, error = %error, "native control synchronization failed; retaining media");
            }
            *published.lock().unwrap() = reports(&hops);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    let result = tokio::select! {
        result = operation => result,
        result = connect_core::shutdown::signal() => result.context("waiting for native node shutdown"),
    };
    refresh.abort();
    health.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_example_is_valid_and_a_site_is_refused() {
        let example = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../examples/native-node.yaml"),
        )
        .expect("reading examples/native-node.yaml");
        let config: Config = serde_norway::from_str(&example).unwrap();
        config
            .validate()
            .unwrap_or_else(|error| panic!("{error:#}"));

        let with_site = example.replace(
            "  topology:\n",
            "  topology:\n    site:\n      id: office\n",
        );
        let config: Config = serde_norway::from_str(&with_site).unwrap();
        let error = format!("{:#}", config.validate().unwrap_err());
        assert!(error.contains("topology.site"), "{error}");
    }
}
