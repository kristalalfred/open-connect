use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio::task::JoinHandle;
use tower::ServiceExt;
use weave_core::ApiErrorCode;
use weave_core::logging::ErrorChain;

use crate::store::{LeaseTerm, PgStore};
use crate::{Controller, error, tick_every};

const FORWARDED: HeaderName = HeaderName::from_static("weave-forwarded");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);
const HOP_BY_HOP: [HeaderName; 10] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
    header::HOST,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControllerUrl(String);

impl ControllerUrl {
    fn join(&self, path_and_query: &str) -> String {
        format!("{}{path_and_query}", self.0)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ControllerUrl {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let base = raw.trim().trim_end_matches('/');
        match reqwest::Url::parse(base) {
            Ok(url) if matches!(url.scheme(), "http" | "https") && url.host().is_some() => {
                Ok(Self(base.to_string()))
            }
            _ => Err(format!("{raw:?} is not an http or https URL with a host")),
        }
    }
}

impl std::fmt::Display for ControllerUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaseTiming {
    ttl: Duration,
    pub(crate) renew_every: Duration,
    hold_for: Duration,
    pub(crate) retry_every: Duration,
}

impl LeaseTiming {
    pub(crate) fn new(ttl: Duration) -> Self {
        let third = ttl / 3;
        Self {
            ttl,
            renew_every: third,
            hold_for: ttl - third,
            retry_every: third.min(Duration::from_secs(1)),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Apps {
    pub(crate) api: Router,
    pub(crate) dashboard: Router,
}

#[derive(Clone, Default)]
pub(crate) struct Leadership(Arc<std::sync::RwLock<Held>>);

#[derive(Default)]
struct Held {
    role: Role,
    until: Option<Instant>,
}

enum Role {
    Standby(Option<ControllerUrl>),
    Taking,
    Leading(Apps),
}

impl Default for Role {
    fn default() -> Self {
        Self::Standby(None)
    }
}

enum ApiRoute {
    Serve(Router),
    Forward(ControllerUrl),
    Refuse,
}

impl Leadership {
    fn held(&self) -> std::sync::RwLockWriteGuard<'_, Held> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Held> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn taking(&self, until: Instant) {
        *self.held() = Held {
            role: Role::Taking,
            until: Some(until),
        };
    }

    fn hold_until(&self, until: Instant) {
        let mut held = self.held();
        if !matches!(held.role, Role::Standby(_)) {
            held.until = Some(until);
        }
    }

    pub(crate) fn lead(&self, apps: Apps) -> bool {
        let mut held = self.held();
        if !matches!(held.role, Role::Taking) {
            return false;
        }
        held.role = Role::Leading(apps);
        true
    }

    pub(crate) fn stand_by(&self) {
        *self.held() = Held::default();
    }

    pub(crate) fn follow(&self, leader: Option<ControllerUrl>) {
        let mut held = self.held();
        let Role::Standby(followed) = &mut held.role else {
            return;
        };
        if *followed != leader {
            match &leader {
                Some(url) => {
                    tracing::info!(leader = %url, "forwarding API requests to the leading controller")
                }
                None => tracing::info!("no leading controller to forward API requests to"),
            }
            *followed = leader;
        }
    }

    pub(crate) fn leading(&self) -> Option<Apps> {
        let held = self.read();
        match &held.role {
            Role::Leading(apps) if held.until.is_some_and(|until| Instant::now() < until) => {
                Some(apps.clone())
            }
            Role::Leading(_) | Role::Standby(_) | Role::Taking => None,
        }
    }

    fn api_route(&self) -> ApiRoute {
        if let Some(apps) = self.leading() {
            return ApiRoute::Serve(apps.api);
        }
        match &self.read().role {
            Role::Standby(Some(leader)) => ApiRoute::Forward(leader.clone()),
            Role::Standby(None) | Role::Taking | Role::Leading(_) => ApiRoute::Refuse,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Forwarder {
    http: reqwest::Client,
}

impl Forwarder {
    pub(crate) fn new(timeout: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("building the HTTP client that forwards to the leading controller")?;
        Ok(Self { http })
    }

    async fn forward(&self, leader: &ControllerUrl, request: Request) -> Response {
        let (parts, body) = request.into_parts();
        let target = leader.join(parts.uri.path_and_query().map_or("/", |pq| pq.as_str()));
        let mut headers = parts.headers;
        without_hop_by_hop(&mut headers);
        headers.insert(FORWARDED, HeaderValue::from_static("1"));
        let sent = self
            .http
            .request(parts.method, &target)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body.into_data_stream()))
            .send()
            .await;
        let answer = match sent {
            Ok(response) => {
                let status = response.status();
                let headers = response.headers().clone();
                response.bytes().await.map(|body| (status, headers, body))
            }
            Err(err) => Err(err),
        };
        match answer {
            Ok((status, mut headers, body)) => {
                without_hop_by_hop(&mut headers);
                headers.remove(header::CONTENT_LENGTH);
                let mut response = (status, Body::from(body)).into_response();
                response.headers_mut().extend(headers);
                response
            }
            Err(err) => {
                tracing::warn!(error = %ErrorChain(&err), leader = %leader, "forwarding to the leading controller failed");
                if err.is_timeout() && !err.is_connect() {
                    error(
                        StatusCode::GATEWAY_TIMEOUT,
                        ApiErrorCode::ControllerTimeout,
                        "the leading controller did not answer in time; it may have applied the request",
                    )
                } else {
                    error(
                        StatusCode::BAD_GATEWAY,
                        ApiErrorCode::ControllerUnreachable,
                        "the leading controller is unreachable",
                    )
                }
            }
        }
    }
}

fn without_hop_by_hop(headers: &mut HeaderMap) {
    for name in &HOP_BY_HOP {
        headers.remove(name);
    }
}

#[derive(Clone)]
struct ApiGate {
    leadership: Leadership,
    forwarder: Forwarder,
}

pub(crate) fn api_gate(leadership: Leadership, forwarder: Forwarder) -> Router {
    Router::new()
        .fallback(serve_or_forward)
        .with_state(ApiGate {
            leadership,
            forwarder,
        })
}

async fn serve_or_forward(State(gate): State<ApiGate>, request: Request) -> Response {
    match gate.leadership.api_route() {
        ApiRoute::Serve(api) => serve(api, request).await,
        ApiRoute::Forward(leader) if !request.headers().contains_key(FORWARDED) => {
            gate.forwarder.forward(&leader, request).await
        }
        ApiRoute::Forward(_) | ApiRoute::Refuse => not_leader(),
    }
}

pub(crate) fn dashboard_gate(leadership: Leadership) -> Router {
    Router::new()
        .fallback(serve_if_leading)
        .with_state(leadership)
}

async fn serve_if_leading(State(leadership): State<Leadership>, request: Request) -> Response {
    match leadership.leading() {
        Some(apps) => serve(apps.dashboard, request).await,
        None => not_leader(),
    }
}

async fn serve(app: Router, request: Request) -> Response {
    match app.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

pub(crate) fn not_leader() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        ApiErrorCode::NotLeader,
        "this controller does not hold the lease; another controller leads",
    )
}

pub(crate) async fn run_with_lease(
    pg: Arc<PgStore>,
    controller: &Controller,
    leadership: &Leadership,
    timing: LeaseTiming,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    tokio::pin!(shutdown);
    let holder = holder_id()?;
    let address = controller.advertise.as_ref().map(ControllerUrl::as_str);
    match address {
        Some(address) => {
            tracing::info!(%holder, %address, "standing by for the controller lease")
        }
        None => tracing::info!(
            %holder,
            "standing by for the controller lease; no advertised URL, so standbys cannot forward to this controller while it leads"
        ),
    }
    loop {
        let (term, asked) = tokio::select! {
            taken = wait_for_lease(&pg, &holder, address, leadership, timing) => taken,
            result = &mut shutdown => return result,
        };
        tracing::info!(
            epoch = term.epoch,
            "took the controller lease; loading state"
        );
        leadership.taking(asked + timing.hold_for);
        let mut hold = tokio::spawn(hold_lease(pg.clone(), leadership.clone(), asked, timing));
        let led = tokio::select! {
            led = controller.lead(Arc::new(pg.for_term(term.epoch))) => led,
            lost = &mut hold => {
                tracing::warn!(reason = lost.unwrap_or("renewal task failed"), "lost the controller lease while loading state; standing by");
                continue;
            }
            result = &mut shutdown => {
                step_down(&pg, leadership, hold).await;
                return result;
            }
        };
        let state = match led {
            Ok((state, apps)) => {
                if !leadership.lead(apps) {
                    let lost = hold.await;
                    tracing::warn!(
                        reason = lost.unwrap_or("renewal task failed"),
                        "lost the controller lease while loading state; standing by"
                    );
                    continue;
                }
                state
            }
            Err(err) => {
                step_down(&pg, leadership, hold).await;
                tracing::error!(error = %ErrorChain(err.as_ref()), "loading state after taking the controller lease failed; standing by");
                tokio::select! {
                    () = tokio::time::sleep(timing.retry_every) => continue,
                    result = &mut shutdown => return result,
                }
            }
        };
        tracing::info!(epoch = term.epoch, "leading");
        let lost = tokio::select! {
            lost = &mut hold => lost,
            result = &mut shutdown => {
                step_down(&pg, leadership, hold).await;
                return result;
            }
            never = tick_every(&state, controller.interval) => match never {},
        };
        tracing::warn!(
            epoch = term.epoch,
            reason = lost.unwrap_or("renewal task failed"),
            "lost the controller lease; standing by"
        );
    }
}

async fn step_down(pg: &PgStore, leadership: &Leadership, hold: JoinHandle<&'static str>) {
    hold.abort();
    leadership.stand_by();
    if let Err(err) = pg.release_lease().await {
        tracing::warn!(error = %ErrorChain(&err), "releasing the controller lease failed; it expires on its own");
    }
}

fn holder_id() -> Result<String> {
    let mut id = [0u8; 8];
    getrandom::fill(&mut id)
        .map_err(|err| anyhow::anyhow!("generating a lease holder id: {err}"))?;
    Ok(id.iter().map(|byte| format!("{byte:02x}")).collect())
}

async fn wait_for_lease(
    pg: &PgStore,
    holder: &str,
    address: Option<&str>,
    leadership: &Leadership,
    timing: LeaseTiming,
) -> (LeaseTerm, Instant) {
    let mut held_elsewhere = false;
    loop {
        let asked = Instant::now();
        let deadline = asked + timing.hold_for;
        match tokio::time::timeout_at(
            deadline.into(),
            pg.acquire_lease(holder, address, timing.ttl),
        )
        .await
        {
            Ok(Ok(Some(term))) => return (term, asked),
            Ok(Ok(None)) => {
                if !held_elsewhere {
                    tracing::info!("another controller holds the lease");
                    held_elsewhere = true;
                }
                follow_leader(pg, leadership, deadline).await;
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %ErrorChain(&err), "taking the controller lease failed; retrying")
            }
            Err(_) => tracing::warn!(
                timeout_secs = timing.hold_for.as_secs_f64(),
                "taking the controller lease timed out; retrying"
            ),
        }
        tokio::time::sleep(timing.retry_every).await;
    }
}

async fn follow_leader(pg: &PgStore, leadership: &Leadership, deadline: Instant) {
    match tokio::time::timeout_at(deadline.into(), pg.leader_address()).await {
        Ok(Ok(address)) => leadership.follow(address.and_then(|address| {
            address
                .parse()
                .inspect_err(
                    |reason| tracing::warn!(%reason, "the lease names an unusable leader URL"),
                )
                .ok()
        })),
        Ok(Err(err)) => {
            tracing::warn!(error = %ErrorChain(&err), "reading the leading controller's URL failed")
        }
        Err(_) => tracing::warn!("reading the leading controller's URL timed out"),
    }
}

async fn hold_lease(
    pg: Arc<PgStore>,
    leadership: Leadership,
    asked: Instant,
    timing: LeaseTiming,
) -> &'static str {
    let mut deadline = asked + timing.hold_for;
    let mut wait = timing.renew_every;
    let reason = loop {
        tokio::time::sleep_until((Instant::now() + wait).min(deadline).into()).await;
        let sent = Instant::now();
        if sent >= deadline {
            break "no renewal succeeded in time";
        }
        match tokio::time::timeout_at(deadline.into(), pg.renew_lease(timing.ttl)).await {
            Ok(Ok(true)) => {
                deadline = sent + timing.hold_for;
                leadership.hold_until(deadline);
                wait = timing.renew_every;
            }
            Ok(Ok(false)) => break "the lease expired or another controller took it",
            Ok(Err(err)) => {
                tracing::warn!(error = %ErrorChain(&err), "renewing the controller lease failed; retrying");
                wait = timing.retry_every;
            }
            Err(_) => break "no renewal succeeded in time",
        }
    };
    leadership.stand_by();
    pg.drop_lease();
    reason
}
