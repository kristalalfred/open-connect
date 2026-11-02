use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use weave_core::ApiErrorCode;
use weave_core::logging::ErrorChain;

use crate::store::{LeaseTerm, PgStore};
use crate::{Controller, error, health, tick_every, ui};

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

#[derive(Clone, Default)]
pub(crate) struct Leadership(Arc<std::sync::RwLock<Held>>);

#[derive(Default)]
struct Held {
    role: Role,
    until: Option<Instant>,
}

#[derive(Default)]
enum Role {
    #[default]
    Standby,
    Taking,
    Leading(Router),
}

impl Leadership {
    fn held(&self) -> std::sync::RwLockWriteGuard<'_, Held> {
        self.0
            .write()
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
        if !matches!(held.role, Role::Standby) {
            held.until = Some(until);
        }
    }

    pub(crate) fn lead(&self, app: Router) -> bool {
        let mut held = self.held();
        if !matches!(held.role, Role::Taking) {
            return false;
        }
        held.role = Role::Leading(app);
        true
    }

    pub(crate) fn stand_by(&self) {
        *self.held() = Held::default();
    }

    pub(crate) fn router(&self) -> Option<Router> {
        let held = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &held.role {
            Role::Leading(app) if held.until.is_some_and(|until| Instant::now() < until) => {
                Some(app.clone())
            }
            Role::Leading(_) | Role::Standby | Role::Taking => None,
        }
    }
}

pub(crate) fn leadership_router(leadership: Leadership) -> Router {
    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .fallback(serve_if_leading)
        .with_state(leadership)
}

async fn serve_if_leading(
    State(leadership): State<Leadership>,
    request: axum::extract::Request,
) -> Response {
    match leadership.router() {
        Some(app) => match app.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => not_leader(),
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
    tracing::info!(%holder, "standing by for the controller lease");
    loop {
        let (term, asked) = tokio::select! {
            taken = wait_for_lease(&pg, &holder, timing) => taken,
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
            Ok((state, app)) => {
                if !leadership.lead(app) {
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

async fn wait_for_lease(pg: &PgStore, holder: &str, timing: LeaseTiming) -> (LeaseTerm, Instant) {
    let mut held_elsewhere = false;
    loop {
        let asked = Instant::now();
        let deadline = asked + timing.hold_for;
        match tokio::time::timeout_at(deadline.into(), pg.acquire_lease(holder, timing.ttl)).await {
            Ok(Ok(Some(term))) => return (term, asked),
            Ok(Ok(None)) if !held_elsewhere => {
                tracing::info!("another controller holds the lease");
                held_elsewhere = true;
            }
            Ok(Ok(None)) => {}
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
