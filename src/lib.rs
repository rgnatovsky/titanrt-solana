mod connector;

use crate::connector::SolanaConnectorExt;
use anyhow::anyhow;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use titanrt::connector::errors::{StreamError, StreamResult};
use titanrt::connector::{
   BaseConnector, Kind, RuntimeCtx, StreamDescriptor, StreamRunner, StreamSpawner, Venue,
};
use titanrt::io::ringbuffer::RingSender;
use titanrt::prelude::{BaseRx, BaseTx, TxPairExt};
use titanrt::utils::{CancelToken, CorePickPolicy, CoreStats, StateCell, StateMarker};
use tokio::runtime::Builder;

#[derive(Clone)]
pub enum GeyserAction {
    Subscribe(SubscribeRequest),
    UnsubscribeAll,
}

#[derive(Clone, Debug)]
pub struct GeyserGrpcDescriptor {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub max_pending_actions: Option<usize>,
    pub max_pending_events: Option<usize>,
    pub core_pick_policy: CorePickPolicy,
    pub subscription: Option<SubscribeRequest>,
}

impl GeyserGrpcDescriptor {
    pub fn new(
        endpoint: String,
        auth_token: Option<String>,
        max_pending_actions: Option<usize>,
        max_pending_events: Option<usize>,
        core_pick_policy: CorePickPolicy,
    ) -> Self {
        Self {
            endpoint,
            auth_token,
            max_pending_actions,
            max_pending_events,
            core_pick_policy,
            subscription: None,
        }
    }

    pub fn with_subscription(mut self, sub: SubscribeRequest) -> Self {
        self.subscription = Some(sub);
        self
    }
}

impl StreamDescriptor for GeyserGrpcDescriptor {
    fn venue(&self) -> impl Venue {
        "Solana"
    }

    fn kind(&self) -> impl Kind {
        "YellowstoneGrpc"
    }

    fn max_pending_actions(&self) -> Option<usize> {
        self.max_pending_actions
    }

    fn max_pending_events(&self) -> Option<usize> {
        self.max_pending_events
    }

    fn core_pick_policy(&self) -> Option<CorePickPolicy> {
        Some(self.core_pick_policy)
    }

    fn health_at_start(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub enum GeyserEvent {
    Raw(UpdateOneof),
}

impl<C, E, S> StreamSpawner<GeyserGrpcDescriptor, E, S> for C
where
    C: BaseConnector + SolanaConnectorExt,
    S: StateMarker,
    E: BaseTx + TxPairExt,
{
}

impl<C, E, S> StreamRunner<GeyserGrpcDescriptor, E, S> for C
where
    C: BaseConnector + SolanaConnectorExt,
    S: StateMarker,
    E: BaseTx,
{
    type Config = ();
    type ActionTx = RingSender<GeyserAction>;
    type RawEvent = GeyserEvent;
    type Hook = fn(&Self::RawEvent, &mut E, &StateCell<S>);

    fn build_config(&mut self, _desc: &GeyserGrpcDescriptor) -> anyhow::Result<Self::Config> {
        Ok(())
    }

    fn run(
        mut ctx: RuntimeCtx<GeyserGrpcDescriptor, Self, E, S>,
        hook: Self::Hook,
    ) -> StreamResult<()> {
        // Однопоточный рантайм ТОЛЬКО внутри run (внешний API остаётся синхронным)
        let rt = Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| StreamError::Unknown(anyhow!(e)))?;

        let mut rng = SmallRng::from_os_rng();
        let mut backoff_ms: u64 = 50;
        let backoff_max_ms: u64 = 5_000;
        let backoff_mul: f64 = 1.7;

        if !ctx.desc.health_at_start() {
            ctx.health.set(true);
        }

        rt.block_on(async move {
         'reconnect: loop {
            if ctx.cancel.is_cancelled() { break Ok(()); }

            if backoff_ms > 50 {
               let j = rng.random_range(0..(backoff_ms / 5 + 1));
               tokio::time::sleep(Duration::from_millis(j)).await;
            }

            let mut client = match GeyserGrpcClient::build_from_shared(ctx.desc.endpoint.clone())
               .map_err(|e| StreamError::Unknown(anyhow!(e)))?
               .x_token(ctx.desc.auth_token.clone())
               .map_err(|e| StreamError::Unknown(anyhow!(e)))?
               .tls_config(ClientTlsConfig::new().with_native_roots())
               .map_err(|e| StreamError::Unknown(anyhow!(e)))?
               .connect().await
            {
               Ok(c) => c,
               Err(e) => {
                  tracing::warn!("grpc connect error: {e}");
                  let sleep = backoff_ms.min(backoff_max_ms);
                  tokio::time::sleep(Duration::from_millis(sleep)).await;
                  backoff_ms = ((backoff_ms as f64) * backoff_mul) as u64;
                  continue 'reconnect;
               }
            };

            let (mut tx, mut rx) = match client.subscribe().await {
               Ok(x) => x,
               Err(e) => {
                  tracing::warn!("subscribe call error: {e}");
                  let sleep = backoff_ms.min(backoff_max_ms);
                  tokio::time::sleep(Duration::from_millis(sleep)).await;
                  backoff_ms = ((backoff_ms as f64) * backoff_mul) as u64;
                  continue 'reconnect;
               }
            };

            if let Some(req) = ctx.desc.subscription.as_ref() {
               if let Err(e) = tx.send(req.clone()).await {
                  tracing::warn!("send initial subreq: {e}");
               }
            }

            ctx.health.set(true);

            backoff_ms = 50;

            let mut tick = tokio::time::interval(Duration::from_millis(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            'session: loop {
               tokio::select! {
                        biased;

                        m = rx.next() => {
                            match m {
                                Some(Ok(update)) => {
                                    match update.update_oneof {

                                        Some(UpdateOneof::Ping(_)) => {
                                            tracing::debug!("ping update");
                                            let _ = tx.send(SubscribeRequest {
                                                              ping: Some(SubscribeRequestPing {id: 0}),
                                                              ..Default::default()
                                                            }).await;
                                        }
                                        Some(u) => {

                                                hook(&GeyserEvent::Raw(u), &mut ctx.event_tx, &ctx.state);
                                        }
                                        None => {}
                                    }
                                }
                                Some(Err(status)) => {
                                    tracing::warn!("grpc stream error: {status}");
                                    ctx.health.set(false);
                                    break 'session;
                                }
                                None => {
                                    tracing::warn!("grpc stream ended");
                                    ctx.health.set(false);
                                    break 'session;
                                }
                            }
                        }

                        _ = tick.tick() => {
                            let mut sent = 0usize;


                            while let Ok(a) = ctx.action_rx.try_recv() {
                                match a {
                                    GeyserAction::Subscribe(req) => {
                                        match tx.send(req).await {
                                            Ok(_) => {
                                                sent += 1;
                                                if sent >= 4096 { break; }
                                            }
                                            Err(e) => {
                                                tracing::warn!("subreq send failed: {e}");
                                                break 'session;
                                            }
                                        }
                                    },
                                    GeyserAction::UnsubscribeAll => {}
                                }
                            }
                        }
                    }
               if ctx.cancel.is_cancelled() { break 'reconnect Ok(()); }
            }

            let sleep = backoff_ms.min(backoff_max_ms);
            tokio::time::sleep(Duration::from_millis(sleep)).await;
            backoff_ms = ((backoff_ms as f64) * backoff_mul) as u64;
         }
      })?;

        Ok(())
    }
}
