use crate::connector::SolanaConnector;
use crate::yellowstone_grpc::descriptor::GeyserGrpcDescriptor;
use anyhow::anyhow;
use futures_util::sink::SinkExt;
use std::time::Duration;
use titanrt::connector::errors::{StreamError, StreamResult};
use titanrt::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use titanrt::io::ringbuffer::RingSender;
use titanrt::prelude::{BaseRx, BaseTx, TxPairExt};
use titanrt::utils::backoff::Backoff;
use titanrt::utils::StateMarker;
use tokio::runtime::Builder;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestPing};
use yellowstone_grpc_proto::tonic::codegen::tokio_stream::StreamExt;
use yellowstone_grpc_proto::tonic::Status;

#[derive(Clone)]
pub enum GeyserAction {
    Subscribe(SubscribeRequest),
    UnsubscribeAll,
}

#[derive(Clone, Debug)]
pub enum GeyserEvent {
    Raw(UpdateOneof),
    ReadError(Status),
    WriteError(String),
    StreamEnd,
}

impl<E, S> StreamSpawner<GeyserGrpcDescriptor, E, S> for SolanaConnector
where
    S: StateMarker,
    E: BaseTx + TxPairExt,
{
}

impl<E, S> StreamRunner<GeyserGrpcDescriptor, E, S> for SolanaConnector
where
    S: StateMarker,
    E: BaseTx,
{
    type Config = ();
    type ActionTx = RingSender<GeyserAction>;
    type RawEvent = GeyserEvent;
    type HookResult = ();

    fn build_config(&mut self, _desc: &GeyserGrpcDescriptor) -> anyhow::Result<Self::Config> {
        Ok(())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<Self::Config, GeyserGrpcDescriptor, Self::ActionTx, E, S>,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<Self::RawEvent, E, S, GeyserGrpcDescriptor, Self::HookResult>,
    {
        let mut hook = hook.into_hook();

        let rt = Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| StreamError::Unknown(anyhow!(e)))?;

        let mut backoff = Backoff::new(ctx.desc.reconnect_cfg.clone());

        let res: anyhow::Result<()> = rt.block_on(async move {
            'reconnect: loop {
                if ctx.cancel.is_cancelled() {
                    break Ok(());
                }

                let mut client = GeyserGrpcClient::build_from_shared(ctx.desc.endpoint.clone())?
                    .x_token(ctx.desc.auth_token.clone())?
                    .tls_config(ClientTlsConfig::new().with_native_roots())?
                    .connect()
                    .await?;

                let (mut writer, mut reader) = client.subscribe().await?;

                if let Some(req) = ctx.desc.subscription.as_ref() {
                    if let Err(e) = writer.send(req.clone()).await {
                        tracing::warn!("send initial subreq: {e}");
                    }
                }

                let mut tick = tokio::time::interval(Duration::from_millis(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                ctx.health.set(true);

                'session: loop {
                    tokio::select! {
                        biased;

                        m = reader.next() => {
                            match m {
                                Some(Ok(update)) => {
                                    match update.update_oneof {
                                        Some(UpdateOneof::Ping(_)) => {
                                            tracing::debug!("ping update");
                                            let _ = writer.send(
                                                SubscribeRequest {
                                                    ping: Some(SubscribeRequestPing {id: 0}),
                                                    ..Default::default()
                                                }).await;
                                        }
                                        Some(u) => {
                                            hook.call(
                                                HookArgs::new(
                                                    &GeyserEvent::Raw(u),
                                                    &mut ctx.event_tx,
                                                    &ctx.state,
                                                    &ctx.desc,
                                                    &ctx.health
                                                )
                                            );
                                        }
                                        None => {}
                                    }
                                }
                                Some(Err(status)) => {
                                    tracing::warn!("grpc stream error: {status}");
                                    hook.call(
                                        HookArgs::new(
                                            &GeyserEvent::ReadError(status),
                                            &mut ctx.event_tx,
                                            &ctx.state,
                                            &ctx.desc,
                                            &ctx.health
                                        )
                                    );
                                    ctx.health.down();
                                    break 'session;
                                }
                                None => {
                                     hook.call(
                                        HookArgs::new(
                                            &GeyserEvent::StreamEnd,
                                            &mut ctx.event_tx,
                                            &ctx.state,
                                            &ctx.desc,
                                            &ctx.health
                                        )
                                    );
                                    ctx.health.down();
                                    break 'session;
                                }
                            }
                        }
                        _ = tick.tick() => {
                            let mut sent = 0usize;

                            while let Ok(a) = ctx.action_rx.try_recv() {
                                // TODO add a corr id to each action
                                match a {
                                    GeyserAction::Subscribe(req) => {
                                        match writer.send(req).await {
                                            Ok(_) => {
                                                sent += 1;
                                                if sent >= 4096 { break; }
                                            }
                                            Err(e) => {
                                                hook.call(
                                                    HookArgs::new(
                                                        &GeyserEvent::WriteError(e.to_string()),
                                                        &mut ctx.event_tx,
                                                        &ctx.state,
                                                        &ctx.desc,
                                                        &ctx.health
                                                    )
                                                );
                                                break 'session;
                                            }
                                        }
                                    },
                                    GeyserAction::UnsubscribeAll => {}
                                }
                            }
                        }
                    }

                    if ctx.cancel.is_cancelled() {
                        break 'reconnect Ok(());
                    }
                }

                if backoff.should_reset() {
                    backoff.on_success();
                }

                if let Some(0) = backoff.attempts_left() {
                    return Err(anyhow!("reconnect attempts exhausted"));
                }

                let delay = backoff.next_delay();

                if !ctx.cancel.sleep_cancellable(delay) {
                    break Ok(());
                }
            }
        });

        Ok(res?)
    }
}
