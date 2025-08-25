use titanrt::connector::{Kind, StreamDescriptor, Venue};
use titanrt::utils::CorePickPolicy;
use titanrt::utils::backoff::ReconnectCfg;
use yellowstone_grpc_proto::geyser::SubscribeRequest;

pub const STREAM_VENUE: &str = "solana";
pub const STREAM_KIND: &str = "yellowstone_grpc";

#[derive(Clone, Debug)]
pub struct GeyserGrpcDescriptor {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub reconnect_cfg: ReconnectCfg,
    pub max_pending_actions: Option<usize>,
    pub max_pending_events: Option<usize>,
    pub core_pick_policy: CorePickPolicy,
    pub subscription: Option<SubscribeRequest>,
}

impl GeyserGrpcDescriptor {
    pub fn new(
        endpoint: String,
        auth_token: Option<String>,
        reconnect_cfg: Option<ReconnectCfg>,
        max_pending_actions: Option<usize>,
        max_pending_events: Option<usize>,
        core_pick_policy: CorePickPolicy,
    ) -> Self {
        Self {
            endpoint,
            auth_token,
            reconnect_cfg: reconnect_cfg.unwrap_or_default(),
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
        STREAM_VENUE
    }

    fn kind(&self) -> impl Kind {
        STREAM_KIND
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
