use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use titanrt::connector::BaseConnector;
use titanrt::utils::{CancelToken, CoreStats};


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolanaConfig {
    pub default_max_cores: Option<usize>,
    pub specific_cores: Vec<usize>,
    pub with_core_stats: bool,
}

pub struct SolanaConnector {
    pub(crate) config: SolanaConfig,
    pub(crate) cancel_token: CancelToken,
    pub(crate) core_stats: Option<Arc<CoreStats>>,
}

impl BaseConnector for SolanaConnector {
    type MainConfig  = SolanaConfig;

    fn init(
        config: Self::MainConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self> {
        let core_stats = if config.with_core_stats {
            Some(CoreStats::new(
                config.default_max_cores,
                config.specific_cores.clone(),
                reserved_core_ids.unwrap_or_default(),
            )?)
        } else {
            None
        };

        Ok(Self {
            config,
            cancel_token,
            core_stats,
        })
    }

    fn name(&self) -> impl AsRef<str> + Display {
        "CompositeConnector"
    }

    fn config(&self) -> &Self::MainConfig {
        &self.config
    }

    fn cancel_token(&self) -> &CancelToken {
        &self.cancel_token
    }

    fn cores_stats(&self) -> Option<Arc<CoreStats>> {
        self.core_stats.clone()
    }
}

