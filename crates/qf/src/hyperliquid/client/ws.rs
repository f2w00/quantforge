#[derive(Clone, Debug)]
pub struct HyperliquidWsClient {
    pub base_url: String,
}

impl HyperliquidWsClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}
