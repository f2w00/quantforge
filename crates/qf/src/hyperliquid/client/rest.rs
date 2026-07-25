#[derive(Clone, Debug)]
pub struct HyperliquidRestClient {
    pub base_url: String,
}

impl HyperliquidRestClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}
