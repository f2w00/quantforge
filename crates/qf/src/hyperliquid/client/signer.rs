#[derive(Clone, Debug)]
pub struct HyperliquidSigner {
    pub wallet_address: String,
}

impl HyperliquidSigner {
    pub fn new(wallet_address: impl Into<String>) -> Self {
        Self {
            wallet_address: wallet_address.into(),
        }
    }
}
