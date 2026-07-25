use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Exchange(pub String);

impl Exchange {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct ExchangeSymbol {
    pub exchange: Exchange,
    pub symbol: Symbol,
}

impl ExchangeSymbol {
    pub fn new(exchange: Exchange, symbol: Symbol) -> Self {
        Self { exchange, symbol }
    }
}
