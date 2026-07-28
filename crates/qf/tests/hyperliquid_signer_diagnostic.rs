use anyhow::{Context, Result, bail};
use qf::core::Decimal;
use qf::hyperliquid::client::{HyperliquidRestClient, HyperliquidSigner};
use qf::hyperliquid::types::{
    HlAssetId, HlClientOrderId, HlExchangeAction, HlOrderAction, HlOrderGrouping, HlOrderType,
    HlTimeInForce, HlWireOrder,
};

const MAINNET_REST_URL: &str = "https://api.hyperliquid.xyz";
const DIAGNOSTIC_CLOID: &str = "0x00000000000000000000019fa8acd839";

#[tokio::test]
#[ignore = "reads Mainnet account role and generates a local diagnostic signature"]
async fn mainnet_signer_diagnostic() -> Result<()> {
    let account_address = std::env::var("QF_MAINNET_ACCOUNT_ADDRESS")
        .context("QF_MAINNET_ACCOUNT_ADDRESS is required")?
        .parse::<alloy::primitives::Address>()
        .context("QF_MAINNET_ACCOUNT_ADDRESS must be a valid address")?;
    let private_key =
        std::env::var("QF_MAINNET_PRIVATE_KEY").context("QF_MAINNET_PRIVATE_KEY is required")?;
    let signer = HyperliquidSigner::from_private_key(&private_key)
        .context("QF_MAINNET_PRIVATE_KEY must be a valid private key")?;
    let client = HyperliquidRestClient::new(MAINNET_REST_URL);
    let signer_address = format!("{:#x}", signer.wallet_address());
    let account_address = format!("{account_address:#x}");
    let role = client
        .user_role(&signer_address)
        .await
        .context("query Mainnet userRole for derived signer")?;
    let owner = role
        .pointer("/data/user")
        .and_then(serde_json::Value::as_str);

    println!("Hyperliquid signer diagnostic");
    println!("network: Mainnet");
    println!("configured account: {account_address}");
    println!("derived signer address: {signer_address}");
    println!("userRole response: {role}");
    println!(
        "agent owner matches configured account: {}",
        owner.is_some_and(|owner| owner.eq_ignore_ascii_case(&account_address))
    );

    if role.get("role").and_then(serde_json::Value::as_str) != Some("agent") {
        bail!("derived signer is not a Mainnet API wallet");
    }
    if !owner.is_some_and(|owner| owner.eq_ignore_ascii_case(&account_address)) {
        bail!("derived API wallet is not authorized for QF_MAINNET_ACCOUNT_ADDRESS");
    }

    let metadata = client.meta().await.context("query Mainnet metadata")?;
    let doge = metadata.asset(&qf::hyperliquid::types::HlCoin::new("DOGE"));
    let doge = doge.context("DOGE must exist in Mainnet metadata")?;
    let action = HlExchangeAction::Order(HlOrderAction {
        orders: vec![HlWireOrder {
            asset: HlAssetId(doge.asset_id.0),
            is_buy: true,
            price: "0.070730".parse::<Decimal>()?,
            size: Decimal::from(158),
            reduce_only: false,
            order_type: HlOrderType::Limit {
                limit_price: "0.070730".parse::<Decimal>()?,
                tif: HlTimeInForce::Ioc,
            },
            client_order_id: Some(HlClientOrderId::new(DIAGNOSTIC_CLOID).unwrap()),
        }],
        grouping: HlOrderGrouping::Na,
    });
    let diagnostics = signer
        .diagnose_action(&action, signer.next_nonce(), None, None, true)
        .context("create local Mainnet L1 action signature")?;

    println!("diagnostic order: DOGE Buy 158 @ 0.070730, reduce_only=false, tif=Ioc");
    println!("nonce: {}", diagnostics.nonce);
    println!("action JSON: {}", diagnostics.action);
    println!("connection id: {:#x}", diagnostics.connection_id);
    println!("EIP-712 digest: {:#x}", diagnostics.digest);
    println!("signature r: {}", diagnostics.signature.r);
    println!("signature s: {}", diagnostics.signature.s);
    println!("signature v: {}", diagnostics.signature.v);
    println!(
        "locally recovered signer: {:#x}",
        diagnostics.recovered_signer
    );
    println!(
        "recovered signer matches derived signer: {}",
        diagnostics.recovered_signer == signer.wallet_address()
    );
    assert_eq!(diagnostics.recovered_signer, signer.wallet_address());
    assert_eq!(diagnostics.action["type"], "order");
    assert_eq!(diagnostics.action["orders"][0]["b"], true);
    Ok(())
}
