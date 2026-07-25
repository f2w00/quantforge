use crate::risk::{KillSwitch, RiskCheckInput, RiskDecision, RiskLimits, RiskViolation};

#[derive(Debug)]
pub struct RiskGuard {
    limits: RiskLimits,
    kill_switch: KillSwitch,
}

impl RiskGuard {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            kill_switch: KillSwitch::default(),
        }
    }

    pub fn kill_switch(&self) -> &KillSwitch {
        &self.kill_switch
    }

    pub fn check(&self, input: &RiskCheckInput) -> RiskDecision {
        let mut violations = Vec::new();

        if self.kill_switch.is_enabled() {
            violations.push(RiskViolation::new("kill_switch", "kill switch is enabled"));
        }

        if self.limits.reduce_only && !input.reduce_only {
            violations.push(RiskViolation::new(
                "reduce_only",
                "only reduce-only orders are allowed",
            ));
        }

        if let Some(max) = self.limits.max_leverage {
            if input.post_trade_leverage > max {
                violations.push(RiskViolation::new("max_leverage", "leverage exceeds limit"));
            }
        }

        if let Some(max) = self.limits.max_order_notional {
            if input.order_notional > max {
                violations.push(RiskViolation::new(
                    "max_order_notional",
                    "order notional exceeds limit",
                ));
            }
        }

        if let Some(max) = self.limits.max_post_trade_notional {
            if input.post_trade_notional > max {
                violations.push(RiskViolation::new(
                    "max_post_trade_notional",
                    "post-trade notional exceeds limit",
                ));
            }
        }

        if let Some(max) = self.limits.max_open_orders {
            if input.open_order_count > max {
                violations.push(RiskViolation::new(
                    "max_open_orders",
                    "open order count exceeds limit",
                ));
            }
        }

        if violations.is_empty() {
            RiskDecision::Approved
        } else {
            RiskDecision::Rejected { violations }
        }
    }
}
