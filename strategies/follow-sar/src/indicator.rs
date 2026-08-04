use yata::core::{IndicatorConfig, IndicatorInstance};
use yata::indicators::{ParabolicSAR, ParabolicSARInstance};

#[derive(Clone, Debug)]
pub struct Candle {
    pub opened_at: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SarTrend {
    Rising,
    Falling,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SarOutput {
    pub value: f64,
    pub trend: SarTrend,
    pub reversal: Option<SarTrend>,
}

pub struct SarIndicator {
    config: ParabolicSAR,
    instance: Option<ParabolicSARInstance>,
    previous_trend: Option<SarTrend>,
    last_opened_at: Option<i64>,
}

impl SarIndicator {
    pub fn new(acceleration_step: f64, acceleration_max: f64) -> anyhow::Result<Self> {
        let config = ParabolicSAR {
            af_step: acceleration_step,
            af_max: acceleration_max,
        };
        if !config.validate() {
            anyhow::bail!("SAR acceleration step must be smaller than maximum");
        }
        Ok(Self {
            config,
            instance: None,
            previous_trend: None,
            last_opened_at: None,
        })
    }

    pub fn next(&mut self, candle: &Candle) -> anyhow::Result<SarOutput> {
        self.validate_candle(candle)?;
        let input = yata::core::Candle {
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
        };
        if let Some(last_opened_at) = self.last_opened_at
            && candle.opened_at <= last_opened_at
        {
            anyhow::bail!("SAR candles must be strictly ordered");
        }

        let output = if let Some(instance) = &mut self.instance {
            instance.next(&input)
        } else {
            self.instance = Some(self.config.init(&input)?);
            self.last_opened_at = Some(candle.opened_at);
            let trend = SarTrend::Rising;
            self.previous_trend = Some(trend);
            return Ok(SarOutput {
                value: candle.low,
                trend,
                reversal: None,
            });
        };

        self.last_opened_at = Some(candle.opened_at);
        let trend = if output.value(1) >= 0.0 {
            SarTrend::Rising
        } else {
            SarTrend::Falling
        };
        let reversal = self
            .previous_trend
            .filter(|previous| *previous != trend)
            .map(|_| trend);
        self.previous_trend = Some(trend);
        Ok(SarOutput {
            value: output.value(0),
            trend,
            reversal,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.instance.is_some()
    }

    fn validate_candle(&self, candle: &Candle) -> anyhow::Result<()> {
        let values = [
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
        ];
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            anyhow::bail!("SAR candle values must be finite and non-negative");
        }
        if candle.high < candle.open
            || candle.high < candle.close
            || candle.high < candle.low
            || candle.low > candle.open
            || candle.low > candle.close
        {
            anyhow::bail!("SAR candle OHLC values are invalid");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(index: i64, open: i64, high: i64, low: i64, close: i64) -> Candle {
        Candle {
            opened_at: index,
            open: open as f64,
            high: high as f64,
            low: low as f64,
            close: close as f64,
            volume: 1.0,
        }
    }

    #[test]
    fn initializes_without_a_false_reversal() {
        let mut indicator = SarIndicator::new(0.02, 0.2).unwrap();

        let output = indicator.next(&candle(0, 100, 105, 99, 104)).unwrap();

        assert_eq!(output.trend, SarTrend::Rising);
        assert_eq!(output.reversal, None);
        assert!(indicator.is_ready());
    }

    #[test]
    fn rejects_duplicate_candles() {
        let mut indicator = SarIndicator::new(0.02, 0.2).unwrap();
        let first = candle(0, 100, 105, 99, 104);

        indicator.next(&first).unwrap();

        assert!(indicator.next(&first).is_err());
    }

    #[test]
    fn emits_reversal_after_incremental_updates() {
        let mut indicator = SarIndicator::new(0.02, 0.2).unwrap();
        let candles = [
            candle(0, 100, 105, 99, 104),
            candle(1, 104, 110, 103, 109),
            candle(2, 109, 111, 108, 110),
            candle(3, 110, 110, 90, 92),
        ];

        let outputs = candles
            .iter()
            .map(|candle| indicator.next(candle).unwrap())
            .collect::<Vec<_>>();

        assert!(
            outputs
                .iter()
                .any(|output| { output.reversal == Some(SarTrend::Falling) })
        );
    }
}
