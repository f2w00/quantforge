use chrono::{DateTime, Utc};

pub type Timestamp = DateTime<Utc>;

pub fn now_utc() -> Timestamp {
    Utc::now()
}
