use std::time::{Duration, SystemTime};

pub fn elapsed(start: SystemTime) -> Duration {
    SystemTime::now()
        .duration_since(start)
        .expect("time went backwards")
}
