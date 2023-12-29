use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Serialize, Serializer};
use uuid::Uuid;

use crate::Mode;

#[derive(Debug, Eq, Serialize)]
pub(crate) struct Msg {
    pub(crate) id: String,
    pub(crate) queue_name: String,
    pub(crate) msg: String,
    #[serde(serialize_with = "serialize_time")]
    pub(crate) delay_time: SystemTime,
    pub(crate) mode: Mode,
}

fn serialize_time<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let duration_since_epoch = match time.duration_since(UNIX_EPOCH) {
        Ok(duration_since_epoch) => duration_since_epoch,
        Err(_) => Duration::ZERO,
    };
    serializer.serialize_u64(duration_since_epoch.as_secs())
}

#[cfg(test)]
mod tests {
    use std::{
        ops::Add,
        time::{Duration, SystemTime},
    };

    use super::Msg;

    #[test]
    fn test_msg_serialize() {
        let msg = Msg::new(
            "queue_name".to_string(),
            "msg".to_string(),
            SystemTime::now().add(Duration::from_secs(10)),
            crate::Mode::P2P,
        );
        let msg_json = serde_json::to_string(&msg).unwrap();
        println!("{msg_json}");
    }
}

impl Msg {
    pub(crate) fn new(queue_name: String, msg: String, delay_time: SystemTime, mode: Mode) -> Self {
        let id = Uuid::new_v4().to_string().replace("-", "");
        Self {
            id,
            queue_name,
            msg,
            delay_time,
            mode,
        }
    }
}

impl PartialEq for Msg {
    fn eq(&self, other: &Msg) -> bool {
        self.delay_time == other.delay_time
    }
}

impl Ord for Msg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delay_time.cmp(&other.delay_time)
    }
}

impl PartialOrd for Msg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
