use std::time::SystemTime;

use serde::Serialize;
use uuid::Uuid;

use crate::Mode;

#[derive(Debug, Eq, Serialize)]
pub(crate) struct Msg {
    pub(crate) id: String,
    pub(crate) queue_name: String,
    pub(crate) msg: String,
    pub(crate) delay_time: SystemTime,
    pub(crate) mode: Mode,
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
