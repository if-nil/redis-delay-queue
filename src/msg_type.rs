use std::{
    collections::BinaryHeap,
    ops::Add,
    os::raw::{c_int, c_void},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis_module::{
    load_string, load_unsigned, native_types::RedisType, raw, save_string, save_unsigned,
    RedisError,
};
use serde::{Serialize, Serializer};
use uuid::Uuid;

pub(crate) struct MsgHeap {
    pub(crate) heap: BinaryHeap<Msg>,
}

impl MsgHeap {
    pub(crate) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }
}

pub static MSG_HEAP_TYPE: RedisType = RedisType::new(
    "msg_heap1",
    1,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: None,
        free: Some(free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,

        // Aux data
        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,

        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,

        copy2: None,
        free_effort2: None,
        mem_usage2: None,
        unlink2: None,
    },
);

#[allow(non_snake_case, unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<MsgHeap>()));
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let msg_heap = (&*(value as *mut MsgHeap));
    let len = msg_heap.heap.len();
    save_unsigned(rdb, (len as u32).into());
    for msg in msg_heap.heap.iter() {
        save_string(rdb, &msg.id);
        save_string(rdb, &msg.queue_name);
        save_string(rdb, &msg.msg);
        let nanos = msg
            .delay_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();
        save_string(rdb, &nanos);
        save_string(rdb, &msg.mode.to_string());
    }
}

#[allow(non_snake_case, unused)]
pub unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
    let mut heap = MsgHeap::new();
    let mut len = match load_unsigned(rdb) {
        Ok(l) => l,
        Err(e) => return Box::into_raw(Box::new(heap)) as *mut c_void,
    };
    while len != 0 {
        let id = load_string(rdb).unwrap().to_string();
        let queue_name = load_string(rdb).unwrap().to_string();
        let msg = load_string(rdb).unwrap().to_string();
        let nanos = load_string(rdb)
            .unwrap()
            .to_string()
            .parse::<u128>()
            .unwrap();
        let delay_time = SystemTime::from(UNIX_EPOCH.add(Duration::from_nanos(nanos as u64)));
        let mode = Mode::from_str(load_string(rdb).unwrap().to_string()).unwrap();
        heap.heap.push(Msg {
            id,
            queue_name,
            msg,
            delay_time,
            mode,
        });
        len -= 1;
    }
    Box::into_raw(Box::new(heap)) as *mut c_void
}

/// Msg ////////////////////////////////
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
            crate::msg_type::Mode::P2P,
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

/// Mode ////////////////////////
#[derive(Debug, Eq, Serialize)]
pub(crate) enum Mode {
    P2P,
    Broadcast,
}

impl PartialEq for Mode {
    fn eq(&self, _: &Mode) -> bool {
        true
    }
}

impl Mode {
    pub(crate) fn from_str(s: String) -> Result<Self, RedisError> {
        match s.to_lowercase().as_str() {
            "p2p" => Ok(Mode::P2P),
            "broadcast" => Ok(Mode::Broadcast),
            _ => Err(RedisError::Str(
                "invalid mode, must be `p2p` or `broadcast`",
            )),
        }
    }
}

impl ToString for Mode {
    #[inline]
    fn to_string(&self) -> String {
        match self {
            Mode::P2P => "p2p".to_string(),
            Mode::Broadcast => "broadcast".to_string(),
        }
    }
}
