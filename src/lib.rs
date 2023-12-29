mod logger;
mod msg;
mod queue_manager;

use once_cell::sync::Lazy;
use redis_module::{redis_module, Context, RedisError, RedisResult, RedisString, Status};
use serde::Serialize;
use std::{
    borrow::Borrow,
    ops::Add,
    time::{Duration, SystemTime},
};

#[cfg(not(test))]
macro_rules! get_allocator {
    () => {
        redis_module::alloc::RedisAlloc
    };
}

#[cfg(test)]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

#[derive(Debug, Eq, Serialize)]
enum Mode {
    P2P,
    Broadcast,
}

impl PartialEq for Mode {
    fn eq(&self, _: &Mode) -> bool {
        true
    }
}

impl Mode {
    fn from_str(s: RedisString) -> Result<Self, RedisError> {
        match s.to_string().as_str() {
            "p2p" => Ok(Mode::P2P),
            "broadcast" => Ok(Mode::Broadcast),
            _ => Err(RedisError::Str(
                "invalid mode, must be `p2p` or `broadcast`",
            )),
        }
    }
}

static MANAGER: Lazy<queue_manager::QueueManager> =
    Lazy::new(|| queue_manager::QueueManager::new());

fn init(_: &Context, _: &[RedisString]) -> Status {
    let _ = *&MANAGER;
    Status::Ok
}

fn deinit(_: &Context) -> Status {
    Status::Ok
}

// delay_queue.push {queue_name} {message} {delay_time} {mode}
fn push_delay_message(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 5 {
        return Err(RedisError::WrongArity);
    }

    let queue_name = args[1].clone().to_string();
    let message = args[2].clone().to_string();
    let delay = args[3].clone().to_string().parse::<u64>()?;
    let mode = Mode::from_str(args[4].clone())?;
    let delay_time = SystemTime::now().add(Duration::from_secs(delay));
    MANAGER
        .borrow()
        .push_delay_message(ctx, queue_name, message, delay_time, mode)
}

//////////////////////////////////////////////////////

redis_module! {
    name: "delay_queue",
    version: 1,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
        ["delay_queue.push", push_delay_message, "", 0, 0, 0],
    ],
}
