mod logger;
mod msg_type;
mod queue_manager;

use crate::msg_type::MSG_HEAP_TYPE;
use msg_type::{Mode, Msg, MsgHeap};
use once_cell::sync::Lazy;
use queue_manager::KEY;
use redis_module::{redis_module, Context, RedisError, RedisResult, RedisString, Status};

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

static MANAGER: Lazy<queue_manager::QueueManager> =
    Lazy::new(|| queue_manager::QueueManager::new());

fn init(_: &Context, _: &[RedisString]) -> Status {
    MANAGER.borrow().trigger();
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
    // get arg
    let queue_name = args[1].clone().to_string();
    let message = args[2].clone().to_string();
    let delay = args[3].clone().to_string().parse::<u64>()?;
    let mode = Mode::from_str(args[4].clone().to_string())?;

    let delay_time = SystemTime::now().add(Duration::from_secs(delay));

    let msg = Msg::new(queue_name, message, delay_time, mode);
    let id = msg.id.clone();
    let key = ctx.open_key_writable(&RedisString::create(None, KEY));
    if let Some(value) = key.get_value::<MsgHeap>(&MSG_HEAP_TYPE)? {
        value.heap.push(msg);
    } else {
        let mut value = MsgHeap::new();
        value.heap.push(msg);
        key.set_value(&MSG_HEAP_TYPE, value)?;
    };
    MANAGER.borrow().trigger();
    Ok(id.into())
}

//////////////////////////////////////////////////////

redis_module! {
    name: "delay_queue",
    version: 1,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [
        MSG_HEAP_TYPE,
    ],
    init: init,
    deinit: deinit,
    commands: [
        ["delay_queue.push", push_delay_message, "write", 0, 0, 0],
    ],
}
