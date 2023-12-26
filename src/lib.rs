mod queue_manager;

use once_cell::sync::Lazy;
use redis_module::{
    redis_module, Context, RedisError, RedisResult, RedisString, RedisValue, Status,
    ThreadSafeContext,
};
use serde::Serialize;
use std::{
    borrow::Borrow,
    ops::Add,
    thread,
    time::{Duration, SystemTime},
};

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
fn push_delay_message(_: &Context, args: Vec<RedisString>) -> RedisResult {
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
        .push_delay_message(queue_name, message, delay_time, mode);
    Ok("OK".into())
}

fn async_demo(ctx: &Context, _: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            // Replace with the actual delay time
            let delay_time = Duration::from_secs(5);
            tokio::time::sleep(delay_time).await;
            thread_ctx.reply(Ok("42".into()));
        });
    });
    Ok(RedisValue::NoReply)
}

//////////////////////////////////////////////////////

redis_module! {
    name: "delay_queue",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
        // ["delay_queue.create", create_delay_queue, "", 0, 0, 0],
        ["delay_queue.push", push_delay_message, "", 0, 0, 0],
        ["delay_queue.async_demo", async_demo, "", 0, 0, 0],
    ],
}
