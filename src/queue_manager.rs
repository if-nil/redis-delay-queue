use crate::{
    logger,
    msg_type::{Mode, Msg, MsgHeap, MSG_HEAP_TYPE},
};
use redis_module::{ContextGuard, RedisError, RedisString, ThreadSafeContext};
use std::{
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};
use tokio::{
    select,
    sync::Notify,
    time::{self, Instant},
};

const SLEEP_SECOND: u64 = 10;
pub const KEY: &str = "DELAY_QUEUE_dh234hgtrf5f34yRr65r6";

pub(crate) struct QueueManager {
    // 用来发送新队列或者延迟消息的tx
    notify: Arc<Notify>,
}

fn handle_heap_msg(ctx: &ContextGuard) -> Result<Duration, RedisError> {
    let key = ctx.open_key_writable(&RedisString::create(None, KEY));
    let mut d = Duration::from_secs(SLEEP_SECOND);
    if let Some(v) = key.get_value::<MsgHeap>(&MSG_HEAP_TYPE)? {
        let now = SystemTime::now();
        let mut flag = false;
        while let Some(msg) = v.heap.peek() {
            if msg.delay_time.le(&now) {
                pop_message(msg, &ctx);
                v.heap.pop();
                flag = true;
            } else {
                d = msg.delay_time.duration_since(now).unwrap();
                break;
            }
        }
        if flag {
            key.set_value(&MSG_HEAP_TYPE, v)?;
        }
    };
    Ok(d)
}

// 发送命令
fn pop_message(msg: &Msg, ctx: &ContextGuard) {
    let msg_json = serde_json::to_string(msg).unwrap();
    match msg.mode {
        Mode::P2P => {
            ctx.call("RPUSH", &[msg.queue_name.as_bytes(), msg_json.as_bytes()])
                .unwrap();
        }
        Mode::Broadcast => {
            ctx.call("PUBLISH", &[msg.queue_name.as_bytes(), msg_json.as_bytes()])
                .unwrap();
        }
    }
}

impl QueueManager {
    pub(crate) fn new() -> Self {
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();
        let q = Self { notify };
        q._start(notify2);
        q
    }

    fn _start(&self, notify: Arc<Notify>) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        thread::spawn(move || {
            rt.block_on(async move {
                let sleep = time::sleep(Duration::from_secs(SLEEP_SECOND));
                let thread_ctx = ThreadSafeContext::new();
                tokio::pin!(sleep);
                let mut done = false;
                loop {
                    select! {
                        () = notify.notified() => {
                            done = false;
                        },
                        () = &mut sleep => {
                            done = false;
                            sleep.as_mut().reset(Instant::now() + Duration::from_secs(SLEEP_SECOND));
                        },
                        () = async {}, if !done => {
                            let ctx = thread_ctx.lock();
                            let res = handle_heap_msg(&ctx);
                            drop(ctx);
                            match res {
                                Ok(d) => {
                                    sleep.as_mut().reset(Instant::now() + d);
                                },
                                Err(e) => {
                                    logger::log_warning(&thread_ctx, format!("handle delay msg failed: {}", e).as_str());
                                    sleep.as_mut().reset(Instant::now() + Duration::from_secs(SLEEP_SECOND));
                                },
                            }
                            done = true;
                        }
                    }
                }
            })
        });
    }

    pub(crate) fn trigger(&self) {
        self.notify.notify_one();
    }
}
