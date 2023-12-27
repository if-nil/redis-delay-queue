use crate::{logger, msg::Msg, Mode};
use redis_module::{Context, DetachedFromClient, RedisError, RedisResult, ThreadSafeContext};
use std::{
    collections::BinaryHeap,
    thread,
    time::{Duration, SystemTime},
};
use tokio::{
    select,
    sync::mpsc,
    time::{self, Instant},
};

const SLEEP_SECOND: u64 = 10;

pub(crate) struct QueueManager {
    // 用来发送新队列或者延迟消息的tx
    tx: mpsc::Sender<Msg>,
}

// 发送命令
async fn pop_message(msg: &Msg, thread_ctx: &ThreadSafeContext<DetachedFromClient>) {
    let msg_json = serde_json::to_string(msg).unwrap();
    let ctx = thread_ctx.lock();
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
    drop(ctx);
}

impl QueueManager {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel(16);
        let q = Self { tx };
        q._start(rx);
        q
    }

    fn _start(&self, mut rx: mpsc::Receiver<Msg>) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        thread::spawn(move || {
            rt.block_on(async move {
                // 构建延迟队列
                let mut heap: BinaryHeap<Msg> = BinaryHeap::new();
                let sleep = time::sleep(Duration::from_secs(SLEEP_SECOND));
                let thread_ctx = ThreadSafeContext::new();
                tokio::pin!(sleep);
                let mut done = true;
                loop {
                    select! {
                        msg = rx.recv() => {
                            if let Some(msg) = msg {
                                logger::log_debug(&thread_ctx, format!("recv msg {:?}", msg).as_str());
                                heap.push(msg);
                            }
                            done = false;
                        },
                        () = &mut sleep => {
                            match heap.peek() {
                                None => {
                                    logger::log_debug(&thread_ctx, "timer elapsed");
                                },
                                Some(_) => {
                                    done = false;
                                }
                            }
                            sleep.as_mut().reset(Instant::now() + Duration::from_secs(SLEEP_SECOND));
                        },
                        () = async {}, if !done => {
                            let now = SystemTime::now();
                            while let Some(msg) = heap.peek() {
                                if msg.delay_time.le(&now) {
                                    pop_message(msg, &thread_ctx).await;
                                    heap.pop();
                                } else {
                                    let sleep_time = msg.delay_time.duration_since(now).unwrap();
                                    sleep.as_mut().reset(Instant::now() + sleep_time);
                                    break;
                                }
                            }
                            done = true;
                        }
                    }
                }
            })
        });
    }

    pub(crate) fn push_delay_message(
        &self,
        ctx: &Context,
        queue_name: String,
        msg: String,
        delay_time: SystemTime,
        mode: Mode,
    ) -> RedisResult {
        let msg = Msg::new(queue_name, msg, delay_time, mode);
        let id = msg.id.clone();
        match self.tx.blocking_send(msg) {
            Ok(()) => Ok(id.into()),
            Err(e) => {
                let err = format!("send message error: {:?}", e);
                ctx.log_warning(err.as_str());
                Err(RedisError::String(err))
            }
        }
    }
}
