use std::{
    collections::BinaryHeap,
    thread,
    time::{Duration, SystemTime},
};

use redis_module::{ThreadSafeContext, DetachedFromClient};
use serde::Serialize;
use tokio::{
    select,
    sync::mpsc,
    time::{self, Instant},
};
use uuid::Uuid;

use crate::Mode;

#[derive(Debug, Eq, Serialize)]
struct Msg {
    id: String,
    #[serde(skip)]
    queue_name: String,
    msg: String,
    delay_time: SystemTime,
    #[serde(skip)]
    mode: Mode,
}

impl Msg {
    fn new(queue_name: String, msg: String, delay_time: SystemTime, mode: Mode) -> Self {
        let id = Uuid::new_v4().to_string();
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
            ctx.call("RPUSH", &[msg.queue_name.as_bytes(), msg_json.as_bytes()]).unwrap();
        },
        Mode::Broadcast => {
            ctx.call("PUBLISH", &[msg.queue_name.as_bytes(), msg_json.as_bytes()]).unwrap();
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
                /* test */
                // let msg = Msg::new();
                // let now = SystemTime::now();
                // let sleep_time = msg.delay_time.duration_since(now).unwrap();
                /* test */
                let sleep = time::sleep(Duration::from_secs(10));
                let thread_ctx = ThreadSafeContext::new();
                tokio::pin!(sleep);
                loop {
                    select! {
                        msg = rx.recv() => {
                            if let Some(msg) = msg {
                                println!("Got msg {:?}", msg);
                                heap.push(msg);
                            }

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
                        },
                        () = &mut sleep => {
                            match heap.peek() {
                                None => {
                                    println!("timer elapsed");
                                    sleep.as_mut().reset(Instant::now() + Duration::from_secs(10));
                                },
                                Some(_) => {
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
                                } 
                            }
                        },
                    }
                }
            })
        });
    }

    pub(crate) fn push_delay_message(
        &self,
        queue_name: String,
        msg: String,
        delay_time: SystemTime,
        mode: Mode,
    ) {
        let msg = Msg::new(queue_name, msg, delay_time, mode);
        match self.tx.blocking_send(msg) {
            Ok(()) => {}
            Err(_) => panic!("The shared runtime has shut down."),
        }
    }
}
